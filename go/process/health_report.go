package process

import (
	"errors"
	"fmt"
	"sort"
	"time"

	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// ErrNoHeartbeatYet is the node error reported before the first heartbeat.
var ErrNoHeartbeatYet = errors.New("no heartbeat has been written yet")

// TenantHealthError is the failure of one tenant backend. It is never fatal by
// itself: the caller decides what a tenant failure means (typically
// orcraft.YieldForTenantFailure once it lasted long enough).
type TenantHealthError struct {
	TenantID string
	Err      error
}

func (e *TenantHealthError) Error() string {
	return fmt.Sprintf("tenant %s: %v", e.TenantID, e.Err)
}

func (e *TenantHealthError) Unwrap() error { return e.Err }

// TenantErrors extracts every *TenantHealthError from err, which may be the
// result of errors.Join (as returned by (*Control).ExpireAvailableNodes and
// ExpireNodesHistory). Sorted by tenant id.
func TenantErrors(err error) []*TenantHealthError {
	var out []*TenantHealthError
	var walk func(error)
	walk = func(err error) {
		switch e := err.(type) {
		case nil:
		case *TenantHealthError:
			out = append(out, e)
		case interface{ Unwrap() []error }:
			for _, inner := range e.Unwrap() {
				walk(inner)
			}
		}
	}
	walk(err)
	sort.Slice(out, func(i, j int) bool { return out[i].TenantID < out[j].TenantID })
	return out
}

// BackendHeartbeat is the outcome of writing the heartbeat into one backend.
type BackendHeartbeat struct {
	Healthy bool
	Err     error
}

// OK tells whether the heartbeat was registered without error.
func (h BackendHeartbeat) OK() bool { return h.Healthy && h.Err == nil }

// HeartbeatResult is the outcome of one (*Control).Heartbeat.
type HeartbeatResult struct {
	At time.Time
	// Control is the control backend. It alone decides the node health.
	Control BackendHeartbeat
	// Tenants holds one entry per registered mirror, by tenant id. A tenant
	// that shares the control backend carries the control outcome.
	Tenants map[string]BackendHeartbeat
}

// TenantErrors lists the tenants whose heartbeat failed, sorted by id.
func (r *HeartbeatResult) TenantErrors() []*TenantHealthError {
	var out []*TenantHealthError
	for id, hb := range r.Tenants {
		if hb.OK() {
			continue
		}
		err := hb.Err
		if err == nil {
			err = errHeartbeatNotRegistered
		}
		out = append(out, &TenantHealthError{TenantID: id, Err: err})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TenantID < out[j].TenantID })
	return out
}

// TenantHealth is the health of one tenant backend as seen by the heartbeat.
type TenantHealth struct {
	TenantID string
	Healthy  bool
	// Error is the last heartbeat error; nil when healthy.
	Error error
	// LastGood is the last successful heartbeat; zero if there has been none.
	LastGood time.Time
	// UnhealthyFor is how long the backend has been failing continuously, 0
	// when healthy. It is what a caller compares against its yield threshold.
	UnhealthyFor time.Duration
}

// AggregatedHealth separates the two health notions of a multi-tenant node:
//
//   - the NODE health (control backend + Raft), which is what logic uses to
//     yield leadership after 5s and to exit after 30s of failure;
//   - the health of each TENANT backend, which must never decide the fate of
//     the node by itself; the caller reacts to a tenant failure with
//     orcraft.YieldForTenantFailure.
type AggregatedHealth struct {
	NodeHealthy bool
	// NodeErr joins the control backend error and the Raft error.
	NodeErr    error
	ControlErr error
	RaftErr    error
	// Tenants is sorted by tenant id.
	Tenants []TenantHealth
}

// UnhealthyTenants returns the tenants that have been failing for at least
// minDuration (0 returns every failing tenant).
func (a *AggregatedHealth) UnhealthyTenants(minDuration time.Duration) []TenantHealth {
	var out []TenantHealth
	for _, t := range a.Tenants {
		if !t.Healthy && t.UnhealthyFor >= minDuration {
			out = append(out, t)
		}
	}
	return out
}

// AggregateHealth computes the node health from the control heartbeat and the
// Raft error (nil when Raft is healthy or not enabled), and the per-tenant
// health from the tenant heartbeats. It is a pure function; hb must not be nil.
// The UnhealthyFor/LastGood fields of the tenants are not known to a bare
// HeartbeatResult: use (*Control).Health for those.
func AggregateHealth(hb *HeartbeatResult, raftErr error) *AggregatedHealth {
	a := &AggregatedHealth{RaftErr: raftErr}
	controlErr := hb.Control.Err
	if controlErr == nil && !hb.Control.Healthy {
		controlErr = errHeartbeatNotRegistered
	}
	a.ControlErr = controlErr
	a.NodeErr = errors.Join(controlErr, raftErr)
	a.NodeHealthy = a.NodeErr == nil
	for id, t := range hb.Tenants {
		th := TenantHealth{TenantID: id, Healthy: t.OK(), Error: t.Err}
		if !t.OK() && th.Error == nil {
			th.Error = errHeartbeatNotRegistered
		}
		a.Tenants = append(a.Tenants, th)
	}
	sort.Slice(a.Tenants, func(i, j int) bool { return a.Tenants[i].TenantID < a.Tenants[j].TenantID })
	return a
}

// RaftHealthError returns a non-nil error when Raft is enabled but this node
// is not part of the quorum; nil otherwise (including Raft disabled).
func RaftHealthError() error {
	if !orcraft.IsRaftEnabled() {
		return nil
	}
	if !orcraft.IsPartOfQuorum() {
		return errors.New("raft: node is not part of the quorum")
	}
	return nil
}

// TenantsHealth returns the health of every registered tenant mirror, with
// durations, sorted by tenant id. It reads the bookkeeping of the heartbeats;
// it does not touch any backend.
func (c *Control) TenantsHealth() []TenantHealth {
	mirrors := c.mirrorTargets()
	out := make([]TenantHealth, 0, len(mirrors))
	for _, t := range mirrors {
		if t.viaControl {
			out = append(out, c.control.snapshot(t.id))
			continue
		}
		out = append(out, t.snapshot(t.id))
	}
	return out
}

// Health aggregates the last heartbeat with the Raft state, and enriches the
// tenants with their failure durations. It does not touch any backend, so it
// is cheap to call on every health tick.
func (c *Control) Health() *AggregatedHealth {
	hb := c.lastHeartbeat.Load()
	if hb == nil {
		return &AggregatedHealth{NodeHealthy: false, NodeErr: ErrNoHeartbeatYet, ControlErr: ErrNoHeartbeatYet}
	}
	a := AggregateHealth(hb, RaftHealthError())
	a.Tenants = c.TenantsHealth()
	return a
}

// ValidateMultiTenantElection tells whether the node election mode is
// compatible with the number of tenants. Without Raft the leader is the row of
// active_node in the control backend, which is a single backend: with several
// tenants (each with its own backend) there would be no single leader for all
// of them. Multi-tenant therefore requires Raft.
func ValidateMultiTenantElection(raftEnabled bool, tenantCount int) error {
	if tenantCount > 1 && !raftEnabled {
		return fmt.Errorf("process: %d tenants configured but Raft is disabled: with several tenants the leader election requires Raft (the active_node election lives in a single backend); enable Raft or configure a single tenant", tenantCount)
	}
	return nil
}
