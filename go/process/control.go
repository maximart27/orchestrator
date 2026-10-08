package process

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
)

// Control owns the node-level ("control") data of this orchestrator process:
// node_health (+history), active_node and access_token. These are NOT tenant
// data: there is one heartbeat, one election and one set of API tokens per
// process, whatever the number of tenants.
//
// The control data lives in the control backend. node_health is additionally
// replicated into the backend of every tenant ("mirrors"), because tenant
// backends run SQL with subselects on node_health (AcknowledgeCrashedRecoveries,
// ExpireMaintenance) and a tenant backend is a separate database. Mirrors are
// best effort and isolated from the node: a failing mirror is reported per
// tenant (see HeartbeatResult, TenantHealth) and never affects the control
// heartbeat, which is what drives the node health (yield / fatal in logic).
//
// active_node (election without Raft) lives only in the control backend, so
// with more than one tenant the election MUST be Raft's; see
// ValidateMultiTenantElection.
//
// Process-wide state that is legitimately global stays at package level:
// ThisHostname, ThisNodeHealth, util.ProcessToken and the node health
// timestamps (SinceLastHealthCheck, SinceLastGoodHealthCheck,
// LastContinousCheckHealthy), which are driven by the control heartbeat only.
type Control struct {
	control *target

	mu      sync.RWMutex
	mirrors map[string]*target

	mirrorTimeout time.Duration
	// writeMirror writes one heartbeat into a mirror; a field so tests can
	// simulate a slow backend.
	writeMirror func(t *target, nh *NodeHealth) (bool, error)

	lastHeartbeat atomic.Pointer[HeartbeatResult]
	healthCache   *cache.Cache
	registerOnce  sync.Once
}

// NewControl builds the control of this node on top of the control backend.
func NewControl(control *db.Backend) *Control {
	c := &Control{
		control:       newTarget("", control),
		mirrors:       make(map[string]*target),
		mirrorTimeout: 2 * config.HealthPollSeconds * time.Second,
		healthCache:   cache.New(config.HealthPollSeconds*time.Second, time.Second),
	}
	c.writeMirror = writeRegisterNode
	return c
}

var defaultControl atomic.Pointer[Control]

// DefaultControl returns the process-wide control used by the package-level
// legacy functions. Unless SetDefaultControl was called it is a Control over
// db.DefaultBackend(), i.e. the legacy single-tenant behaviour.
func DefaultControl() *Control {
	if c := defaultControl.Load(); c != nil {
		return c
	}
	defaultControl.CompareAndSwap(nil, NewControl(db.DefaultBackend()))
	return defaultControl.Load()
}

// SetDefaultControl installs the Control the package-level functions use. It
// is meant to be called once at startup, before anything uses the package
// level functions. The control is node-level, not tenant-level state.
func SetDefaultControl(c *Control) {
	defaultControl.Store(c)
}

// Backend returns the control backend.
func (c *Control) Backend() *db.Backend { return c.control.backend }

// SetMirrorTimeout sets how long a heartbeat waits for the mirrors before
// reporting the slow ones as failed (they keep running in the background).
func (c *Control) SetMirrorTimeout(d time.Duration) { c.mirrorTimeout = d }

// AddMirror registers the backend of a tenant as a destination of the node
// heartbeat. Adding the control backend itself is accepted (the tenant simply
// shares the control backend, as the legacy "default" tenant does) and needs
// no extra write. Adding an id twice replaces the previous mirror.
func (c *Control) AddMirror(id string, b *db.Backend) error {
	if id == "" {
		return errors.New("process: mirror id must not be empty")
	}
	if b == nil {
		return fmt.Errorf("process: mirror %q: nil backend", id)
	}
	t := newTarget(id, b)
	t.viaControl = b == c.control.backend
	c.mu.Lock()
	c.mirrors[id] = t
	c.mu.Unlock()
	return nil
}

// RemoveMirror stops replicating the heartbeat into the tenant's backend. It
// does not close the backend.
func (c *Control) RemoveMirror(id string) {
	c.mu.Lock()
	delete(c.mirrors, id)
	c.mu.Unlock()
}

// MirrorIDs returns the ids of the registered mirrors, sorted.
func (c *Control) MirrorIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.mirrors))
	for id := range c.mirrors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// mirrorTargets returns the registered mirrors sorted by id.
func (c *Control) mirrorTargets() []*target {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ts := make([]*target, 0, len(c.mirrors))
	for _, t := range c.mirrors {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i].id < ts[j].id })
	return ts
}

// target is one destination of the heartbeat: the control backend (id "") or a
// tenant mirror, with the health bookkeeping of that destination.
type target struct {
	id      string
	backend *db.Backend
	// viaControl: the backend is the control backend; nothing to write.
	viaControl bool

	mu           sync.Mutex
	history      map[string]struct{} // node_health_history rows already inserted
	inFlight     bool
	lastGood     time.Time
	failingSince time.Time
	lastErr      error
}

func newTarget(id string, b *db.Backend) *target {
	return &target{id: id, backend: b, history: make(map[string]struct{})}
}

// firstHistory tells whether the history row of (hostname, token) still has to
// be inserted, and marks it as done.
func (t *target) firstHistory(nh *NodeHealth) bool {
	key := nh.Hostname + "\x00" + nh.Token
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, done := t.history[key]; done {
		return false
	}
	t.history[key] = struct{}{}
	return true
}

func (t *target) tryStart() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inFlight {
		return false
	}
	t.inFlight = true
	return true
}

func (t *target) finish() {
	t.mu.Lock()
	t.inFlight = false
	t.mu.Unlock()
}

// record stores the outcome of a heartbeat attempt.
func (t *target) record(hb BackendHeartbeat) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if hb.Healthy && hb.Err == nil {
		t.lastGood = now
		t.failingSince = time.Time{}
		t.lastErr = nil
		return
	}
	if t.failingSince.IsZero() {
		t.failingSince = now
	}
	t.lastErr = hb.Err
	if t.lastErr == nil {
		t.lastErr = errHeartbeatNotRegistered
	}
}

func (t *target) snapshot(id string) TenantHealth {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := TenantHealth{TenantID: id, Healthy: t.failingSince.IsZero(), Error: t.lastErr, LastGood: t.lastGood}
	if !t.failingSince.IsZero() {
		h.UnhealthyFor = time.Since(t.failingSince)
	}
	return h
}
