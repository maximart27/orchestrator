package orcraft

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// LegacyTenantID is the tenant to which every command/snapshot that carries no
// tenant information belongs. It is the single tenant of a single-tenant
// (legacy) deployment, and the one that raft log entries written before
// multi-tenancy existed are applied to. It must never change: after a restart
// the FSM replays log entries that do not carry a tenant.
const LegacyTenantID = "default"

// Raft commands that are not tenant-scoped. They are applied by the node itself
// (or are meaningful for the Raft group as a whole) and must never carry a tenant.
const (
	HeartbeatCommand           = "heartbeat"
	AsyncSnapshotCommand       = "async-snapshot"
	RegisterNodeCommand        = "register-node"
	LeaderURICommand           = "leader-uri"
	RequestHealthReportCommand = "request-health-report"
	// TenantSetCommand is declared in tenant_set.go
	// YieldCommand and YieldHintCommand are declared in raft.go
)

// globalOps is the single source of truth for which ops are global.
var globalOps = map[string]struct{}{
	HeartbeatCommand:           {},
	AsyncSnapshotCommand:       {},
	RegisterNodeCommand:        {},
	LeaderURICommand:           {},
	RequestHealthReportCommand: {},
	TenantSetCommand:           {},
	YieldCommand:               {},
	YieldHintCommand:           {},
}

// IsGlobalOp tells whether op is a global (non tenant-scoped) command.
func IsGlobalOp(op string) bool {
	_, ok := globalOps[op]
	return ok
}

// ResolveTenant maps the empty tenant (absent in the log entry / not given by the
// caller) to LegacyTenantID.
func ResolveTenant(tenantID string) string {
	if tenantID == "" {
		return LegacyTenantID
	}
	return tenantID
}

// ErrUnknownTenant must be returned (possibly wrapped with %w) by an applier that
// does not have a runtime for the tenant of a command. The FSM logs it with a
// dedicated message and counts it (see UnknownTenantCommands).
var ErrUnknownTenant = errors.New("orchestrator/raft: unknown tenant")

// ErrTenantOnGlobalCommand is the error response for a global op carrying a
// non-legacy tenant.
var ErrTenantOnGlobalCommand = errors.New("orchestrator/raft: global command must not carry a tenant")

// ErrUndecodableCommand is the response to a log entry that is not a valid command. It is
// never applied, on any node.
var ErrUndecodableCommand = errors.New("orchestrator/raft: undecodable command")

// TenantCommandApplier is implemented by appliers that are tenant-aware. tenantID
// is never empty: the FSM resolves "" to LegacyTenantID. Only tenant-scoped ops
// are routed here; global ops (IsGlobalOp) are handled by ApplyCommand.
//
// Contract for implementations:
//   - Must be deterministic and must not depend on leadership.
//   - For a tenantID without a local runtime: return an error wrapping
//     ErrUnknownTenant. Do not panic and do not apply the command to another tenant.
//   - Errors are returned as a value of type error (the Raft response); the caller
//     of Publish*Command on the leader gets it as err.
type TenantCommandApplier interface {
	ApplyTenantCommand(tenantID, op string, value []byte) interface{}
}

var unknownTenantCommands int64

// UnknownTenantCommands returns how many commands this node could not apply since
// start because their tenant is unknown here (config divergence between nodes).
func UnknownTenantCommands() int64 {
	return atomic.LoadInt64(&unknownTenantCommands)
}

// routeCommand routes a decoded log entry to the applier. Global ops go to the
// legacy ApplyCommand. Tenant-scoped ops go to ApplyTenantCommand when the applier
// implements TenantCommandApplier; otherwise to ApplyCommand if the tenant is the
// legacy one, and are rejected with ErrUnknownTenant if not (a tenant-unaware
// applier knows only the legacy tenant).
func routeCommand(applier CommandApplier, c storeCommand) interface{} {
	tenantID := ResolveTenant(c.Tenant)
	if IsGlobalOp(c.Op) {
		if tenantID != LegacyTenantID {
			return fmt.Errorf("%w: op=%s tenant=%s", ErrTenantOnGlobalCommand, c.Op, tenantID)
		}
		return applier.ApplyCommand(c.Op, c.Value)
	}
	if ta, ok := applier.(TenantCommandApplier); ok {
		return ta.ApplyTenantCommand(tenantID, c.Op, c.Value)
	}
	if tenantID != LegacyTenantID {
		return fmt.Errorf("%w: %s (applier is not tenant-aware), op=%s", ErrUnknownTenant, tenantID, c.Op)
	}
	return applier.ApplyCommand(c.Op, c.Value)
}
