package logic

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/proxysql/golib/log"

	"github.com/proxysql/orchestrator/go/config"
	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// The tenant-set announcement (orcraft.TenantSetCommand) tells every node which tenants
// the leader runs. A node that runs other tenants drops, silently, the commands of the
// tenants it lacks, and adding the tenant later does not recover them; this makes the
// divergence visible: an ERROR log naming the tenants, the tenant_set_mismatch metric,
// and, when configured (RaftTenantSetMismatchPolicy=yield), the leadership moving to
// another node.
//
// A follower compares what the leader announces with its own tenants, on every
// announcement. The leader sees its own announcement come back, which proves nothing; it
// compares its tenants with the last set announced by another node (the previous
// leader), if that one is recent: this catches a node with different tenants that has
// just been elected, which is the case "yield" is for.

// foreignTenantSetTTL is how long the set announced by another node serves as the
// baseline of the leader: the previous leader announced it at most one interval before
// the election, and a cluster that stays without that node's announcements for longer
// is no longer evidence of anything.
const foreignTenantSetTTL = 2 * orcraft.TenantSetPublishInterval

// tenantSetYield is orcraft.Yield, a seam for tests.
var tenantSetYield = orcraft.Yield

type tenantSetState struct {
	mu            sync.Mutex
	lastForeign   *orcraft.TenantSet
	lastForeignAt time.Time
	// lastDiff is the outcome of the last comparison (empty: the tenants match).
	lastDiff orcraft.TenantSetDiff
}

// tenantProviders is the source of the tenant set a leader announces: tenant ID -> provider.
func (m *TenantManager) tenantProviders() map[string]string {
	providers := make(map[string]string, len(m.tenants))
	for _, t := range m.tenants {
		providers[t.ID] = t.Provider
	}
	return providers
}

// TenantSet returns the tenants this node runs, as announced to the Raft group.
func (m *TenantManager) TenantSet() orcraft.TenantSet {
	return orcraft.NewTenantSet(m.thisNodeID(), m.tenantProviders())
}

func (m *TenantManager) thisNodeID() string {
	if m.thisNode != nil {
		return m.thisNode()
	}
	return orcraft.ThisNode()
}

func (m *TenantManager) clock() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

func (m *TenantManager) mismatchPolicy() string {
	if strings.EqualFold(strings.TrimSpace(m.base.RaftTenantSetMismatchPolicy), config.TenantSetMismatchYield) {
		return config.TenantSetMismatchYield
	}
	return config.TenantSetMismatchWarn
}

func (m *TenantManager) recordTenantSetDiff(diff orcraft.TenantSetDiff) {
	m.tenantSetState.mu.Lock()
	m.tenantSetState.lastDiff = diff
	m.tenantSetState.mu.Unlock()
	orcraft.SetTenantSetMismatch(!diff.Empty())
}

// TenantSetDiff is the difference found by the last tenant-set announcement between the
// tenants of this node and the reference it was compared with (empty: no mismatch).
func (m *TenantManager) TenantSetDiff() orcraft.TenantSetDiff {
	m.tenantSetState.mu.Lock()
	defer m.tenantSetState.mu.Unlock()
	return m.tenantSetState.lastDiff
}

// applyTenantSet handles the Raft command tenant-set on this node.
func (m *TenantManager) applyTenantSet(value []byte) interface{} {
	var announced orcraft.TenantSet
	if err := json.Unmarshal(value, &announced); err != nil {
		return log.Errore(err)
	}
	local := m.TenantSet()
	ownAnnouncement := announced.Node != "" && announced.Node == local.Node

	st := &m.tenantSetState
	st.mu.Lock()
	var compareWith *orcraft.TenantSet
	var source string
	if ownAnnouncement {
		if st.lastForeign != nil && m.clock().Sub(st.lastForeignAt) <= foreignTenantSetTTL {
			compareWith = st.lastForeign
			source = "the previous leader (" + st.lastForeign.Node + ")"
		}
	} else {
		cp := announced
		st.lastForeign, st.lastForeignAt = &cp, m.clock()
		compareWith = &announced
		source = "the leader (" + announced.Node + ")"
	}
	st.mu.Unlock()

	if compareWith == nil {
		m.recordTenantSetDiff(orcraft.TenantSetDiff{})
		return nil
	}
	diff := orcraft.DiffTenantSets(*compareWith, local)
	m.recordTenantSetDiff(diff)
	if diff.Empty() {
		return nil
	}
	_ = log.Errorf("orchestrator/raft: the tenants of this node (%s) differ from the ones of %s: %s. "+
		"A node without a tenant drops the Raft commands of that tenant for good, and adding it later does not recover them: "+
		"every Raft node must run the same tenants (tenants file); policy RaftTenantSetMismatchPolicy=%s",
		orcraft.DescribeTenantSet(local), source, diff, m.mismatchPolicy())
	if ownAnnouncement && m.mismatchPolicy() == config.TenantSetMismatchYield && m.leader.IsLeader() {
		_ = log.Errorf("orchestrator/raft: yielding the leadership: this node runs different tenants than the rest of the group")
		// not on the FSM goroutine: the transfer needs the FSM to keep running
		go func() { _ = tenantSetYield() }()
	}
	return nil
}
