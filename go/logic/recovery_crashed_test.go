package logic

import (
	"testing"

	"github.com/proxysql/golib/sqlutils"

	"github.com/proxysql/orchestrator/go/process"
	"github.com/proxysql/orchestrator/go/util"
)

func countActiveRecoveries(t *testing.T, tn *Tenant) int {
	t.Helper()
	return countRowsOf(t, tn, `select count(*) as c from topology_recovery where in_active_period = 1`)
}

// A follower stores a replicated recovery as the one of the node that originated it: when that node
// is gone from node_health (it was killed), the new leader recognises the recovery as crashed instead
// of waiting out the whole RecoveryPeriodBlockSeconds.
func TestReplicatedRecoveryOfADeadLeaderIsCrashed(t *testing.T) {
	leaderM, _ := newTestManager(t, []string{"a"}, nil)
	followerM, followerLeadership := newTestManager(t, []string{"a"}, nil)
	oldLeader, follower := mustTenant(t, leaderM, "a"), mustTenant(t, followerM, "a")

	// the leader registers the recovery and publishes it: the entry carries the node that originates it
	registered, err := oldLeader.registerTopologyRecovery(replicatedRecovery(0, "uid-1", "db1"))
	if err != nil || registered == nil {
		t.Fatalf("registering: %v, %v", registered, err)
	}
	if registered.ProcessingNodeHostname != process.ThisHostname || registered.ProcessingNodeToken != util.ProcessToken.Hash {
		t.Fatalf("the replicated entry does not carry its origin: %q %q", registered.ProcessingNodeHostname, registered.ProcessingNodeToken)
	}
	// ... which, for the follower, is another node (the test process is every node)
	entry := replicatedRecovery(registered.Id, registered.UID, "db1")
	entry.ProcessingNodeHostname, entry.ProcessingNodeToken = "old-leader", "old-leader-token"
	applyWriteRecovery(t, NewCommandApplier(followerM), entry)

	var hostname, token string
	if err := follower.Backend.QueryRowsMap(`select processing_node_hostname, processcing_node_token from topology_recovery`, func(m sqlutils.RowMap) error {
		hostname, token = m.GetString("processing_node_hostname"), m.GetString("processcing_node_token")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if hostname != "old-leader" || token != "old-leader-token" {
		t.Fatalf("the follower stamped the recovery with %q/%q, not with the node that originated it", hostname, token)
	}

	followerM.Control().Heartbeat(process.ThisNodeHealth) // the follower itself is in node_health
	// still a follower: acknowledging is the leader's business
	if n, err := follower.acknowledgeCrashedRecoveries(); err != nil || n != 0 {
		t.Fatalf("a follower acknowledged a recovery (%d, %v)", n, err)
	}
	followerLeadership.leader.Store(true) // the new leader
	if n, err := follower.acknowledgeCrashedRecoveries(); err != nil || n != 1 {
		t.Fatalf("the recovery of a node that is not in node_health is crashed: acknowledged %d, %v", n, err)
	}
	if n := countActiveRecoveries(t, follower); n != 0 {
		t.Fatalf("the orphan recovery still blocks the instance (%d active)", n)
	}
}

// The origin being alive (it is in node_health), or being this very node, is not a crash; neither is an
// entry of an old log that carries no origin (it falls back to this node's identity).
func TestReplicatedRecoveryOfALiveNodeIsNotCrashed(t *testing.T) {
	followerM, followerLeadership := newTestManager(t, []string{"a"}, nil)
	followerLeadership.leader.Store(true)
	follower := mustTenant(t, followerM, "a")
	applier := NewCommandApplier(followerM)

	alive := replicatedRecovery(1, "uid-alive", "db1")
	alive.ProcessingNodeHostname, alive.ProcessingNodeToken = "alive-leader", "alive-token"
	self := replicatedRecovery(2, "uid-self", "db2")
	self.ProcessingNodeHostname, self.ProcessingNodeToken = process.ThisHostname, util.ProcessToken.Hash
	legacy := replicatedRecovery(3, "uid-legacy", "db3") // no origin in the entry
	for _, tr := range []*TopologyRecovery{alive, self, legacy} {
		applyWriteRecovery(t, applier, tr)
	}
	followerM.Control().Heartbeat(process.ThisNodeHealth)
	if _, err := follower.Backend.Exec(`insert into node_health (hostname, token, first_seen_active, last_seen_active) values ('alive-leader', 'alive-token', now(), now())`); err != nil {
		t.Fatal(err)
	}
	if n, err := follower.acknowledgeCrashedRecoveries(); err != nil || n != 0 {
		t.Fatalf("a recovery of a live node was acknowledged (%d, %v)", n, err)
	}
	if n := countActiveRecoveries(t, follower); n != 3 {
		t.Fatalf("active recoveries: %d", n)
	}
}
