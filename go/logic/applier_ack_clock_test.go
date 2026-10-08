package logic

import (
	"testing"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/inst"
)

// ack-recovery closes the active period of a recovery with end_active_period_unixtime = the
// SECOND (of the node's clock) in which it is applied, and that column is part of the unique
// index (hostname, port, in_active_period, end_active_period_unixtime) of topology_recovery.
// Two recoveries of the same instance acknowledged in the same second therefore collide:
// the second acknowledgement fails -- or not, if the nodes apply it in different seconds.
// The outcome depends on the clock of the node that applies the entry, so it is NOT
// deterministic across the nodes of the Raft group.
func TestApplierAckTwiceInTheSameSecondDependsOnTheClock(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	tn := mustTenant(t, m, "a")
	applier := NewCommandApplier(m)
	key := inst.InstanceKey{Hostname: "db1", Port: 3306}
	analysis := inst.ReplicationAnalysis{AnalyzedInstanceKey: key, Analysis: inst.DeadMaster, CountReplicas: 1,
		ClusterDetails: inst.ClusterInfo{ClusterName: "cl", ClusterAlias: "al"}}
	write := func(uid string) {
		if res := applier.ApplyTenantCommand("a", "write-recovery", mustJSON(t, &TopologyRecovery{UID: uid, AnalysisEntry: analysis})); res != nil {
			t.Fatalf("write-recovery %s: %v", uid, res)
		}
	}
	ack := func() interface{} {
		return applier.ApplyTenantCommand("a", "ack-recovery", mustJSON(t, RecoveryAcknowledgement{Owner: "me", Comment: "c", AllRecoveries: true}))
	}
	active := func() (n int) {
		_ = tn.Backend.QueryRowsMap("select count(*) as c from topology_recovery where in_active_period = 1", func(m sqlutils.RowMap) error {
			n = m.GetInt("c")
			return nil
		})
		return n
	}
	write("uid-1")
	if res := ack(); res != nil {
		t.Fatalf("first ack: %v", res)
	}
	write("uid-2") // the instance fails again
	if active() != 1 {
		t.Fatalf("the second recovery must be active")
	}
	res := ack() // same second as the first ack, almost certainly
	if res == nil {
		t.Logf("both acknowledgements were applied (they fell in different seconds)")
		return
	}
	t.Logf("DOCUMENTED: the second ack-recovery in the same second fails (%v) and leaves the recovery active (%d active); a node that applies it one second later succeeds: the nodes diverge", res, active())
	if active() != 1 {
		t.Errorf("a failed ack must leave the recovery active")
	}
}
