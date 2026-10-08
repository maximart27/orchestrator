package logic

import (
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/inst"
)

// ack-recovery closes the active period of a recovery with an end_active_period_unixtime that is part
// of the unique index (hostname, port, in_active_period, end_active_period_unixtime) of
// topology_recovery. Stamped with the second of the clock of the node that applies the entry, two
// recoveries of an instance acknowledged within the same second collided on a node that applied them
// in that second and not on a node that applied them in different seconds: the nodes diverged.
// The acknowledgement must succeed, with the same effect, on every node whatever its clock says.
func TestApplierAckTwiceInTheSameSecondIsTheSameOnEveryNode(t *testing.T) {
	key := inst.InstanceKey{Hostname: "db1", Port: 3306}
	analysis := inst.ReplicationAnalysis{AnalyzedInstanceKey: key, Analysis: inst.DeadMaster, CountReplicas: 1,
		ClusterDetails: inst.ClusterInfo{ClusterName: "cl", ClusterAlias: "al"}}

	// the same entries, applied by a node that applies the two acks in the same second and by a
	// node that applies them more than a second apart
	run := func(t *testing.T, pause time.Duration) (state string, ends []int64) {
		m, _ := newTestManager(t, []string{"a"}, nil)
		tn := mustTenant(t, m, "a")
		applier := NewCommandApplier(m)
		write := func(uid string) {
			if res := applier.ApplyTenantCommand("a", "write-recovery", mustJSON(t, &TopologyRecovery{UID: uid, AnalysisEntry: analysis})); res != nil {
				t.Fatalf("write-recovery %s: %v", uid, res)
			}
		}
		ack := func() {
			if res := applier.ApplyTenantCommand("a", "ack-recovery", mustJSON(t, RecoveryAcknowledgement{Owner: "me", Comment: "c", AllRecoveries: true})); res != nil {
				t.Fatalf("ack-recovery: %v", res)
			}
		}
		write("uid-1")
		ack()
		write("uid-2") // the instance fails again
		time.Sleep(pause)
		ack()
		if err := tn.Backend.QueryRowsMap("select end_active_period_unixtime as e from topology_recovery order by recovery_id", func(m sqlutils.RowMap) error {
			ends = append(ends, m.GetInt64("e"))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return tenantDump(t, tn), ends
	}
	sameSecond, endsSame := run(t, 0)
	otherSecond, endsOther := run(t, 1100*time.Millisecond)
	if sameSecond != otherSecond {
		t.Errorf("the nodes diverged:\n--- both acks in the same second\n%s\n--- acks in different seconds\n%s", sameSecond, otherSecond)
	}
	for _, ends := range [][]int64{endsSame, endsOther} {
		if len(ends) != 2 || ends[0] == 0 || ends[1] == 0 || ends[0] == ends[1] {
			t.Errorf("the two recoveries must be closed with distinct, non zero stamps, got %v", ends)
		}
	}
}

// Acknowledging again what is already acknowledged is a no-op, and the stamp of a closed recovery is kept.
func TestApplierAckIsIdempotentAndKeepsTheStamp(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	tn := mustTenant(t, m, "a")
	applier := NewCommandApplier(m)
	analysis := inst.ReplicationAnalysis{AnalyzedInstanceKey: inst.InstanceKey{Hostname: "db1", Port: 3306}, Analysis: inst.DeadMaster, CountReplicas: 1,
		ClusterDetails: inst.ClusterInfo{ClusterName: "cl"}}
	if res := applier.ApplyTenantCommand("a", "write-recovery", mustJSON(t, &TopologyRecovery{UID: "u", AnalysisEntry: analysis})); res != nil {
		t.Fatal(res)
	}
	ack := mustJSON(t, RecoveryAcknowledgement{Owner: "me", Comment: "c", UID: "u"})
	stamp := func() (e int64) {
		_ = tn.Backend.QueryRowsMap("select end_active_period_unixtime as e from topology_recovery", func(m sqlutils.RowMap) error { e = m.GetInt64("e"); return nil })
		return e
	}
	if res := applier.ApplyTenantCommand("a", "ack-recovery", ack); res != nil {
		t.Fatal(res)
	}
	first := stamp()
	time.Sleep(1100 * time.Millisecond)
	if res := applier.ApplyTenantCommand("a", "ack-recovery", ack); res != nil {
		t.Fatal(res)
	}
	if first == 0 || stamp() != first {
		t.Errorf("stamp %d, then %d: re-acknowledging must not move it", first, stamp())
	}
}
