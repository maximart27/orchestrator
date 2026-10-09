package logic

import (
	"fmt"
	"testing"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/inst"
)

func replicatedRecovery(id int64, uid, host string) *TopologyRecovery {
	return &TopologyRecovery{
		Id:  id,
		UID: uid,
		AnalysisEntry: inst.ReplicationAnalysis{
			AnalyzedInstanceKey: inst.InstanceKey{Hostname: host, Port: 3306},
			Analysis:            inst.DeadMaster,
			ClusterDetails:      inst.ClusterInfo{ClusterName: "cl", ClusterAlias: "al"},
		},
	}
}

func applyWriteRecovery(t *testing.T, applier *CommandApplier, tr *TopologyRecovery) {
	t.Helper()
	if res := applier.ApplyTenantCommand("a", "write-recovery", mustJSON(t, tr)); res != nil {
		t.Fatalf("write-recovery %s: %v", tr.UID, res)
	}
}

// recoveryRows renders the rows of topology_recovery as "uid:active/end" sorted by id.
func recoveryRows(t *testing.T, tn *Tenant) []string {
	t.Helper()
	var rows []string
	if err := tn.Backend.QueryRowsMap(`select uid, in_active_period, end_active_period_unixtime from topology_recovery order by recovery_id`,
		func(m sqlutils.RowMap) error {
			state := "active"
			if m.GetInt("in_active_period") == 0 {
				state = "closed"
			}
			rows = append(rows, fmt.Sprintf("%s:%s", m.GetString("uid"), state))
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	return rows
}

// A follower closes the active period of its recoveries by its own clock, so it can still hold the
// previous recovery of an instance as active when the leader replicates the next one. The row must
// not be dropped by the unique index (hostname, port, in_active_period, end_active_period_unixtime).
func TestWriteRecoveryIsNotLostWhileThePreviousRecoveryIsStillActive(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	a := mustTenant(t, m, "a")
	applier := NewCommandApplier(m)

	applyWriteRecovery(t, applier, replicatedRecovery(1, "uid-1", "db1"))
	// row 1 is still active here (this node has not cleared it yet)
	applyWriteRecovery(t, applier, replicatedRecovery(2, "uid-2", "db1"))

	if got := fmt.Sprint(recoveryRows(t, a)); got != "[uid-1:closed uid-2:active]" {
		t.Fatalf("the second recovery was lost or both are active: %s", got)
	}
}

// The sequence of the chaos scenario 4: the node clears the first recovery and applies the next one
// within the same second, then keeps clearing/applying further ones.
func TestWriteRecoveryAfterALocalClearInTheSameSecond(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	a := mustTenant(t, m, "a")
	a.Cfg.RecoveryPeriodBlockSeconds = 1
	applier := NewCommandApplier(m)
	age := func() {
		execOn(t, a, `update topology_recovery set start_active_period = now() - interval 10 second where in_active_period = 1`)
	}

	applyWriteRecovery(t, applier, replicatedRecovery(1, "uid-1", "db1"))
	age()
	if err := a.ClearActiveRecoveries(); err != nil {
		t.Fatal(err)
	}
	applyWriteRecovery(t, applier, replicatedRecovery(2, "uid-2", "db1"))
	// the next one arrives while row 2 is active, right after row 1 was closed in this very second
	applyWriteRecovery(t, applier, replicatedRecovery(3, "uid-3", "db1"))
	if got := fmt.Sprint(recoveryRows(t, a)); got != "[uid-1:closed uid-2:closed uid-3:active]" {
		t.Fatalf("rows: %s", got)
	}
}

// Raft replays entries over a backend that kept its data: re-applying a recovery that came before a
// later one must neither duplicate it nor close the later one.
func TestWriteRecoveryReplayDoesNotCloseALaterRecovery(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	a := mustTenant(t, m, "a")
	applier := NewCommandApplier(m)
	one, two := replicatedRecovery(1, "uid-1", "db1"), replicatedRecovery(2, "uid-2", "db1")

	applyWriteRecovery(t, applier, one)
	applyWriteRecovery(t, applier, two)
	applyWriteRecovery(t, applier, one)
	applyWriteRecovery(t, applier, two)
	if got := fmt.Sprint(recoveryRows(t, a)); got != "[uid-1:closed uid-2:active]" {
		t.Fatalf("rows after the replay: %s", got)
	}
}

// What the leader registers is applied again by its own FSM: no effect, and another instance's active
// recovery is untouched by a recovery of this one.
func TestWriteRecoveryOfAnotherInstanceLeavesActiveRecoveriesAlone(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	a := mustTenant(t, m, "a")
	applier := NewCommandApplier(m)

	registered, err := a.registerTopologyRecovery(replicatedRecovery(0, "uid-1", "db1"))
	if err != nil || registered == nil {
		t.Fatalf("registering: %v, %v", registered, err)
	}
	if again, err := a.registerTopologyRecovery(replicatedRecovery(0, "uid-x", "db1")); err != nil || again != nil {
		t.Fatalf("a second active recovery of the instance must be refused (nil, nil), got %v, %v", again, err)
	}
	applyWriteRecovery(t, applier, registered) // the leader's own application
	applyWriteRecovery(t, applier, replicatedRecovery(7, "uid-2", "db2"))
	if got := fmt.Sprint(recoveryRows(t, a)); got != "[uid-1:active uid-2:active]" {
		t.Fatalf("rows: %s", got)
	}
}
