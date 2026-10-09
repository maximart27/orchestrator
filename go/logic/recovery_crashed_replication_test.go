package logic

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"

	"github.com/proxysql/orchestrator/go/process"
)

// recoveryRowsExact renders every column of topology_recovery, time stamps included, one line per row;
// the columns in skip are left out.
func recoveryRowsExact(t testing.TB, tn *Tenant, skip ...string) string {
	t.Helper()
	skipped := map[string]bool{}
	for _, c := range skip {
		skipped[c] = true
	}
	var rows []string
	if err := tn.Backend.QueryRowsMap(`select * from topology_recovery order by recovery_id`, func(m sqlutils.RowMap) error {
		var cols []string
		for c := range m {
			if !skipped[c] {
				cols = append(cols, c)
			}
		}
		sort.Strings(cols)
		var cells []string
		for _, c := range cols {
			cells = append(cells, fmt.Sprintf("%s=%s", c, m.GetString(c)))
		}
		rows = append(rows, strings.Join(cells, " | "))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return strings.Join(rows, "\n")
}

// raftOf makes the tenant publish to the appliers of the given nodes, in the order of the log, as the
// leader's Raft would: every node applies what the leader publishes, the leader included. A node applies
// more than a second after the previous one, so that nothing can depend on the clock of the node.
func raftOf(t *testing.T, appliers ...*CommandApplier) {
	t.Helper()
	raftRecoveries(t, true)
	prev := recoveryPublish
	recoveryPublish = func(tenantID, op string, value interface{}) (interface{}, error) {
		b := mustJSON(t, value)
		for i, a := range appliers {
			if i > 0 {
				time.Sleep(1100 * time.Millisecond)
			}
			if res := a.ApplyTenantCommand(tenantID, op, b); res != nil {
				return nil, fmt.Errorf("%v", res)
			}
		}
		return nil, nil
	}
	t.Cleanup(func() { recoveryPublish = prev })
}

// The leader that closes the recovery of a crashed node publishes the acknowledgement: every node ends
// with the same acknowledged row, down to the instant (acknowledged_at, end_recovery, the end stamp of
// the active period). Applied locally on the leader alone, the followers kept the recovery open.
func TestCrashedRecoveryAcknowledgementIsReplicated(t *testing.T) {
	leaderM, leaderLeadership := newTestManager(t, []string{"a"}, nil)
	followerM, _ := newTestManager(t, []string{"a"}, nil)
	leader, follower := mustTenant(t, leaderM, "a"), mustTenant(t, followerM, "a")
	leaderApplier, followerApplier := NewCommandApplier(leaderM), NewCommandApplier(followerM)

	// the recovery the dead leader originated, replicated to both
	orphan := replicatedRecovery(1, "uid-orphan", "db1")
	orphan.ProcessingNodeHostname, orphan.ProcessingNodeToken = "dead-leader", "dead-token"
	applyWriteRecovery(t, leaderApplier, orphan)
	applyWriteRecovery(t, followerApplier, orphan)
	leaderM.Control().Heartbeat(process.ThisNodeHealth)

	raftOf(t, leaderApplier, followerApplier)

	// a follower acknowledges nothing, and publishes nothing
	if acks, err := follower.CrashedRecoveryAcknowledgements(); err != nil || len(acks) != 0 {
		t.Fatalf("a follower has acknowledgements to publish: %v, %v", acks, err)
	}
	leaderLeadership.leader.Store(true)
	if n, err := leader.AcknowledgeCrashedRecoveries(); err != nil || n != 1 {
		t.Fatalf("the leader acknowledged %d, %v", n, err)
	}
	if n := countActiveRecoveries(t, leader) + countActiveRecoveries(t, follower); n != 0 {
		t.Fatalf("an orphan recovery is still active (%d)", n)
	}
	// start_active_period is the instant each node applied write-recovery at: stamped by the leader in the next commit
	l, f := recoveryRowsExact(t, leader, "start_active_period"), recoveryRowsExact(t, follower, "start_active_period")
	if !strings.Contains(l, "acknowledged=1") || !strings.Contains(l, "acknowledge_comment=detected crashed recovery") || !strings.Contains(l, "acknowledged_by=orchestrator") {
		t.Fatalf("the leader did not acknowledge the recovery as crashed:\n%s", l)
	}
	if l != f {
		t.Errorf("the nodes hold different rows:\nleader   %s\nfollower %s", l, f)
	}
}

// A recovery the leader stamped is written with the leader's instants on every node, however late a node
// applies it (a follower that was down, a node that restarts and replays the log): start_active_period,
// the end stamp with which the next recovery of the instance closes it, end_recovery.
func TestReplicatedRecoveryCarriesTheLeadersInstants(t *testing.T) {
	apply := func(t *testing.T, pause time.Duration) string {
		m, _ := newTestManager(t, []string{"a"}, nil)
		tn := mustTenant(t, m, "a")
		applier := NewCommandApplier(m)
		time.Sleep(pause)
		first := replicatedRecovery(1, "uid-1", "db1")
		first.RecoveryStartTimestamp = "2026-10-09 00:55:20"
		second := replicatedRecovery(2, "uid-2", "db1")
		second.RecoveryStartTimestamp = "2026-10-09 00:55:23"
		applyWriteRecovery(t, applier, first)
		applyWriteRecovery(t, applier, second) // the first one is still active here: it is closed by the arrival of the second
		resolved := replicatedRecovery(2, "uid-2", "db1")
		resolved.IsSuccessful = true
		resolved.RecoveryEndTimestamp = "2026-10-09 00:55:43"
		resolved.SuccessorKey = &resolved.AnalysisEntry.AnalyzedInstanceKey
		if res := applier.ApplyTenantCommand("a", "resolve-recovery", mustJSON(t, resolved)); res != nil {
			t.Fatal(res)
		}
		return recoveryRowsExact(t, tn)
	}
	onTime, late := apply(t, 0), apply(t, 1100*time.Millisecond)
	if onTime != late {
		t.Errorf("a node that applied the same entries a second later holds other rows:\n%s\n---\n%s", onTime, late)
	}
	for _, want := range []string{"start_active_period=2026-10-09T00:55:20Z", "start_active_period=2026-10-09T00:55:23Z", "end_recovery=2026-10-09T00:55:43Z"} {
		if !strings.Contains(onTime, want) {
			t.Errorf("the rows lack %q:\n%s", want, onTime)
		}
	}
	// the first recovery was closed at the instant the second started
	if want := fmt.Sprintf("end_active_period_unixtime=%d", time.Date(2026, 10, 9, 0, 55, 23, 0, time.UTC).Unix()); !strings.Contains(onTime, want) {
		t.Errorf("the first recovery is not closed at the start of the second (%s):\n%s", want, onTime)
	}
}

// A leader that registers a recovery under Raft stamps the entry it publishes with the instant it wrote in
// its own row, so the followers write the very same row (down to start_active_period).
func TestRegisteredRecoveryIsPublishedWithItsInstant(t *testing.T) {
	leaderM, _ := newTestManager(t, []string{"a"}, nil)
	followerM, _ := newTestManager(t, []string{"a"}, nil)
	leader, follower := mustTenant(t, leaderM, "a"), mustTenant(t, followerM, "a")
	raftRecoveries(t, true)

	registered, err := leader.registerTopologyRecovery(replicatedRecovery(0, "uid-1", "db1"))
	if err != nil || registered == nil {
		t.Fatalf("registering: %v, %v", registered, err)
	}
	if registered.RecoveryStartTimestamp == "" {
		t.Fatal("the entry to publish carries no instant")
	}
	time.Sleep(1100 * time.Millisecond)
	applyWriteRecovery(t, NewCommandApplier(followerM), registered)
	if l, f := recoveryRowsExact(t, leader), recoveryRowsExact(t, follower); l != f {
		t.Errorf("the follower wrote another row:\nleader   %s\nfollower %s", l, f)
	}

	// without Raft nothing is stamped: the clock of the backend is the only one
	alone, _ := newTestManager(t, []string{"a"}, nil)
	raftRecoveries(t, false)
	plain, err := mustTenant(t, alone, "a").registerTopologyRecovery(replicatedRecovery(0, "uid-2", "db2"))
	if err != nil || plain == nil || plain.RecoveryStartTimestamp != "" {
		t.Fatalf("without Raft: %+v, %v", plain, err)
	}
}

// The detection the leader publishes carries the instant it registered it at.
func TestReplicatedFailureDetectionCarriesTheLeadersInstant(t *testing.T) {
	startOf := func(t *testing.T, pause time.Duration) string {
		m, _ := newTestManager(t, []string{"a"}, nil)
		tn := mustTenant(t, m, "a")
		analysis := replicatedRecovery(0, "", "db1").AnalysisEntry
		analysis.StartActivePeriod = "2026-10-09 00:55:20"
		time.Sleep(pause)
		if res := NewCommandApplier(m).ApplyTenantCommand("a", "register-failure-detection", mustJSON(t, analysis)); res != nil {
			t.Fatal(res)
		}
		var start string
		if err := tn.Backend.QueryRowsMap(`select start_active_period from topology_failure_detection`, func(m sqlutils.RowMap) error {
			start = m.GetString("start_active_period")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return start
	}
	if a, b := startOf(t, 0), startOf(t, 1100*time.Millisecond); a != b || a != "2026-10-09T00:55:20Z" {
		t.Errorf("start_active_period of the detection: %q and %q", a, b)
	}
}

// A node that restores a snapshot holds the same NULLs as the node that took it: a recovery that is not
// finished keeps end_recovery NULL (and acknowledged_at, the acknowledgement being absent), which is what
// the recognition of a crashed recovery looks for. A NULL restored as an empty string made an orphan
// recovery of a restored node invisible, and the rows differ from the other nodes'.
func TestSnapshotRestoreKeepsTheNullsOfARecovery(t *testing.T) {
	leaderM, _ := newTestManager(t, []string{"a"}, nil)
	restoredM, restoredLeadership := newTestManager(t, []string{"a"}, nil)
	leader, restored := mustTenant(t, leaderM, "a"), mustTenant(t, restoredM, "a")
	orphan := replicatedRecovery(1, "uid-orphan", "db1")
	orphan.ProcessingNodeHostname, orphan.ProcessingNodeToken = "dead-leader", "dead-token"
	applyWriteRecovery(t, NewCommandApplier(leaderM), orphan)
	restoreInto(t, restoredM, snapshotOf(t, leaderM))

	if l, r := recoveryRowsExact(t, leader), recoveryRowsExact(t, restored); l != r {
		t.Errorf("the restored node holds another row:\nleader   %s\nrestored %s", l, r)
	}
	restoredLeadership.leader.Store(true)
	acks, err := restored.CrashedRecoveryAcknowledgements()
	if err != nil || len(acks) != 1 {
		t.Fatalf("the orphan recovery of a restored node must be recognised as crashed: %v, %v", acks, err)
	}
}
