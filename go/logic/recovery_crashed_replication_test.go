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
