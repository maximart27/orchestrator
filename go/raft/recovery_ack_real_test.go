package orcraft_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"

	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/logic"
)

// exactAuditState renders the recovery audit tables of a tenant with EVERY column, the time stamps
// included: what the other digests of this package leave out because the nodes used to stamp them
// with their own clocks.
func exactAuditState(t testing.TB, tn *logic.Tenant) string {
	t.Helper()
	var out strings.Builder
	for _, table := range []string{"topology_recovery", "topology_recovery_steps", "topology_failure_detection"} {
		var rows []string
		if err := tn.Backend.QueryRowsMap("select * from "+table, func(m sqlutils.RowMap) error {
			var cols []string
			for c := range m {
				cols = append(cols, c)
			}
			sort.Strings(cols)
			var cells []string
			for _, c := range cols {
				cells = append(cells, fmt.Sprintf("%s=%s", c, m.GetString(c)))
			}
			rows = append(rows, strings.Join(cells, " | "))
			return nil
		}); err != nil {
			t.Fatalf("dumping %s: %v", table, err)
		}
		sort.Strings(rows)
		fmt.Fprintf(&out, "[%s] %d\n", table, len(rows))
		for _, r := range rows {
			out.WriteString("  " + r + "\n")
		}
	}
	return out.String()
}

// assertSameAuditState waits (Raft's AppliedIndex counts what was handed to the FSM, not what it has
// applied) until every node holds the same recovery rows, time stamps included.
func assertSameAuditState(t *testing.T, reals []*realApp, tenant string) {
	t.Helper()
	diff := func() (string, string) {
		want := exactAuditState(t, reals[0].tenant(tenant))
		for i := 1; i < len(reals); i++ {
			if got := exactAuditState(t, reals[i].tenant(tenant)); got != want {
				return want, fmt.Sprintf("node%d holds other recovery rows than node0:\n--- node0\n%s--- node%d\n%s", i, want, i, got)
			}
		}
		return want, ""
	}
	deadline := time.Now().Add(10 * time.Second)
	want, d := diff()
	for d != "" && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		want, d = diff()
	}
	if d != "" {
		t.Error(d)
	}
	if !strings.Contains(want, "acknowledge_comment=detected crashed recovery") || !strings.Contains(want, "uid-new") {
		t.Errorf("the orphan recovery was never acknowledged as crashed, or the new recovery is missing:\n%s", want)
	}
}

// orphanRecoveryOfADeadLeader publishes, through the leader ld, what a leader leaves behind when it dies in the
// middle of a failover: the detection, the recovery (originated by it) and a step.
func orphanRecoveryOfADeadLeader(t *testing.T, c *chaosCluster, ld int, tenant string) {
	t.Helper()
	analysis := inst.ReplicationAnalysis{
		AnalyzedInstanceKey: inst.InstanceKey{Hostname: "db1", Port: 3306},
		Analysis:            inst.DeadMaster,
		ClusterDetails:      inst.ClusterInfo{ClusterName: "cl", ClusterAlias: "al"},
		StartActivePeriod:   "2026-10-09 00:55:19",
	}
	mustPublish(t, c, ld, tenant, "register-failure-detection", jsonOf(t, analysis))
	orphan := &logic.TopologyRecovery{
		UID: "uid-orphan", AnalysisEntry: analysis,
		ProcessingNodeHostname: "dead-leader", ProcessingNodeToken: "dead-token",
		RecoveryStartTimestamp: "2026-10-09 00:55:20",
	}
	mustPublish(t, c, ld, tenant, "write-recovery", jsonOf(t, orphan))
	step := logic.NewTopologyRecoveryStep("uid-orphan", "Running 1 PreFailoverProcesses hooks")
	step.AuditAt = "2026-10-09 00:55:21"
	mustPublish(t, c, ld, tenant, "write-recovery-step", jsonOf(t, step))
}

// afterTheKill is what the new leader does once the old one is dead: acknowledge the crashed recovery of
// the dead node (as AcknowledgeCrashedRecoveries publishes it) and run its own recovery of the instance.
func afterTheKill(t *testing.T, c *chaosCluster, reals []*realApp, nl int, tenant string) {
	t.Helper()
	reals[nl].leader.Store(true)
	tn := reals[nl].tenant(tenant)
	acks, err := tn.CrashedRecoveryAcknowledgements()
	if err != nil || len(acks) != 1 {
		t.Fatalf("the new leader must find one crashed recovery: %v, %v", acks, err)
	}
	for _, ack := range acks {
		mustPublish(t, c, nl, tenant, "ack-recovery", jsonOf(t, ack))
	}
	analysis := inst.ReplicationAnalysis{
		AnalyzedInstanceKey: inst.InstanceKey{Hostname: "db1", Port: 3306},
		Analysis:            inst.DeadMaster,
		ClusterDetails:      inst.ClusterInfo{ClusterName: "cl", ClusterAlias: "al"},
	}
	second := &logic.TopologyRecovery{
		UID: "uid-new", AnalysisEntry: analysis,
		ProcessingNodeHostname: "new-leader", ProcessingNodeToken: "new-token",
		RecoveryStartTimestamp: "2026-10-09 00:55:23",
	}
	mustPublish(t, c, nl, tenant, "write-recovery", jsonOf(t, second))
	second.IsSuccessful = true
	second.SuccessorKey = &inst.InstanceKey{Hostname: "db2", Port: 3306}
	second.RecoveryEndTimestamp = "2026-10-09 00:55:43"
	mustPublish(t, c, nl, tenant, "resolve-recovery", jsonOf(t, second))
}

// kill -9 of the leader in the middle of a failover: the new leader acknowledges the orphan recovery,
// and the node that comes back (applying, later and with its own clock, what it missed) ends with the
// very same rows as the others: timestamps included.
func TestRealCrashedRecoveryAckConvergesAfterKillingTheLeader(t *testing.T) {
	tenants := []string{"a"}
	apps, reals := newRealApps(t, 3, tenants, tenants, realHosts...)
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	orphanRecoveryOfADeadLeader(t, c, ld, "a")
	c.WaitConverged(30 * time.Second)

	c.Stop(ld) // kill -9
	var rest []int
	for i := range reals {
		if i != ld {
			rest = append(rest, i)
		}
	}
	nl := c.WaitLeader(rest...)
	afterTheKill(t, c, reals, nl, "a")
	time.Sleep(1200 * time.Millisecond) // the restarted node applies the entries it missed at another second
	c.Restart(ld)
	c.WaitConverged(60 * time.Second)
	t.Logf("old leader node%d, new leader node%d", ld, nl)
	assertSameAuditState(t, reals, "a")
	c.AssertIdentical(tenants...)
}

// The same, for the nodes that get the state through a snapshot (the restarted leader after the log was
// compacted, and a node that joins from scratch) and, from then on, the entries of the log.
func TestRealCrashedRecoveryAckConvergesThroughSnapshots(t *testing.T) {
	tenants := []string{"a"}
	apps, reals := newRealApps(t, 3, tenants, tenants, realHosts...)
	c := newChaosCluster(t, apps, realCompactingConfig)
	ld := c.WaitLeader()
	orphanRecoveryOfADeadLeader(t, c, ld, "a")
	c.WaitConverged(30 * time.Second)

	c.Stop(ld)
	var rest []int
	for i := range reals {
		if i != ld {
			rest = append(rest, i)
		}
	}
	nl := c.WaitLeader(rest...)
	afterTheKill(t, c, reals, nl, "a")
	publishAdditive(t, c, nl, 60, tenants)
	c.WaitConverged(30 * time.Second)
	waitCompacted(t, c, nl)
	time.Sleep(1200 * time.Millisecond)
	c.Restart(ld)
	fresh := newRealApp(t, tenants, tenants)
	fresh.seedInstances(realHosts...)
	c.AddNode(fresh)
	c.WaitConverged(60 * time.Second)
	assertSameAuditState(t, append(reals, fresh), "a")
}
