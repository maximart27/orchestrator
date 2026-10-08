package orcraft_test

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/proxysql/golib/sqlutils"

	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/logic"
	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// additiveOp draws a command that only ADDS or UPDATES state (upserts): the kind a snapshot
// restore, which is additive too, reproduces exactly. n makes every command distinct.
func additiveOp(t testing.TB, n int, tenant string) (op string, value []byte) {
	key := inst.InstanceKey{Hostname: realHosts[n%len(realHosts)], Port: 3306}
	switch n % 6 {
	case 0:
		return "put-instance-tag", jsonOf(t, inst.InstanceTag{Key: key, T: inst.Tag{TagName: fmt.Sprintf("tag%d", n), TagValue: "tenant:" + tenant}})
	case 1:
		return "register-candidate", jsonOf(t, inst.CandidateDatabaseInstance{Hostname: key.Hostname, Port: key.Port, PromotionRule: inst.PreferPromoteRule})
	case 2:
		return "set-cluster-alias-manual-override", jsonOf(t, [2]string{fmt.Sprintf("cluster%d", n), "alias:" + tenant})
	case 3:
		return "put-key-value", jsonOf(t, map[string]string{"Key": fmt.Sprintf("%s/k%d", tenant, n), "Value": "tenant:" + tenant})
	case 4:
		return "submit-pool-instances", jsonOf(t, inst.PoolInstancesSubmission{Pool: fmt.Sprintf("pool-%s-%d", tenant, n), DelimitedInstances: "db1:3306,db2:3306"})
	default:
		return "register-hostname-unresolve", jsonOf(t, inst.HostnameRegistration{Key: inst.InstanceKey{Hostname: fmt.Sprintf("h%d", n), Port: 3306}, Hostname: "unres-" + tenant})
	}
}

// realCompactingConfig compacts the log often enough for the tests, but not on every entry (a real
// snapshot reads every table of every tenant).
func realCompactingConfig(cfg *raft.Config) {
	cfg.SnapshotThreshold = 16
	cfg.TrailingLogs = 4
	cfg.SnapshotInterval = 150 * time.Millisecond
}

func publishAdditive(t *testing.T, c *chaosCluster, ld, count int, tenants []string) {
	t.Helper()
	for n := 0; n < count; n++ {
		tn := tenants[n%len(tenants)]
		op, v := additiveOp(t, n, tn)
		mustPublish(t, c, ld, tn, op, v)
	}
}

// Scenario 4 with the real appliers: a follower that was down while the log was compacted comes
// back through a snapshot RESTORE (2 tenants...) and holds exactly the state of the others.
func TestRealFollowerRestoresV2SnapshotAfterCompaction(t *testing.T) {
	tenants := []string{"a", "b", "c"}
	apps, reals := newRealApps(t, 3, tenants, tenants, realHosts...)
	c := newChaosCluster(t, apps, realCompactingConfig)
	ld := c.WaitLeader()
	down := (ld + 1) % 3
	c.Stop(down)
	publishAdditive(t, c, ld, 60, tenants)
	c.WaitConverged(30 * time.Second)
	waitCompacted(t, c, ld)
	firstBefore, snapsBefore := c.logState(down)
	c.Restart(down)
	c.WaitConverged(60 * time.Second)
	c.AssertIdentical(tenants...)
	first, snaps := c.logState(down)
	t.Logf("follower log: first index %d -> %d, snapshots %d -> %d (a received snapshot is stored as one)", firstBefore, first, snapsBefore, snaps)
	if snaps <= snapsBefore && first <= firstBefore {
		t.Errorf("the follower must have been caught up through a snapshot")
	}
	for _, id := range tenants {
		assertNoCrossTenantLeak(t, down, id, dumpTenant(t, reals[down].tenant(id)), tenants)
	}
}

// Scenario 4: a brand new node (empty Raft data, empty tenant backends) joins a cluster whose
// log was truncated: everything arrives through the snapshot, each tenant in its own backend.
func TestRealNewNodeJoinsTruncatedLog(t *testing.T) {
	tenants := []string{"a", "b"}
	apps, _ := newRealApps(t, 3, tenants, tenants, realHosts...)
	c := newChaosCluster(t, apps, realCompactingConfig)
	ld := c.WaitLeader()
	publishAdditive(t, c, ld, 48, tenants)
	c.WaitConverged(30 * time.Second)
	waitCompacted(t, c, ld)
	fresh := newRealApp(t, tenants, tenants)
	idx := c.AddNode(fresh)
	c.WaitConverged(60 * time.Second)
	c.AssertIdentical(tenants...)
	_, snaps := c.logState(idx)
	if snaps == 0 {
		t.Errorf("the new node must have received a snapshot")
	}
}

func newRealApp0(t *testing.T, all, active []string, seed ...string) *realApp {
	a := newRealApp(t, all, active)
	a.seedInstances(seed...)
	return a
}

// Scenario 5 with the real appliers. Node2 runs tenants a and b only; the others also run x.
// Then the operator adds x to node2 (new tenants file, restart). Does x get what it lost?
func TestRealTenantsDifferBetweenNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	for _, withOwnSnapshot := range []bool{false, true} {
		withOwnSnapshot := withOwnSnapshot
		t.Run(fmt.Sprintf("node2SnapshotsBeforeBeingFixed=%v", withOwnSnapshot), func(t *testing.T) {
			start := time.Now()
			phase := func(name string) { t.Logf("%6.1fs %s", time.Since(start).Seconds(), name) }
			all := []string{"a", "b", "x"}
			app0, app1 := newRealApp0(t, all, all, realHosts...), newRealApp0(t, all, all, realHosts...)
			app2 := newRealApp0(t, all, []string{"a", "b"}, realHosts...)
			phase("apps built")
			c := newChaosCluster(t, []nodeApp{app0, app1, app2}, nil)
			ld := c.WaitLeader()
			phase("leader elected")
			for ld == 2 {
				_ = c.raftOf(2).LeadershipTransfer().Error()
				ld = c.WaitLeader()
			}
			before := orcraft.UnknownTenantCommands()
			const rounds = 4
			for n := 0; n < rounds; n++ {
				for _, tn := range all {
					op, v := additiveOp(t, n, tn)
					mustPublish(t, c, ld, tn, op, v)
				}
			}
			c.WaitConverged(30 * time.Second)
			phase("published and converged")
			if got := orcraft.UnknownTenantCommands() - before; got != rounds {
				t.Errorf("UnknownTenantCommands rose by %d, want %d", got, rounds)
			}
			c.AssertIdentical("a", "b") // the node neither blocks nor diverges on the tenants it runs
			if app0.Digest("x") != app1.Digest("x") {
				t.Errorf("the nodes that run x must agree on it")
			}
			if got := app2.Digest("x"); got != "<tenant not running on this node>" {
				t.Errorf("node2 must not have a tenant x: %s", got)
			}
			for _, id := range []string{"a", "b"} { // no command of x reached a or b on node2
				assertNoCrossTenantLeak(t, 2, id, dumpTenant(t, app2.tenant(id)), all)
			}

			if withOwnSnapshot {
				// the periodic snapshot (every 30 minutes in production) of node2, after it dropped the commands
				if err := c.raftOf(2).Snapshot().Error(); err != nil {
					t.Fatal(err)
				}
			}
			phase("snapshot step done")
			c.Stop(2)
			phase("node2 stopped")
			app2.rebuild(all)
			app2.seedInstances(realHosts...) // the new tenant has discovered the same topology as on the other nodes
			phase("node2 rebuilt")
			c.Restart(2)
			c.WaitConverged(60 * time.Second)
			phase("node2 restarted and converged")
			healed := false
			deadline := time.Now().Add(20 * time.Second)
			if withOwnSnapshot { // nothing will come: just make sure the replay of the tail is over
				deadline = time.Now().Add(3 * time.Second)
			}
			for time.Now().Before(deadline) && !healed {
				time.Sleep(100 * time.Millisecond)
				healed = app0.Digest("x") == app2.Digest("x")
			}
			c.AssertIdentical("a", "b")
			t.Logf("after adding x to node2 and restarting it: x converged=%v (node2 had snapshotted after the loss: %v)", healed, withOwnSnapshot)
			if withOwnSnapshot && healed {
				t.Errorf("the dropped commands are behind node2's own snapshot: Raft cannot replay them")
			}
			if !withOwnSnapshot && !healed {
				t.Errorf("without a snapshot of its own node2 replays the whole log over the new tenant and gets x")
			}
			if !healed {
				// the manual repair: restore the snapshot of a healthy node into node2
				data, err := app0.GetData()
				if err != nil {
					t.Fatal(err)
				}
				if err := app2.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
					t.Fatal(err)
				}
				if app0.Digest("x") != app2.Digest("x") {
					t.Errorf("restoring a healthy node's snapshot into node2 must repair x")
				}
				t.Logf("repaired by restoring the snapshot of node0 into node2 (SnapshotDataCreatorApplier.Restore)")
			}
		})
	}
}

func stepCount(t *testing.T, tn *logic.Tenant) (n int) {
	t.Helper()
	if err := tn.Backend.QueryRowsMap("select count(*) as c from topology_recovery_steps", func(m sqlutils.RowMap) error {
		n = m.GetInt("c")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// Scenario 7: a node restarted over its existing backend replays the Raft log that follows its
// last snapshot (the whole log if it has none). The commands are idempotent except the recovery
// steps: the node ends up with them twice. Everything else stays identical.
func TestRealRestartReplayDuplicatesNonIdempotentSteps(t *testing.T) {
	tenants := []string{"a", "b"}
	apps, reals := newRealApps(t, 3, tenants, tenants, realHosts...)
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	key := inst.InstanceKey{Hostname: "db1", Port: 3306}
	mustPublish(t, c, ld, "a", "write-recovery", jsonOf(t, &logic.TopologyRecovery{UID: "uid-a-1",
		AnalysisEntry: inst.ReplicationAnalysis{AnalyzedInstanceKey: key, Analysis: inst.DeadMaster}}))
	for n := 0; n < 3; n++ {
		mustPublish(t, c, ld, "a", "write-recovery-step", jsonOf(t, logic.TopologyRecoveryStep{RecoveryUID: "uid-a-1", Message: fmt.Sprintf("step %d", n)}))
	}
	publishAdditive(t, c, ld, 12, tenants)
	c.WaitConverged(30 * time.Second)
	for i, r := range reals {
		if n := stepCount(t, r.tenant("a")); n != 3 {
			t.Fatalf("node%d: %d steps before the restart, want 3", i, n)
		}
	}
	victim := (ld + 1) % 3
	c.Stop(victim)
	c.Restart(victim)
	c.WaitConverged(30 * time.Second)
	time.Sleep(500 * time.Millisecond) // the replay is applied asynchronously
	c.AssertIdentical(tenants...)      // everything but the steps is identical (the digest leaves them out)
	got := stepCount(t, reals[victim].tenant("a"))
	other := stepCount(t, reals[ld].tenant("a"))
	t.Logf("recovery steps after restarting node%d: restarted node=%d, others=%d", victim, got, other)
	if other != 3 {
		t.Errorf("a node that was not restarted must still have 3 steps, has %d", other)
	}
	if got != 6 {
		t.Errorf("DOCUMENTED: write-recovery-step is not idempotent: the replay after a restart should leave 6 rows (3 duplicated), got %d", got)
	}
}

// Scenario 7, at cluster level: SIGTERM stops the TenantManager while Raft is still applying
// (acceptSignals: Stop, then exit). The node's FSM keeps getting commands for a few moments;
// the other nodes must not be affected, and nothing may panic or deadlock.
func TestRealManagerStoppedWhileRaftKeepsApplying(t *testing.T) {
	tenants := []string{"a", "b"}
	apps, reals := newRealApps(t, 3, tenants, tenants, realHosts...)
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	for ld == 1 {
		_ = c.raftOf(1).LeadershipTransfer().Error()
		ld = c.WaitLeader()
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	acked := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			tn := tenants[n%2]
			op, v := additiveOp(t, n, tn)
			if _, err := c.PublishAny(tn, op, v); err == nil {
				mu.Lock()
				acked++
				mu.Unlock()
			}
			time.Sleep(3 * time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	stopped := make(chan struct{})
	go func() { reals[1].stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("stopping the manager of node1 hung while Raft kept applying")
	}
	time.Sleep(400 * time.Millisecond) // Raft keeps delivering commands to the stopped node
	close(stop)
	wg.Wait()
	// nodes 0 and 2 keep a consistent state; node1's FSM is still alive in Raft
	deadline := time.Now().Add(20 * time.Second)
	for {
		same := true
		for _, id := range tenants {
			if apps[0].Digest(id) != apps[2].Digest(id) {
				same = false
			}
		}
		if same {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node0 and node2 diverged while node1's manager was stopped")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if r := c.raftOf(1); r == nil {
		t.Fatal("node1 is not running")
	}
	mu.Lock()
	t.Logf("acked=%d commands; node1's manager was stopped mid-stream, Raft of node1 kept running without panic", acked)
	mu.Unlock()
}
