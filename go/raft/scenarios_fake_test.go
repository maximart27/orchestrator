package orcraft_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	orcraft "github.com/proxysql/orchestrator/go/raft"
)

func mustPublish(t *testing.T, c *chaosCluster, i int, tenant, op string, v []byte) {
	t.Helper()
	if _, err := c.Publish(i, tenant, op, v); err != nil {
		t.Fatalf("publish %s/%s through node%d: %v", tenant, op, i, err)
	}
}

// Scenario 2: the leader ends up in a minority. It cannot commit (publishing there
// fails and nothing of it is ever applied), the majority elects a new leader and keeps
// working, and after healing the old leader rejoins and converges, discarding what it
// had not committed.
func TestPartitionMinorityLeaderCannotCommit(t *testing.T) {
	apps, fakes := newFakeApps(3, false, "a", "b")
	c := newChaosCluster(t, apps, nil)
	old := c.WaitLeader()
	mustPublish(t, c, old, "a", "put", fakeVal("before", "1"))
	c.WaitConverged(10 * time.Second)
	oldTerm := c.raftOf(old).CurrentTerm()

	c.Isolate(old)
	// published into the minority: must fail, never be acknowledged
	for i := 0; i < 3; i++ {
		start := time.Now()
		_, err := c.Publish(old, "a", "put", fakeVal(fmt.Sprintf("lost%d", i), "x"))
		if err == nil {
			t.Fatalf("a command published to a leader without quorum was acknowledged")
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Errorf("the minority leader took %v to fail", d)
		}
	}
	if c.raftOf(old).State() == raft.Leader {
		t.Errorf("the isolated leader must give up leadership")
	}
	var majority []int
	for i := 0; i < 3; i++ {
		if i != old {
			majority = append(majority, i)
		}
	}
	nl := c.WaitLeader(majority...)
	if nl == old {
		t.Fatal("the old leader cannot be re-elected while isolated")
	}
	mustPublish(t, c, nl, "b", "put", fakeVal("during", "2"))
	mustPublish(t, c, nl, "a", "put", fakeVal("during", "3"))

	c.Heal()
	c.WaitConverged(15 * time.Second)
	c.AssertIdentical("a", "b")
	for i, f := range fakes {
		if v := f.get("a", "lost0") + f.get("a", "lost1") + f.get("a", "lost2"); v != "" {
			t.Errorf("node%d applied a command that was never committed: %q", i, v)
		}
		if f.get("a", "before") != "1" || f.get("b", "during") != "2" || f.get("a", "during") != "3" {
			t.Errorf("node%d lost committed state: before=%q during=%q/%q", i, f.get("a", "before"), f.get("a", "during"), f.get("b", "during"))
		}
	}
	if got := c.raftOf(old).CurrentTerm(); got <= oldTerm {
		t.Errorf("the old leader must have adopted a newer term (%d <= %d)", got, oldTerm)
	}
	if ls := c.Leaders(); len(ls) != 1 {
		t.Errorf("exactly one leader expected after healing, got %v", ls)
	}
	c.AssertSingleLeaderPerTerm()
}

// Scenario 2, 5 nodes: a symmetric 2/2/1 partition leaves nobody with a quorum: no
// leader can exist, nothing is acknowledged, and healing brings back a single leader
// with every node converging.
func TestPartition221NoQuorumAnywhere(t *testing.T) {
	apps, fakes := newFakeApps(5, false, "a", "b")
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	mustPublish(t, c, ld, "a", "put", fakeVal("before", "1"))
	c.WaitConverged(10 * time.Second)

	c.Partition([]int{0, 1}, []int{2, 3}, []int{4})
	time.Sleep(800 * time.Millisecond) // every former leader steps down; no election can succeed
	if ls := c.Leaders(); len(ls) != 0 {
		// A node can briefly remain Leader until its lease check runs; wait a bit more.
		time.Sleep(time.Second)
		if ls = c.Leaders(); len(ls) != 0 {
			t.Fatalf("no group has a quorum, yet nodes %v believe they lead", ls)
		}
	}
	for i := 0; i < 5; i++ {
		if r := c.raftOf(i); r != nil && r.State() == raft.Leader {
			continue
		}
		if _, err := c.Publish(i, "a", "put", fakeVal("nope", "x")); err == nil {
			t.Errorf("node%d acknowledged a command with no quorum anywhere", i)
		}
	}
	c.Heal()
	nl := c.WaitLeader()
	mustPublish(t, c, nl, "b", "put", fakeVal("after", "2"))
	c.WaitConverged(15 * time.Second)
	c.AssertIdentical("a", "b")
	for i, f := range fakes {
		if f.get("a", "nope") != "" {
			t.Errorf("node%d applied a command refused during the partition", i)
		}
		if f.get("b", "after") != "2" {
			t.Errorf("node%d missed the command after healing", i)
		}
	}
	c.AssertSingleLeaderPerTerm()
}

// Scenario 2, 3/2 split of 5 nodes: the side with the majority keeps committing, the
// minority side (even if it contains the old leader) does not; after healing everything converges.
func TestPartition32MajoritySideProgresses(t *testing.T) {
	apps, fakes := newFakeApps(5, false, "a")
	c := newChaosCluster(t, apps, nil)
	old := c.WaitLeader()
	// put the old leader in the 2-node side
	other := (old + 1) % 5
	var rest []int
	for i := 0; i < 5; i++ {
		if i != old && i != other {
			rest = append(rest, i)
		}
	}
	c.Partition([]int{old, other}, rest)
	if _, err := c.Publish(old, "a", "put", fakeVal("minority", "x")); err == nil {
		t.Error("the 2-node side acknowledged a command")
	}
	nl := c.WaitLeader(rest...)
	mustPublish(t, c, nl, "a", "put", fakeVal("majority", "y"))
	c.Heal()
	c.WaitConverged(15 * time.Second)
	c.AssertIdentical("a")
	for i, f := range fakes {
		if f.get("a", "minority") != "" || f.get("a", "majority") != "y" {
			t.Errorf("node%d: minority=%q majority=%q", i, f.get("a", "minority"), f.get("a", "majority"))
		}
	}
	c.AssertSingleLeaderPerTerm()
}

// Scenario 3: commands in flight while leaders keep crashing. Every command that was
// acknowledged is applied exactly once everywhere; the others zero or one time (never
// twice, never in another tenant); all nodes end identical.
func TestInFlightCommandsDuringLeaderCrashes(t *testing.T) {
	rounds := chaosSeeds(3)
	for round := 0; round < rounds; round++ {
		round := round
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) {
			tenants := []string{"a", "b", "c"}
			apps, fakes := newFakeApps(3, false, tenants...)
			c := newChaosCluster(t, apps, nil)
			tr := newTracker()
			c.WaitLeader()

			stop := make(chan struct{})
			var wg sync.WaitGroup
			for p := 0; p < 6; p++ {
				wg.Add(1)
				p := p
				go func() {
					defer wg.Done()
					for n := 0; ; n++ {
						select {
						case <-stop:
							return
						default:
						}
						tr.cat(c, tenants[(p+n)%len(tenants)])
						time.Sleep(time.Millisecond)
					}
				}()
			}
			for crash := 0; crash < 4; crash++ {
				time.Sleep(time.Duration(30+50*((round+crash)%3)) * time.Millisecond)
				ld := c.Leaders()
				if len(ld) == 0 {
					continue
				}
				c.Stop(ld[0])
				c.WaitLeader()
				time.Sleep(60 * time.Millisecond)
				c.Restart(ld[0])
			}
			close(stop)
			wg.Wait()
			c.Heal()
			c.WaitConverged(20 * time.Second)
			c.AssertIdentical(tenants...)
			applied := tr.checkExactlyOnce(t, fakes, tenants, c.Up)
			assertSameApplyOrder(t, fakes, tenants)
			tr.mu.Lock()
			t.Logf("published=%d acked=%d ambiguous=%d (ambiguous but applied=%d)", len(tr.tenant), len(tr.acked), len(tr.tenant)-len(tr.acked)-len(tr.notLdr), applied)
			tr.mu.Unlock()
			c.AssertSingleLeaderPerTerm()
		})
	}
}

// Scenario 3, documentation test: what happens when the client retries after an
// AMBIGUOUS error. The leader replicated the entry but never learned that it was
// committed (followers cannot answer), lost leadership, and the entry was committed by
// the next leader. The client got an error, the command IS applied, and a retry applies
// it a second time: Raft gives at-most-once per log entry, not per client intention, and
// orchestrator's commands carry no request id to deduplicate on.
func TestAmbiguousErrorThenRetryAppliesTwice(t *testing.T) {
	apps, fakes := newFakeApps(3, false, "a")
	c := newChaosCluster(t, apps, nil)
	old := c.WaitLeader()
	c.WaitConverged(10 * time.Second)
	// followers receive and append what the leader sends, but its answers are lost: the leader never learns the commit
	c.SetLossy(old, true)
	_, err := c.Publish(old, "a", "cat", fakeVal("log", "req-1"))
	if err == nil {
		t.Skip("the command was acknowledged: the replication race of this scenario did not occur")
	}
	t.Logf("client sees: %v", err)
	c.SetLossy(old, false)
	c.Heal()
	c.WaitConverged(15 * time.Second)
	c.AssertIdentical("a")
	first := occurrences(fakes[0].get("a", "log"))["req-1"]
	t.Logf("after the error the command is applied %d time(s) on every node (ambiguous error)", first)
	// the client retries
	nl := c.WaitLeader()
	mustPublish(t, c, nl, "a", "cat", fakeVal("log", "req-1"))
	c.WaitConverged(15 * time.Second)
	c.AssertIdentical("a")
	total := occurrences(fakes[1].get("a", "log"))["req-1"]
	if total != first+1 {
		t.Errorf("a retry must be applied as one more entry: %d then %d", first, total)
	}
	if first == 1 {
		t.Logf("DOCUMENTED: error + retry => applied %d times; non idempotent ops (see command_applier_idempotency_test) are duplicated", total)
	}
}

// Scenario 4 helpers
func compactingConfig(cfg *raft.Config) {
	cfg.SnapshotThreshold = 8
	cfg.TrailingLogs = 4
	cfg.SnapshotInterval = 20 * time.Millisecond
}

func waitCompacted(t *testing.T, c *chaosCluster, i int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if first, snaps := c.logState(i); snaps > 0 && first > 10 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	first, snaps := c.logState(i)
	t.Fatalf("node%d log was not compacted: firstIndex=%d snapshots=%d", i, first, snaps)
}

func publishMix(t *testing.T, c *chaosCluster, ld int, n int, tenants []string) {
	t.Helper()
	for k := 0; k < n; k++ {
		tn := tenants[k%len(tenants)]
		mustPublish(t, c, ld, tn, "cat", fakeVal("log", fmt.Sprintf("m%d", k)))
		if k%5 == 0 {
			mustPublish(t, c, ld, tn, "put", fakeVal(fmt.Sprintf("k%d", k%3), fmt.Sprintf("v%d", k)))
		}
		if k%7 == 0 {
			mustPublish(t, c, ld, tn, "del", fakeVal("k0", ""))
		}
	}
}

// Scenario 4: a follower that was down while the log was compacted comes back by
// RESTORING a snapshot (not by replaying entries it no longer can get) and converges
// exactly, for every tenant.
func TestFollowerRestoresFromSnapshotAfterCompaction(t *testing.T) {
	tenants := []string{"a", "b", "c"}
	apps, fakes := newFakeApps(3, false, tenants...)
	c := newChaosCluster(t, apps, compactingConfig)
	ld := c.WaitLeader()
	down := (ld + 1) % 3
	c.Stop(down)
	publishMix(t, c, ld, 90, tenants)
	c.WaitConverged(15 * time.Second)
	waitCompacted(t, c, ld)
	before := fakes[down].restoreCount()
	c.Restart(down)
	c.WaitConverged(20 * time.Second)
	c.AssertIdentical(tenants...)
	if fakes[down].restoreCount() <= before {
		t.Errorf("the returning follower did not restore a snapshot (the log was compacted)")
	}
	if n := fakes[down].appliedCount(); n >= 90 {
		t.Errorf("the follower replayed %d entries: it should have restored a snapshot and replayed only the tail", n)
	}
	assertTenantsOnlyHaveTheirOwnCommands(t, fakes, tenants, "m")
}

// assertTenantsOnlyHaveTheirOwnCommands checks ids m<k> only appear in tenant tenants[k%len].
func assertTenantsOnlyHaveTheirOwnCommands(t *testing.T, fakes []*fakeApp, tenants []string, prefix string) {
	t.Helper()
	for ni, f := range fakes {
		for ti, tn := range tenants {
			for id := range occurrences(f.get(tn, "log")) {
				var k int
				if _, err := fmt.Sscanf(id, prefix+"%d", &k); err != nil {
					t.Errorf("node%d tenant %s: unexpected id %q", ni, tn, id)
					continue
				}
				if k%len(tenants) != ti {
					t.Errorf("node%d: command %s of tenant %s found in tenant %s", ni, id, tenants[k%len(tenants)], tn)
				}
			}
		}
	}
}

// Scenario 4: a brand new node joins a cluster whose log is already truncated and gets
// everything from a snapshot.
func TestNewNodeJoinsWithTruncatedLog(t *testing.T) {
	tenants := []string{"a", "b", "c"}
	apps, fakes := newFakeApps(3, false, tenants...)
	c := newChaosCluster(t, apps, compactingConfig)
	ld := c.WaitLeader()
	publishMix(t, c, ld, 80, tenants)
	c.WaitConverged(15 * time.Second)
	waitCompacted(t, c, ld)
	fresh := newFakeApp(false, tenants...)
	idx := c.AddNode(fresh)
	c.WaitConverged(20 * time.Second)
	c.AssertIdentical(tenants...)
	if fresh.restoreCount() == 0 {
		t.Errorf("a node joining a compacted cluster must receive a snapshot")
	}
	// and it follows from now on
	mustPublish(t, c, c.WaitLeader(), "b", "put", fakeVal("post-join", "1"))
	c.WaitConverged(10 * time.Second)
	if fresh.get("b", "post-join") != "1" {
		t.Errorf("node%d does not follow after joining", idx)
	}
	_ = fakes
}

// Scenario 4: snapshots taken (on every node) while commands keep being applied, with
// follower restarts in between, never produce a divergent node: the snapshot is
// point-in-time (regression of the lazy-Persist bug) even for NON idempotent ops.
func TestSnapshotsConcurrentWithWrites(t *testing.T) {
	tenants := []string{"a", "b"}
	apps, fakes := newFakeApps(3, false, tenants...)
	c := newChaosCluster(t, apps, compactingConfig)
	tr := newTracker()
	ld := c.WaitLeader()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			tr.cat(c, tenants[n%2])
		}
	}()
	for round := 0; round < 6; round++ {
		for i := 0; i < 3; i++ {
			if r := c.raftOf(i); r != nil {
				_ = r.Snapshot().Error() // forced, on top of the automatic ones
			}
		}
		victim := (ld + 1 + round) % 3
		c.Stop(victim)
		time.Sleep(80 * time.Millisecond)
		c.Restart(victim)
		time.Sleep(60 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	c.Heal()
	c.WaitConverged(20 * time.Second)
	c.AssertIdentical(tenants...)
	tr.checkExactlyOnce(t, fakes, tenants, c.Up)
	assertSameApplyOrder(t, fakes, tenants)
	c.AssertSingleLeaderPerTerm()
}

// Scenario 5: a node that does not run tenant x receives commands of x.
func TestUnknownTenantOnOneNode(t *testing.T) {
	tenants := []string{"a", "b"}
	apps, fakes := newFakeApps(3, false, tenants...)
	fakes[0].learn("x")
	fakes[1].learn("x")
	// node2 does NOT know x (divergent tenants file)
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	for ld == 2 { // the leader must be a node that knows x for the first part of the test
		_ = c.raftOf(2).LeadershipTransfer().Error()
		ld = c.WaitLeader()
	}
	before := orcraft.UnknownTenantCommands()
	const xCmds = 6
	for k := 0; k < xCmds; k++ {
		mustPublish(t, c, ld, "x", "cat", fakeVal("log", fmt.Sprintf("x%d", k)))
		mustPublish(t, c, ld, "a", "cat", fakeVal("log", fmt.Sprintf("a%d", k)))
		mustPublish(t, c, ld, "b", "cat", fakeVal("log", fmt.Sprintf("b%d", k)))
	}
	c.WaitConverged(10 * time.Second)

	if got := orcraft.UnknownTenantCommands() - before; got != xCmds {
		t.Errorf("UnknownTenantCommands rose by %d, want %d (only node2 lacks x)", got, xCmds)
	}
	c.AssertIdentical("a", "b") // the other tenants are unaffected: the node neither blocks nor diverges
	if fakes[2].get("a", "log") == "" || fakes[2].get("b", "log") == "" {
		t.Error("tenants a and b must keep applying on the node that lacks x")
	}
	if fakes[0].get("x", "log") != fakes[1].get("x", "log") || fakes[0].get("x", "log") == "" {
		t.Error("nodes that know x must agree on it")
	}
	if fakes[2].get("x", "log") != "" {
		t.Error("node2 has no runtime for x: nothing can be applied there")
	}
	// the node keeps following: new commands of the other tenants still reach it
	mustPublish(t, c, ld, "a", "put", fakeVal("after", "1"))
	c.WaitConverged(10 * time.Second)
	if fakes[2].get("a", "after") != "1" {
		t.Error("node2 stopped applying after unknown-tenant commands")
	}

	// When the LEADER is the node that lacks x, the client is told the command failed
	// although it was committed, and applied by the nodes that run x.
	_ = c.raftOf(ld).LeadershipTransferToServer(raft.ServerID("node2"), raft.ServerAddress("node2")).Error()
	l2 := c.WaitLeader()
	if l2 == 2 {
		_, err := c.Publish(2, "x", "cat", fakeVal("log", "via-node2"))
		if !errors.Is(err, orcraft.ErrUnknownTenant) {
			t.Errorf("a leader without tenant x must answer ErrUnknownTenant, got %v", err)
		}
		c.WaitConverged(10 * time.Second)
		if occurrences(fakes[0].get("x", "log"))["via-node2"] != 1 {
			t.Errorf("DOCUMENTED: the command refused by the leader is applied by the nodes that know x")
		} else {
			t.Logf("DOCUMENTED: leader without tenant x answered an error to the client although the command was committed and applied on the other nodes")
		}
	} else {
		t.Logf("leadership did not move to node2 (leader node%d): leader-without-tenant case not exercised", l2)
	}
}

// Scenario 5: the node later gets tenant x. It does NOT recover what it dropped: the
// commands are in the Raft log (and in the other nodes) but were consumed.
func TestTenantAddedLaterDoesNotRecoverDroppedCommands(t *testing.T) {
	for _, withOwnSnapshot := range []bool{false, true} {
		withOwnSnapshot := withOwnSnapshot
		t.Run(fmt.Sprintf("nodeSnapshotsAfterTheLoss=%v", withOwnSnapshot), func(t *testing.T) {
			apps, fakes := newFakeApps(3, false, "a")
			fakes[0].learn("x")
			fakes[1].learn("x")
			c := newChaosCluster(t, apps, nil)
			ld := c.WaitLeader()
			for ld == 2 {
				_ = c.raftOf(2).LeadershipTransfer().Error()
				ld = c.WaitLeader()
			}
			for k := 0; k < 5; k++ {
				mustPublish(t, c, ld, "x", "cat", fakeVal("log", fmt.Sprintf("old%d", k)))
			}
			c.WaitConverged(10 * time.Second)
			if withOwnSnapshot {
				// the node takes its periodic snapshot (every 30 min in production) after losing them
				if err := c.raftOf(2).Snapshot().Error(); err != nil {
					t.Fatal(err)
				}
			}

			// the operator adds x to node2 (at runtime here; a real node needs a restart)
			fakes[2].learn("x")
			mustPublish(t, c, ld, "x", "cat", fakeVal("log", "new"))
			c.WaitConverged(10 * time.Second)
			if got := fakes[2].get("x", "log"); got != "new," {
				t.Fatalf("node2 should only have the command published after it got tenant x, has %q", got)
			}
			if fakes[0].get("x", "log") == fakes[2].get("x", "log") {
				t.Fatal("expected a permanent divergence of x on node2")
			}
			t.Logf("DOCUMENTED: x on node0=%q, on node2=%q: nothing detects or repairs this", fakes[0].get("x", "log"), fakes[2].get("x", "log"))

			// does a restart of node2 heal it? Raft replays the log from its own last snapshot
			c.Stop(2)
			c.Restart(2)
			c.WaitConverged(15 * time.Second)
			healed := fakes[0].get("x", "log") == fakes[2].get("x", "log")
			if withOwnSnapshot && healed {
				t.Errorf("node2 snapshotted after the loss: the dropped entries are behind its snapshot, a replay cannot bring them back")
			}
			if !withOwnSnapshot && !healed {
				t.Errorf("without a snapshot of its own the restart replays the whole log and should heal x")
			}
			t.Logf("after restart: healed=%v (node2 has its own snapshot after the loss: %v)", healed, withOwnSnapshot)
		})
	}
}

// Scenario 6: the node gives up the leadership because the backend of a tenant is
// unhealthy (YieldForTenantFailure): ONE other node leads from then on, for every tenant.
func TestYieldForTenantFailureMovesAllTenants(t *testing.T) {
	tenants := []string{"a", "b", "x"}
	apps, fakes := newFakeApps(3, false, tenants...)
	c := newChaosCluster(t, apps, nil)
	old := c.WaitLeader()
	restore := orcraft.SetGlobalStore(c.storeOf(old))
	defer restore()
	for _, tn := range tenants {
		mustPublish(t, c, old, tn, "put", fakeVal("before", "1"))
	}
	if err := orcraft.YieldForTenantFailure("x", errors.New("backend of x is down")); err != nil {
		t.Fatalf("yield: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var nl int
	for time.Now().Before(deadline) {
		if ls := c.Leaders(); len(ls) == 1 && ls[0] != old {
			nl = ls[0]
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if nl == old {
		t.Fatal("leadership did not move to another node")
	}
	for _, tn := range tenants { // every tenant is served by the same new leader
		mustPublish(t, c, nl, tn, "put", fakeVal("after", "2"))
	}
	c.WaitConverged(10 * time.Second)
	c.AssertIdentical(tenants...)
	for i, f := range fakes {
		for _, tn := range tenants {
			if f.get(tn, "after") != "2" {
				t.Errorf("node%d tenant %s did not get the command of the new leader", i, tn)
			}
		}
	}
	if ls := c.Leaders(); len(ls) != 1 || ls[0] != nl {
		t.Errorf("leaders=%v, want only node%d", ls, nl)
	}
	// the old leader refuses to publish for ANY tenant now
	for _, tn := range tenants {
		if _, err := c.Publish(old, tn, "put", fakeVal("x", "y")); err == nil || !strings.Contains(err.Error(), "not leader") {
			t.Errorf("former leader accepted a command of tenant %s: %v", tn, err)
		}
	}
	c.AssertSingleLeaderPerTerm()
}

// Scenario 6: the yield-hint command (published by the operator) moves the leadership
// away from the hinted-against node, for all tenants at once.
func TestYieldHintMovesLeadership(t *testing.T) {
	tenants := []string{"a", "b"}
	apps, _ := newFakeApps(3, false, tenants...)
	c := newChaosCluster(t, apps, nil)
	old := c.WaitLeader()
	restore := orcraft.SetGlobalStore(c.storeOf(old))
	defer restore()
	defer orcraft.SetThisHostname("node-that-is-not-hinted")()
	if _, err := c.GenericCommand(old, orcraft.YieldHintCommand, []byte("some-other-host")); err != nil {
		t.Logf("yield-hint response: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ls := c.Leaders(); len(ls) == 1 && ls[0] != old {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	ls := c.Leaders()
	if len(ls) != 1 || ls[0] == old {
		t.Fatalf("yield-hint did not move the leadership: leaders=%v old=%d", ls, old)
	}
	for _, tn := range tenants {
		mustPublish(t, c, ls[0], tn, "put", fakeVal("k", "v"))
	}
	c.WaitConverged(10 * time.Second)
	c.AssertIdentical(tenants...)
}
