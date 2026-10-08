package orcraft_test

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// chaosSeeds is how many seeds the randomized tests run: CHAOS_SEEDS overrides it.
func chaosSeeds(def int) int {
	if v, err := strconv.Atoi(os.Getenv("CHAOS_SEEDS")); err == nil && v > 0 {
		return v
	}
	if testing.Short() {
		return 2
	}
	return def
}

// cmdTracker records the outcome of every command a test publishes.
type cmdTracker struct {
	mu     sync.Mutex
	next   int
	tenant map[string]string // id -> tenant
	acked  map[string]bool
	notLdr map[string]bool // refused before reaching Raft: can never be applied
	errs   map[string]error
}

func newTracker() *cmdTracker {
	return &cmdTracker{tenant: map[string]string{}, acked: map[string]bool{}, notLdr: map[string]bool{}, errs: map[string]error{}}
}

// cat publishes "cat log <id>" for tenant through whatever node leads.
func (tr *cmdTracker) cat(c *chaosCluster, tenant string) string {
	tr.mu.Lock()
	tr.next++
	id := fmt.Sprintf("c%d", tr.next)
	tr.tenant[id] = tenant
	tr.mu.Unlock()
	_, err := c.PublishAny(tenant, "cat", fakeVal("log", id))
	tr.record(id, err)
	return id
}

func (tr *cmdTracker) record(id string, err error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	switch {
	case err == nil:
		tr.acked[id] = true
	case strings.Contains(err.Error(), "not leader"):
		tr.notLdr[id] = true
		tr.errs[id] = err
	default:
		tr.errs[id] = err
	}
}

func occurrences(logValue string) map[string]int {
	out := map[string]int{}
	for _, id := range strings.Split(logValue, ",") {
		if id != "" {
			out[id]++
		}
	}
	return out
}

// checkExactlyOnce verifies, on every up node, what Raft promises for the commands the tracker saw:
//   - acknowledged: applied exactly once, in their own tenant only
//   - refused as "not leader": applied zero times
//   - failed with another error (ambiguous): applied zero or one time (never twice)
//   - nothing else is ever applied.
//
// It returns how many ambiguous commands were applied after all (counted on node 0).
func (tr *cmdTracker) checkExactlyOnce(t *testing.T, fakes []*fakeApp, tenants []string, up func(int) bool) (ambiguousApplied int) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for ni, f := range fakes {
		if !up(ni) {
			continue
		}
		seen := map[string]string{} // id -> tenant where it was found
		for _, tn := range tenants {
			for id, n := range occurrences(f.get(tn, "log")) {
				if n != 1 {
					t.Errorf("node%d tenant %s: command %s applied %d times", ni, tn, id, n)
				}
				if prev, dup := seen[id]; dup {
					t.Errorf("node%d: command %s applied in tenants %s and %s", ni, id, prev, tn)
				}
				seen[id] = tn
				if want, known := tr.tenant[id]; !known {
					t.Errorf("node%d tenant %s: applied a command nobody published: %s", ni, tn, id)
				} else if want != tn {
					t.Errorf("node%d: command %s of tenant %s applied in tenant %s", ni, id, want, tn)
				}
			}
		}
		for id := range tr.tenant {
			_, applied := seen[id]
			switch {
			case tr.acked[id] && !applied:
				t.Errorf("node%d: command %s was ACKNOWLEDGED but is not applied (lost)", ni, id)
			case tr.notLdr[id] && applied:
				t.Errorf("node%d: command %s was refused (not leader) but is applied", ni, id)
			case !tr.acked[id] && !tr.notLdr[id] && applied && ni == 0:
				ambiguousApplied++
			}
		}
	}
	return ambiguousApplied
}

type chaosOutcome struct {
	published, acked, notLeader, ambiguous, ambiguousApplied int
}

func (o *chaosOutcome) add(x chaosOutcome) {
	o.published += x.published
	o.acked += x.acked
	o.notLeader += x.notLeader
	o.ambiguous += x.ambiguous
	o.ambiguousApplied += x.ambiguousApplied
}

// assertSameApplyOrder checks that every node applied the commands of each tenant in the same order.
func assertSameApplyOrder(t *testing.T, fakes []*fakeApp, tenants []string) {
	t.Helper()
	for _, tn := range tenants {
		ref := strings.Join(fakes[0].seqOf(tn), "|")
		for i := 1; i < len(fakes); i++ {
			if got := strings.Join(fakes[i].seqOf(tn), "|"); got != ref {
				t.Errorf("tenant %s: apply order differs between node0 and node%d:\n%s\n%s", tn, i, ref, got)
			}
		}
	}
}

// runChaos drives one randomized schedule for a seed and checks the invariants.
func runChaos(t *testing.T, seed int64, nodes int, steps int, tune func(*raft.Config)) chaosOutcome {
	t.Helper()
	tenants := []string{"a", "b", "c"}
	rng := rngFor(t, seed)
	apps, fakes := newFakeApps(nodes, false, tenants...)
	c := newChaosCluster(t, apps, tune)
	tr := newTracker()
	c.WaitLeader()
	maxDown := (nodes - 1) / 2
	down := map[int]bool{}

	// background publishers: commands are in flight while the topology changes
	stopPub := make(chan struct{})
	var pubWG sync.WaitGroup
	for p := 0; p < 3; p++ {
		pubWG.Add(1)
		prng := rand.New(rand.NewSource(seed*10 + int64(p)))
		go func() {
			defer pubWG.Done()
			for {
				select {
				case <-stopPub:
					return
				default:
				}
				tr.cat(c, tenants[prng.Intn(len(tenants))])
				if prng.Intn(4) == 0 { // order-sensitive non-cat commands too
					op := []string{"put", "del"}[prng.Intn(2)]
					_, _ = c.PublishAny(tenants[prng.Intn(len(tenants))], op, fakeVal(fmt.Sprintf("k%d", prng.Intn(3)), fmt.Sprintf("v%d", prng.Intn(100))))
				}
				time.Sleep(time.Duration(prng.Intn(4)) * time.Millisecond)
			}
		}()
	}

	for step := 0; step < steps; step++ {
		time.Sleep(time.Duration(10+rng.Intn(50)) * time.Millisecond)
		switch r := rng.Intn(100); {
		case r < 15:
			time.Sleep(time.Duration(rng.Intn(100)) * time.Millisecond)
		case r < 29: // random 2-way split
			var g1, g2 []int
			for i := 0; i < nodes; i++ {
				if rng.Intn(2) == 0 {
					g1 = append(g1, i)
				} else {
					g2 = append(g2, i)
				}
			}
			c.Partition(g1, g2)
		case r < 45:
			if rng.Intn(2) == 0 {
				c.Isolate(rng.Intn(nodes))
			} else {
				c.SetLossy(rng.Intn(nodes), true) // answers to this node's AppendEntries get lost
			}
		case r < 65:
			c.Heal()
		case r < 75:
			if i := rng.Intn(nodes); !down[i] && len(down) < maxDown {
				c.Stop(i)
				down[i] = true
			}
		case r < 90:
			for i := range down {
				c.Restart(i)
				delete(down, i)
				break
			}
		case r < 100:
			if ld := c.Leaders(); len(ld) > 0 {
				if lr := c.raftOf(ld[0]); lr != nil {
					_ = lr.LeadershipTransfer().Error()
				}
			}
		}
	}
	c.Heal()
	close(stopPub)
	pubWG.Wait()
	c.Heal()
	for i := range down {
		c.Restart(i)
	}
	c.Heal()
	c.WaitConverged(30 * time.Second)
	c.AssertIdentical(tenants...)
	c.AssertSingleLeaderPerTerm()
	out := chaosOutcome{}
	out.ambiguousApplied = tr.checkExactlyOnce(t, fakes, tenants, c.Up)
	assertSameApplyOrder(t, fakes, tenants)
	tr.mu.Lock()
	out.published = len(tr.tenant)
	out.acked = len(tr.acked)
	out.notLeader = len(tr.notLdr)
	out.ambiguous = out.published - out.acked - out.notLeader
	tr.mu.Unlock()
	return out
}

// Scenario 1 (deterministic applier): random publish/partition/crash/leader-change
// schedules per seed; at the end every node holds the identical per-tenant state, in
// the same apply order, and every acknowledged command is applied exactly once.
func TestChaosConvergenceFake(t *testing.T) {
	seeds := chaosSeeds(8)
	var total chaosOutcome
	start := time.Now()
	for s := 0; s < seeds; s++ {
		seed := int64(1000 + s)
		nodes := 3 + 2*(s%2) // alternate 3 and 5 nodes
		t.Run(fmt.Sprintf("seed%d_n%d", seed, nodes), func(t *testing.T) {
			total.add(runChaos(t, seed, nodes, 60, nil))
		})
	}
	t.Logf("%d seeds in %v: published=%d acked=%d refused(not leader)=%d ambiguous=%d (applied anyway=%d)",
		seeds, time.Since(start).Round(time.Second), total.published, total.acked, total.notLeader, total.ambiguous, total.ambiguousApplied)
}

// aggressiveSnapshots makes Raft compact its log all the time, so that restarted and
// partitioned nodes come back through a snapshot restore instead of a log replay.
func aggressiveSnapshots(cfg *raft.Config) {
	cfg.SnapshotThreshold = 16
	cfg.TrailingLogs = 8
	cfg.SnapshotInterval = 30 * time.Millisecond
}

// Scenarios 1+4: the same random schedules, with the log compacted every few entries.
func TestChaosConvergenceFakeWithSnapshots(t *testing.T) {
	seeds := chaosSeeds(8)
	var total chaosOutcome
	for s := 0; s < seeds; s++ {
		seed := int64(2000 + s)
		nodes := 3 + 2*(s%2)
		t.Run(fmt.Sprintf("seed%d_n%d", seed, nodes), func(t *testing.T) {
			total.add(runChaos(t, seed, nodes, 60, aggressiveSnapshots))
		})
	}
	t.Logf("%d seeds: published=%d acked=%d ambiguous=%d (applied anyway=%d)", seeds, total.published, total.acked, total.ambiguous, total.ambiguousApplied)
}
