package orcraft_test

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// nodeApp is what a node of the test cluster runs as its state machine: the
// tenant-aware applier and the snapshotter, plus what the harness needs to
// compare and restart nodes.
type nodeApp interface {
	orcraft.TenantCommandApplier
	orcraft.CommandApplier
	orcraft.SnapshotCreatorApplier
	// Digest is a canonical rendering of the state of one tenant on this node
	// (excluding what is legitimately node-specific, e.g. apply-time stamps).
	Digest(tenantID string) string
	// Restarted is called when the raft instance of the node is restarted. An
	// in-memory state machine forgets everything (Raft rebuilds it); a persistent
	// one (SQLite) keeps its data.
	Restarted()
}

type logStore interface {
	raft.LogStore
	raft.StableStore
}

type chaosNode struct {
	idx   int
	addr  raft.ServerAddress
	logs  logStore
	snaps raft.SnapshotStore
	trans *raft.InmemTransport
	app   nodeApp
	store *orcraft.Store
	up    bool
}

type chaosCluster struct {
	t       *testing.T
	nodes   []*chaosNode
	link    [][]bool // link[i][j]: i can reach j (directional)
	mu      sync.Mutex
	tune    func(*raft.Config)
	seed    int64
	dir     string
	relLogs bool

	// leader samples (term -> set of node idx that were seen leading in that term)
	leadersMu    sync.Mutex
	leaderByTerm map[uint64]map[int]bool
	stopSampler  chan struct{}
	samplerDone  chan struct{}
}

func raftConfig(addr raft.ServerAddress, tune func(*raft.Config)) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(addr)
	cfg.HeartbeatTimeout = 100 * time.Millisecond
	cfg.ElectionTimeout = 100 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.LogOutput = io.Discard
	cfg.NoLegacyTelemetry = true
	if tune != nil {
		tune(cfg)
	}
	return cfg
}

// clusterOption tunes newChaosCluster.
type clusterOption func(*chaosCluster)

// withRelLogs keeps the Raft log and stable store in the production SQLite store
// (orcraft.NewRelationalStore) instead of in memory. Snapshots always use the
// production file snapshot store (orcraft.NewFileSnapshotStore) in a temp dir.
func withRelLogs(c *chaosCluster) { c.relLogs = true }

// newStores creates the log and snapshot stores of a node.
func (c *chaosCluster) newStores(idx int) (logStore, raft.SnapshotStore) {
	dir := filepath.Join(c.dir, fmt.Sprintf("node%d", idx))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		c.t.Fatal(err)
	}
	snaps, err := orcraft.NewFileSnapshotStore(dir, 3, io.Discard)
	if err != nil {
		c.t.Fatal(err)
	}
	if c.relLogs {
		return orcraft.NewRelationalStore(dir), snaps
	}
	return raft.NewInmemStore(), snaps
}

// newChaosCluster starts n nodes; apps[i] is the state machine of node i.
func newChaosCluster(t *testing.T, apps []nodeApp, tune func(*raft.Config), opts ...clusterOption) *chaosCluster {
	t.Helper()
	n := len(apps)
	c := &chaosCluster{t: t, tune: tune, leaderByTerm: map[uint64]map[int]bool{}, dir: t.TempDir()}
	for _, o := range opts {
		o(c)
	}
	var servers []raft.Server
	for i := 0; i < n; i++ {
		addr := raft.ServerAddress(fmt.Sprintf("node%d", i))
		servers = append(servers, raft.Server{ID: raft.ServerID(addr), Address: addr})
		logs, snaps := c.newStores(i)
		c.nodes = append(c.nodes, &chaosNode{idx: i, addr: addr, app: apps[i], logs: logs, snaps: snaps})
		c.link = append(c.link, make([]bool, n))
	}
	for _, nd := range c.nodes {
		_, tr := raft.NewInmemTransportWithTimeout(nd.addr, 250*time.Millisecond)
		nd.trans = tr
		cfg := raftConfig(nd.addr, tune)
		if err := raft.BootstrapCluster(cfg, nd.logs, nd.logs, nd.snaps, noPipeline{tr}, raft.Configuration{Servers: servers}); err != nil {
			t.Fatal(err)
		}
	}
	for _, nd := range c.nodes {
		c.startRaft(nd)
	}
	c.Heal()
	c.stopSampler, c.samplerDone = make(chan struct{}), make(chan struct{})
	go c.sample()
	t.Cleanup(c.Close)
	return c
}

func (c *chaosCluster) startRaft(nd *chaosNode) {
	c.t.Helper()
	cfg := raftConfig(nd.addr, c.tune)
	s, f := orcraft.NewStoreForTest(string(nd.addr), nd.app, nd.app)
	r, err := raft.NewRaft(cfg, f, nd.logs, nd.logs, nd.snaps, noPipeline{nd.trans})
	if err != nil {
		c.t.Fatalf("starting %s: %v", nd.addr, err)
	}
	s.SetRaft(r)
	nd.store = s
	nd.up = true
}

// sample records which nodes lead in which term: Raft allows at most one leader per term.
func (c *chaosCluster) sample() {
	defer close(c.samplerDone)
	for {
		select {
		case <-c.stopSampler:
			return
		case <-time.After(2 * time.Millisecond):
		}
		c.mu.Lock()
		for _, nd := range c.nodes {
			if !nd.up {
				continue
			}
			r := nd.store.Raft()
			term := r.CurrentTerm()
			if r.State() == raft.Leader && r.CurrentTerm() == term {
				c.leadersMu.Lock()
				if c.leaderByTerm[term] == nil {
					c.leaderByTerm[term] = map[int]bool{}
				}
				c.leaderByTerm[term][nd.idx] = true
				c.leadersMu.Unlock()
			}
		}
		c.mu.Unlock()
	}
}

// AssertSingleLeaderPerTerm fails if two different nodes were seen leading the same term.
func (c *chaosCluster) AssertSingleLeaderPerTerm() {
	c.t.Helper()
	c.leadersMu.Lock()
	defer c.leadersMu.Unlock()
	for term, set := range c.leaderByTerm {
		if len(set) > 1 {
			c.t.Errorf("SPLIT BRAIN: term %d had leaders %v", term, set)
		}
	}
}

func (c *chaosCluster) Close() {
	if c.stopSampler != nil {
		select {
		case <-c.stopSampler:
		default:
			close(c.stopSampler)
		}
		<-c.samplerDone
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, nd := range c.nodes {
		if nd.up {
			_ = nd.store.Raft().Shutdown().Error()
			nd.up = false
		}
	}
}

// applyLinks pushes the link matrix to the transports.
func (c *chaosCluster) applyLinksLocked() {
	for i, a := range c.nodes {
		for j, b := range c.nodes {
			if i == j {
				continue
			}
			if c.link[i][j] && a.up && b.up {
				a.trans.Connect(b.addr, b.trans)
			} else {
				a.trans.Disconnect(b.addr)
			}
		}
	}
}

// Heal restores every link between up nodes.
func (c *chaosCluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.link {
		for j := range c.link[i] {
			c.link[i][j] = i != j
		}
	}
	c.applyLinksLocked()
}

// Partition splits the nodes in groups; nodes of different groups cannot talk, in either direction.
func (c *chaosCluster) Partition(groups ...[]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	group := map[int]int{}
	for g, members := range groups {
		for _, m := range members {
			group[m] = g
		}
	}
	for i := range c.link {
		for j := range c.link[i] {
			gi, oki := group[i]
			gj, okj := group[j]
			c.link[i][j] = i != j && oki && okj && gi == gj
		}
	}
	c.applyLinksLocked()
}

// Isolate cuts every link of node i.
func (c *chaosCluster) Isolate(i int) {
	var rest []int
	for j := range c.nodes {
		if j != i {
			rest = append(rest, j)
		}
	}
	c.Partition([]int{i}, rest)
}

// Stop shuts node i down (its logs, snapshots and persistent app state stay).
func (c *chaosCluster) Stop(i int) {
	c.mu.Lock()
	nd := c.nodes[i]
	if !nd.up {
		c.mu.Unlock()
		return
	}
	nd.up = false
	r := nd.store.Raft()
	c.applyLinksLocked()
	c.mu.Unlock()
	if err := r.Shutdown().Error(); err != nil {
		c.t.Errorf("shutdown node%d: %v", i, err)
	}
}

// Restart boots node i again over the same logs, snapshots and app.
func (c *chaosCluster) Restart(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	nd := c.nodes[i]
	if nd.up {
		return
	}
	nd.app.Restarted()
	_, nd.trans = raft.NewInmemTransportWithTimeout(nd.addr, 250*time.Millisecond)
	c.startRaft(nd)
	c.applyLinksLocked()
}

// Up tells whether node i is running.
func (c *chaosCluster) Up(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i].up
}

func (c *chaosCluster) raftOf(i int) *raft.Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.nodes[i].up {
		return nil
	}
	return c.nodes[i].store.Raft()
}

func (c *chaosCluster) storeOf(i int) *orcraft.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.nodes[i].up {
		return nil
	}
	return c.nodes[i].store
}

// Leaders returns the up nodes that currently believe they lead.
func (c *chaosCluster) Leaders() []int {
	var out []int
	for i := range c.nodes {
		if r := c.raftOf(i); r != nil && r.State() == raft.Leader {
			out = append(out, i)
		}
	}
	return out
}

// WaitLeader waits for a leader among the given nodes (all if none given) and returns its index.
func (c *chaosCluster) WaitLeader(among ...int) int {
	c.t.Helper()
	if len(among) == 0 {
		for i := range c.nodes {
			among = append(among, i)
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, i := range among {
			r := c.raftOf(i)
			if r == nil || r.State() != raft.Leader {
				continue
			}
			// a leader that can reach a quorum of the nodes it is allowed to talk to
			if r.VerifyLeader().Error() == nil {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader among %v", among)
	return -1
}

// Publish sends a tenant command through node i (which must be the leader).
func (c *chaosCluster) Publish(i int, tenant, op string, value []byte) (interface{}, error) {
	s := c.storeOf(i)
	if s == nil {
		return nil, fmt.Errorf("node%d is down", i)
	}
	type result struct {
		resp interface{}
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := s.TenantCommand(tenant, op, value)
		done <- result{resp, err}
	}()
	select {
	case r := <-done:
		return r.resp, r.err
	case <-time.After(30 * time.Second):
		c.t.Errorf("Publish(%s, %s) on node%d did not return in 30s: the Raft apply future never completed", tenant, op, i)
		return nil, fmt.Errorf("publish hung")
	}
}

// PublishAny tries the nodes in turn (as the HTTP layer would after a redirect to the
// leader); if nobody leads it waits a little for an election to settle.
func (c *chaosCluster) PublishAny(tenant, op string, value []byte) (interface{}, error) {
	var lastErr error
	deadline := time.Now().Add(300 * time.Millisecond)
	for {
		for _, i := range rand.Perm(len(c.nodes)) {
			if r := c.raftOf(i); r == nil || r.State() != raft.Leader {
				continue
			}
			resp, err := c.Publish(i, tenant, op, value)
			if err == nil || !strings.Contains(err.Error(), "not leader") {
				return resp, err
			}
			lastErr = err
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("not leader")
	}
	return nil, lastErr
}

// WaitConverged waits until every up node has applied everything the leader has
// committed. It returns when stable, and fails after the deadline.
func (c *chaosCluster) WaitConverged(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ld := c.WaitLeader()
		lr := c.raftOf(ld)
		if lr != nil && lr.Barrier(5*time.Second).Error() == nil {
			last := lr.LastIndex()
			for time.Now().Before(deadline) {
				ok := true
				for i := range c.nodes {
					if r := c.raftOf(i); r != nil && r.AppliedIndex() < last {
						ok = false
					}
				}
				if ok {
					return
				}
				if lr.State() != raft.Leader {
					break // leadership moved: barrier again
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		if time.Now().After(deadline) {
			var st []string
			for i := range c.nodes {
				if r := c.raftOf(i); r != nil {
					st = append(st, fmt.Sprintf("node%d applied=%d last=%d state=%s", i, r.AppliedIndex(), r.LastIndex(), r.State()))
				}
			}
			c.t.Fatalf("cluster did not converge: %s", strings.Join(st, "; "))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// identicalDiff returns "" if the digest of every tenant is the same on every up node.
func (c *chaosCluster) identicalDiff(tenants []string) string {
	for _, tn := range tenants {
		refNode := -1
		var ref string
		for i, nd := range c.nodes {
			if !c.Up(i) {
				continue
			}
			d := nd.app.Digest(tn)
			if refNode < 0 {
				ref, refNode = d, i
				continue
			}
			if d != ref {
				return fmt.Sprintf("tenant %s differs between node%d and node%d:\n--- node%d\n%s\n--- node%d\n%s", tn, refNode, i, refNode, ref, i, d)
			}
		}
	}
	return ""
}

// AssertIdentical fails if the digest of any tenant differs between up nodes. Raft's
// AppliedIndex counts what was handed to the FSM goroutine, not what it has applied
// yet, so it retries for a few seconds before declaring a difference.
func (c *chaosCluster) AssertIdentical(tenants ...string) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		diff := c.identicalDiff(tenants)
		if diff == "" {
			return
		}
		if time.Now().After(deadline) {
			c.t.Errorf("%s", diff)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// rng returns a deterministic generator for the seed, and logs the seed.
func rngFor(t *testing.T, seed int64) *rand.Rand {
	t.Helper()
	t.Logf("seed=%d", seed)
	return rand.New(rand.NewSource(seed))
}

func sortedKeys(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// AddNode joins a brand new node (empty logs, no bootstrap) to the cluster as a
// voter and returns its index.
func (c *chaosCluster) AddNode(app nodeApp) int {
	c.t.Helper()
	c.mu.Lock()
	idx := len(c.nodes)
	addr := raft.ServerAddress(fmt.Sprintf("node%d", idx))
	logs, snaps := c.newStores(idx)
	nd := &chaosNode{idx: idx, addr: addr, app: app, logs: logs, snaps: snaps}
	_, nd.trans = raft.NewInmemTransportWithTimeout(addr, 250*time.Millisecond)
	c.nodes = append(c.nodes, nd)
	for i := range c.link {
		c.link[i] = append(c.link[i], true)
	}
	row := make([]bool, idx+1)
	for j := 0; j < idx; j++ {
		row[j] = true
	}
	c.link = append(c.link, row)
	c.startRaft(nd)
	c.applyLinksLocked()
	c.mu.Unlock()
	ld := c.WaitLeader()
	if err := c.raftOf(ld).AddVoter(raft.ServerID(addr), addr, 0, 10*time.Second).Error(); err != nil {
		c.t.Fatalf("AddVoter: %v", err)
	}
	return idx
}

// SnapshotCount returns how many snapshots node i holds, and the index of the first log entry it still has.
func (c *chaosCluster) logState(i int) (firstLog uint64, snapshots int) {
	nd := c.nodes[i]
	firstLog, _ = nd.logs.FirstIndex()
	metas, _ := nd.snaps.List()
	return firstLog, len(metas)
}

// noPipeline hides the pipelined AppendEntries of the in-memory transport: a pipeline
// towards a node that was shut down and restarted (a new transport instance) can wedge
// forever, which a real network connection never does (it errors out). Plain
// AppendEntries RPCs, with a timeout, fail and are retried instead.
type noPipeline struct{ *raft.InmemTransport }

func (noPipeline) AppendEntriesPipeline(raft.ServerID, raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, raft.ErrPipelineReplicationNotSupported
}
