package orcraft_test

import (
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	orcraft "github.com/proxysql/orchestrator/go/raft"
)

func persistSnapshot(t *testing.T, store *orcraft.FileSnapshotStore, index, term uint64, state []byte) {
	t.Helper()
	_, trans := raft.NewInmemTransport("node0")
	conf := raft.Configuration{Servers: []raft.Server{{ID: "node0", Address: "node0"}}}
	sink, err := store.Create(raft.SnapshotVersionMax, index, term, conf, 1, trans)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write(state); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

func snapshotIndexes(t *testing.T, store *orcraft.FileSnapshotStore) []uint64 {
	t.Helper()
	metas, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	var indexes []uint64
	for _, m := range metas {
		indexes = append(indexes, m.Index)
	}
	return indexes
}

// Regression (rt-prop): FileSnapshotStore.ReapSnapshots used to delete every snapshot NEWER
// (term, index) than the one that had just been closed. The local snapshot (runSnapshots
// goroutine) and the InstallSnapshot of the leader run concurrently, so an older snapshot
// can close after a newer one; deleting the newer one left the node with a snapshot older
// than the first entry of its truncated log.
func TestReapNeverDeletesANewerSnapshot(t *testing.T) {
	store, err := orcraft.NewFileSnapshotStore(t.TempDir(), 3, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	persistSnapshot(t, store, 200, 3, []byte("state")) // installed from the leader
	persistSnapshot(t, store, 150, 3, []byte("state")) // the local snapshot that was taking longer
	if got := snapshotIndexes(t, store); len(got) != 2 || got[0] != 200 || got[1] != 150 {
		t.Fatalf("the newest snapshot (index 200) must survive the close of an older one; snapshots now: %v", got)
	}
	// (term 2, index 200) is the oldest of the four: it is the one reaped, the (term 4, index 10) one sorts first
	persistSnapshot(t, store, 200, 2, []byte("state"))
	persistSnapshot(t, store, 10, 4, []byte("state"))
	if got := snapshotIndexes(t, store); len(got) != 3 || got[0] != 10 || got[1] != 200 || got[2] != 150 {
		t.Fatalf("snapshots are ordered by (term, index) and the retain count is honoured; got %v", got)
	}
}

// Retention keeps the `retain` newest snapshots whatever the order in which they close.
func TestReapRetentionIsIndependentOfCloseOrder(t *testing.T) {
	store, err := orcraft.NewFileSnapshotStore(t.TempDir(), 2, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	for _, idx := range []uint64{30, 10, 50, 20, 40} {
		persistSnapshot(t, store, idx, 1, []byte("state"))
		time.Sleep(2 * time.Millisecond) // snapshot names carry a millisecond stamp
	}
	if got := snapshotIndexes(t, store); len(got) != 2 || got[0] != 50 || got[1] != 40 {
		t.Fatalf("want the 2 newest snapshots [50 40], got %v", got)
	}
}

// A node whose Raft log was truncated by an InstallSnapshot (index 200) while a slower local
// snapshot (index 150) closed afterwards must restart: before the fix the snapshot 200 was
// gone, NewRaft looked for log entry 151 and panicked with "log not found".
func TestNodeRestartsAfterOutOfOrderSnapshotClose(t *testing.T) {
	app := newFakeApp(false, "a")
	app.ApplyTenantCommand("a", "put", fakeVal("k", "v"))
	state, err := app.GetData()
	if err != nil {
		t.Fatal(err)
	}

	snaps, err := orcraft.NewFileSnapshotStore(t.TempDir(), 3, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	persistSnapshot(t, snaps, 200, 3, state) // InstallSnapshot from the leader ...
	persistSnapshot(t, snaps, 150, 3, state) // ... and the local one, closed afterwards

	// the log was compacted up to the installed snapshot: it starts at 201
	logs := raft.NewInmemStore()
	for i := uint64(201); i <= 205; i++ {
		if err := logs.StoreLog(&raft.Log{Index: i, Term: 3, Type: raft.LogNoop}); err != nil {
			t.Fatal(err)
		}
	}
	if err := logs.SetUint64([]byte("CurrentTerm"), 3); err != nil {
		t.Fatal(err)
	}

	_, trans := raft.NewInmemTransport("node0")
	cfg := raftConfig("node0", nil)
	_, fsm := orcraft.NewStoreForTest("node0", app, app)
	r, err := raft.NewRaft(cfg, fsm, logs, logs, snaps, trans)
	if err != nil {
		t.Fatalf("restart over the snapshots left by an out-of-order close: %v", err)
	}
	defer func() { _ = r.Shutdown().Error() }()
	if got := snapshotIndexes(t, snaps); len(got) == 0 || got[0] != 200 {
		t.Fatalf("snapshots after restart: %v", got)
	}
}
