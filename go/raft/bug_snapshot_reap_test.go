//go:build rtbugs

package orcraft

import (
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

// BUG (rt-prop): FileSnapshotStore.ReapSnapshots (file_snapshot.go, the orchestrator-specific
// "deprecated snapshots" branch) deletes every snapshot that is NEWER (term, index) than the
// one that has just been closed. A node can close an older snapshot after a newer one: the
// automatic/forced local snapshot (runSnapshots goroutine) and the InstallSnapshot sent by the
// leader (main goroutine) run concurrently, and a follower that is far behind receives
// the leader's snapshot (index 200) while its own (index 150) is still being persisted. When
// the older one closes it deletes the newer one, but Raft has already truncated the log up to
// the newer one: the next start of that node dies in raft.NewRaft ("log not found" panic, api.go
// log scan) until its Raft data directory is wiped. Seen in the chaos run
// CHAOS_SEEDS=30 TestChaosConvergenceFakeWithSnapshots/seed2025_n5.
//
// Run: go test -tags rtbugs -run TestBugReapDeletesNewerSnapshot ./go/raft/
func TestBugReapDeletesNewerSnapshot(t *testing.T) {
	store, err := NewFileSnapshotStore(t.TempDir(), 3, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, trans := raft.NewInmemTransport("node0")
	conf := raft.Configuration{Servers: []raft.Server{{ID: "node0", Address: "node0"}}}
	persist := func(index uint64) {
		sink, err := store.Create(raft.SnapshotVersionMax, index, 3, conf, 1, trans)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sink.Write([]byte("state")); err != nil {
			t.Fatal(err)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	}
	persist(200) // installed from the leader
	persist(150) // the local snapshot that was taking longer
	metas, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	var indexes []uint64
	for _, m := range metas {
		indexes = append(indexes, m.Index)
	}
	if len(indexes) == 0 || indexes[0] != 200 {
		t.Fatalf("the newest snapshot (index 200) must survive the close of an older one; snapshots now: %v", indexes)
	}
}
