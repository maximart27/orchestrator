package orcraft

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
)

// Regression test (rt-prop): the snapshot must be a point-in-time image of the FSM at
// the Raft index it is labelled with. fsm.Snapshot() used to return immediately and
// fsmSnapshot.Persist to read the state LATER, while Apply kept running (hashicorp/raft:
// "Persist should write the state as of the time Snapshot was called"). A node that
// restored such a snapshot and replayed the log from the labelled index applied the
// entries between the two points TWICE: harmless for idempotent upserts, a divergence
// for any op that is not idempotent.

type countingApplier struct {
	mu    sync.Mutex
	state []string
}

func (a *countingApplier) ApplyCommand(op string, value []byte) interface{} { return nil }
func (a *countingApplier) ApplyTenantCommand(tenantID, op string, value []byte) interface{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state = append(a.state, string(value)) // NOT idempotent
	return nil
}
func (a *countingApplier) GetData() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return json.Marshal(a.state)
}
func (a *countingApplier) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	a.mu.Lock()
	defer a.mu.Unlock()
	return json.NewDecoder(rc).Decode(&a.state)
}
func (a *countingApplier) joined() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return strings.Join(a.state, ",")
}

type bytesSink struct{ bytes.Buffer }

func (*bytesSink) Close() error  { return nil }
func (*bytesSink) ID() string    { return "test" }
func (*bytesSink) Cancel() error { return nil }

func TestSnapshotIsPointInTime(t *testing.T) {
	leaderApp, followerApp := &countingApplier{}, &countingApplier{}
	leader := (*fsm)(NewStore("", "", "", leaderApp, leaderApp))
	follower := (*fsm)(NewStore("", "", "", followerApp, followerApp))
	apply := func(f *fsm, idx uint64) {
		data, _ := json.Marshal(&storeCommand{Op: "cat", Value: []byte{byte('0' + idx)}, Tenant: "t"})
		f.Apply(&raft.Log{Index: idx, Data: data})
	}
	for i := uint64(1); i <= 3; i++ {
		apply(leader, i)
	}
	snap, err := leader.Snapshot() // Raft labels this snapshot with index 3
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(4); i <= 6; i++ { // Apply runs concurrently with Persist: entries 4..6 land first
		apply(leader, i)
	}
	sink := &bytesSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	// a follower restores "index 3" and replays the log from index 4
	if err := follower.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	for i := uint64(4); i <= 6; i++ {
		apply(follower, i)
	}
	if leaderApp.joined() != followerApp.joined() {
		t.Fatalf("follower diverged after snapshot restore + replay:\n leader:   %s\n follower: %s", leaderApp.joined(), followerApp.joined())
	}
}
