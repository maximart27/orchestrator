package orcraft

import (
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// Test-only exports for the external orcraft_test package: an in-memory cluster
// is built around the real FSM (fsm.go) and the real Store.tenantCommand.

// NewStoreForTest returns a Store without a Raft yet (see SetRaft) and the FSM
// that must be handed to raft.NewRaft.
func NewStoreForTest(addr string, applier CommandApplier, snapshotCreatorApplier SnapshotCreatorApplier) (*Store, raft.FSM) {
	s := NewStore("", addr, addr, applier, snapshotCreatorApplier)
	return s, (*fsm)(s)
}

// SetRaft attaches the raft instance to the store.
func (store *Store) SetRaft(r *raft.Raft) { store.raft = r }

// Raft returns the raft instance of the store.
func (store *Store) Raft() *raft.Raft { return store.raft }

// TenantCommand is the unexported tenantCommand: what PublishTenantCommand does after json encoding.
func (store *Store) TenantCommand(tenantID, op string, value []byte) (interface{}, error) {
	return store.tenantCommand(tenantID, op, value)
}

// SetGlobalStore makes s the process-wide store (what Yield, GetState... use) and
// returns a function that restores the previous one.
func SetGlobalStore(s *Store) (restore func()) {
	prev, prevComplete := store, atomic.LoadInt64(&raftSetupComplete)
	store = s
	atomic.StoreInt64(&raftSetupComplete, 1)
	return func() {
		store = prev
		atomic.StoreInt64(&raftSetupComplete, prevComplete)
	}
}

// SetThisHostname sets the hostname the yield-hint command is matched against.
func SetThisHostname(h string) (restore func()) {
	prev := ThisHostname
	ThisHostname = h
	return func() { ThisHostname = prev }
}

// ApplyRaw feeds raw bytes to the FSM as a log entry (decoder robustness).
func ApplyRaw(applier CommandApplier, data []byte) interface{} {
	f := &fsm{applier: applier}
	return f.Apply(&raft.Log{Index: 1, Data: data})
}
