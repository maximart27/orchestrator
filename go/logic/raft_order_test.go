package logic

import (
	"errors"
	"testing"

	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// Raft replays its log into the tenants the moment it opens: every tenant must
// already exist then, or its commands are discarded for good.
func TestRaftIsSetUpAfterEveryTenantIsBuilt(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b", "c"}, nil)
	m.base.RaftEnabled = true

	prevSetup, prevMonitor := raftSetup, raftMonitor
	defer func() { raftSetup, raftMonitor = prevSetup, prevMonitor }()
	monitored := make(chan struct{})
	raftMonitor = func() { close(monitored) }
	setupCalls := 0
	raftSetup = func(applier orcraft.CommandApplier, _ orcraft.SnapshotCreatorApplier, _ string) error {
		setupCalls++
		tenantApplier, ok := applier.(orcraft.TenantCommandApplier)
		if !ok {
			t.Fatalf("the applier handed to Raft must be tenant-aware, got %T", applier)
		}
		for _, id := range []string{"a", "b", "c"} {
			res := tenantApplier.ApplyTenantCommand(id, "enable-global-recoveries", nil)
			if err, isErr := res.(error); isErr && errors.Is(err, orcraft.ErrUnknownTenant) {
				t.Errorf("when Raft opens, tenant %s must already exist: %v", id, err)
			}
		}
		return nil
	}
	if err := m.bootstrapRaft(); err != nil {
		t.Fatal(err)
	}
	<-monitored
	if setupCalls != 1 {
		t.Errorf("Raft set up %d times, want 1", setupCalls)
	}
}

func TestRaftIsNotSetUpWhenDisabled(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	m.base.RaftEnabled = false
	prev := raftSetup
	defer func() { raftSetup = prev }()
	raftSetup = func(orcraft.CommandApplier, orcraft.SnapshotCreatorApplier, string) error {
		t.Error("raft must not be set up when disabled")
		return nil
	}
	if err := m.bootstrapRaft(); err != nil {
		t.Fatal(err)
	}
}

// A node that does not run a tenant of the log drops its commands with
// ErrUnknownTenant (counted by Raft), never panics.
func TestUnknownTenantCommandIsRejectedNotApplied(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	res := NewCommandApplier(m).ApplyTenantCommand("not-on-this-node", "enable-global-recoveries", nil)
	if err, ok := res.(error); !ok || !errors.Is(err, orcraft.ErrUnknownTenant) {
		t.Errorf("expected ErrUnknownTenant, got %v", res)
	}
}
