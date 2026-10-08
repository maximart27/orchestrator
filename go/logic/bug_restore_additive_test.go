//go:build rtbugs

package logic

import (
	"bytes"
	"io"
	"testing"

	"github.com/proxysql/orchestrator/go/inst"
)

// BUG (rt-prop): Restore is additive. Tables are written with REPLACE INTO row by row and
// never deleted from (snapshot_data.go restoreSnapshot -> writeTableData ->
// sqlutils.WriteTable), so a node that was down while rows were deleted (end-downtime,
// delete-instance-tag, forget-cluster, submit-pool-instances of fewer instances, ...) and
// comes back through a snapshot RESTORE keeps those rows: it diverges from the node that
// created the snapshot, permanently (nothing replays a delete that is behind the snapshot).
// Only database_instance rows are reconciled (the "forget" loop with the PR #124 guard).
//
// Run: go test -tags rtbugs -run TestBugRestoreKeepsRowsDeletedWhileTheNodeWasDown ./go/logic/
func TestBugRestoreKeepsRowsDeletedWhileTheNodeWasDown(t *testing.T) {
	leader, _ := newTestManager(t, []string{"a"}, nil)
	stale, _ := newTestManager(t, []string{"a"}, nil)
	key := inst.InstanceKey{Hostname: "db1", Port: 3306}
	for _, m := range []*TenantManager{leader, stale} {
		tenantWriteInstance(t, mustTenant(t, m, "a"), "db1", 3306)
		applier := NewCommandApplier(m)
		tagCommand(t, applier, "a", "db1", "doomed", "x")
		if res := applier.ApplyTenantCommand("a", "begin-downtime", mustJSON(t, inst.NewDowntime(&key, "o", "r", 3600*1e9))); res != nil {
			t.Fatal(res)
		}
	}
	// the stale node goes down; meanwhile the leader deletes the tag and ends the downtime
	la := NewCommandApplier(leader)
	if res := la.ApplyTenantCommand("a", "delete-instance-tag", mustJSON(t, inst.InstanceTag{Key: key, T: inst.Tag{TagName: "doomed"}})); res != nil {
		t.Fatal(res)
	}
	if res := la.ApplyTenantCommand("a", "end-downtime", mustJSON(t, key)); res != nil {
		t.Fatal(res)
	}
	// the log is compacted: the stale node can only catch up through the snapshot
	data, err := NewSnapshotDataCreatorApplier(leader).GetData()
	if err != nil {
		t.Fatal(err)
	}
	if err := NewSnapshotDataCreatorApplier(stale).Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	if want, got := tenantDump(t, mustTenant(t, leader, "a")), tenantDump(t, mustTenant(t, stale, "a")); want != got {
		t.Fatalf("the node that restored the snapshot kept rows the leader had deleted:\n--- leader\n%s\n--- restored node\n%s", want, got)
	}
}
