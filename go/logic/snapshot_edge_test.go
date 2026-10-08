package logic

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/inst"
)

func tenantTagCount(t *testing.T, tn *Tenant) (n int) {
	t.Helper()
	if err := tn.Backend.QueryRowsMap("select count(*) as c from database_instance_tags", func(m sqlutils.RowMap) error {
		n = m.GetInt("c")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func tagCommand(t *testing.T, applier *CommandApplier, tenant, host, name, value string) {
	t.Helper()
	payload := mustJSON(t, inst.InstanceTag{Key: inst.InstanceKey{Hostname: host, Port: 3306}, T: inst.Tag{TagName: name, TagValue: value}})
	if res := applier.ApplyTenantCommand(tenant, "put-instance-tag", payload); res != nil {
		t.Fatalf("put-instance-tag %s: %v", tenant, res)
	}
}

// Scenario 4, end to end: the node that creates the snapshot cannot read the backend of tenant b
// (it is omitted, not written as empty); the node that restores it, which does run b with its
// instances, must NOT forget them: b is absent from the snapshot, so it is left untouched.
func TestSnapshotOfAnUnreadableTenantDoesNotMakeAnotherNodeForgetItsInstances(t *testing.T) {
	src, _ := newTestManager(t, []string{"a", "b"}, nil)
	dst, _ := newTestManager(t, []string{"a", "b"}, nil)
	for _, m := range []*TenantManager{src, dst} {
		for _, id := range []string{"a", "b"} {
			tenantWriteInstance(t, mustTenant(t, m, id), "db1", 3306)
		}
	}
	// stale on dst for a long time: the guard of PR #124 would NOT protect it
	tenantWriteInstance(t, mustTenant(t, dst, "b"), "old-in-b", 3306)
	tenantSetLastSeen(t, mustTenant(t, dst, "b"), "old-in-b", 3306, 5000)
	tenantWriteInstance(t, mustTenant(t, dst, "a"), "old-in-a", 3306)
	tenantSetLastSeen(t, mustTenant(t, dst, "a"), "old-in-a", 3306, 5000)

	_ = mustTenant(t, src, "b").Backend.Close() // b unreadable on the node that snapshots
	data, err := NewSnapshotDataCreatorApplier(src).GetData()
	if err != nil {
		t.Fatal(err)
	}
	if err := NewSnapshotDataCreatorApplier(dst).Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	if !tenantHasInstance(t, mustTenant(t, dst, "b"), "old-in-b", 3306) || !tenantHasInstance(t, mustTenant(t, dst, "b"), "db1", 3306) {
		t.Error("tenant b was absent from the snapshot (unreadable): its instances must not be forgotten")
	}
	if tenantHasInstance(t, mustTenant(t, dst, "a"), "old-in-a", 3306) {
		t.Error("tenant a was in the snapshot without old-in-a, which is stale: it must be forgotten (control)")
	}
}

// Scenario 4: the PR #124 guard per tenant. An instance missing from the snapshot is forgotten
// only if it was not seen within the tenant's UnseenInstanceForgetHours; a recent one is kept.
func TestSnapshotRestoreGuardKeepsRecentInstancesAndForgetsStaleOnesPerTenant(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, nil)
	snapshot := NewSnapshotData()
	for _, id := range []string{"a", "b"} {
		tn := mustTenant(t, m, id)
		tenantWriteInstance(t, tn, "in-snapshot", 3306)
		tenantWriteInstance(t, tn, "recent", 3306)
		tenantWriteInstance(t, tn, "stale", 3306)
		tenantSetLastSeen(t, tn, "stale", 3306, 5000)
		snapshot.Tenants[id] = &TenantSnapshot{MinimalInstances: []inst.MinimalInstance{{Key: inst.InstanceKey{Hostname: "in-snapshot", Port: 3306}}}}
	}
	if err := NewSnapshotDataCreatorApplier(m).Restore(gzipJSON(t, snapshot)); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		tn := mustTenant(t, m, id)
		if !tenantHasInstance(t, tn, "recent", 3306) || !tenantHasInstance(t, tn, "in-snapshot", 3306) {
			t.Errorf("tenant %s: a recently seen instance (or one in the snapshot) was forgotten", id)
		}
		if tenantHasInstance(t, tn, "stale", 3306) {
			t.Errorf("tenant %s: a stale instance absent from the snapshot must be forgotten", id)
		}
	}
}

// Scenario 4: the tables of a v2 snapshot go to their own tenant only; a v1 snapshot (built by
// hand, with tables, the way the former code wrote it) is the state of tenant "default".
func TestSnapshotTablesStayInTheirTenant(t *testing.T) {
	src, _ := newTestManager(t, []string{"default", "a", "b"}, nil)
	applierSrc := NewCommandApplier(src)
	for _, id := range []string{"default", "a", "b"} {
		tenantWriteInstance(t, mustTenant(t, src, id), "db1", 3306)
	}
	tagCommand(t, applierSrc, "a", "db1", "t", "only-a")
	tagCommand(t, applierSrc, "a", "db1", "t2", "only-a")
	tagCommand(t, applierSrc, "default", "db1", "t", "only-default")
	data, err := NewSnapshotDataCreatorApplier(src).GetData()
	if err != nil {
		t.Fatal(err)
	}
	dst, _ := newTestManager(t, []string{"default", "a", "b"}, nil)
	if err := NewSnapshotDataCreatorApplier(dst).Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	if n := tenantTagCount(t, mustTenant(t, dst, "a")); n != 2 {
		t.Errorf("tenant a: %d tags restored, want 2", n)
	}
	if n := tenantTagCount(t, mustTenant(t, dst, "default")); n != 1 {
		t.Errorf("tenant default: %d tags restored, want 1", n)
	}
	if n := tenantTagCount(t, mustTenant(t, dst, "b")); n != 0 {
		t.Errorf("tenant b: %d tags, want 0: the tables of another tenant leaked into it", n)
	}

	// v1, by hand: the tables at the top level
	v1src := mustTenant(t, src, "a")
	ts, err := v1src.createSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	v1 := map[string]interface{}{"MinimalInstances": ts.MinimalInstances, "InstanceTags": ts.InstanceTags, "LeaderURI": "http://l:3000"}
	dst2, _ := newTestManager(t, []string{"default", "a", "b"}, nil)
	if err := NewSnapshotDataCreatorApplier(dst2).Restore(gzipJSON(t, v1)); err != nil {
		t.Fatal(err)
	}
	if n := tenantTagCount(t, mustTenant(t, dst2, "default")); n != 2 {
		t.Errorf("v1 snapshot: %d tags restored into default, want 2", n)
	}
	if n := tenantTagCount(t, mustTenant(t, dst2, "a")) + tenantTagCount(t, mustTenant(t, dst2, "b")); n != 0 {
		t.Errorf("v1 snapshot leaked %d tags into tenants other than default", n)
	}
}

// Scenario 4: a snapshot is created while commands keep being applied (GetData runs in Persist,
// concurrently with Apply on the nodes that did not yet take it eagerly, and in the FSM goroutine
// now): it always decodes and restores, with -race.
func TestSnapshotCreationConcurrentWithApplies(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, nil)
	applier := NewCommandApplier(m)
	for _, id := range []string{"a", "b"} {
		tenantWriteInstance(t, mustTenant(t, m, id), "db1", 3306)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				id := []string{"a", "b"}[(w+n)%2]
				tagCommand(t, applier, id, "db1", "t", time.Now().Format(time.RFC3339Nano))
				_ = applier.ApplyTenantCommand(id, "disable-global-recoveries", nil)
				_ = applier.ApplyTenantCommand(id, "enable-global-recoveries", nil)
			}
		}(w)
	}
	creator := NewSnapshotDataCreatorApplier(m)
	for i := 0; i < 15; i++ {
		data, err := creator.GetData()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeSnapshot(bytes.NewReader(data)); err != nil {
			t.Fatalf("snapshot %d taken during applies does not decode: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
}
