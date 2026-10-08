package logic

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/inst"
)

func countRows(t *testing.T, tn *Tenant, table string) (n int) {
	t.Helper()
	if err := tn.Backend.QueryRowsMap("select count(*) as c from "+table, func(m sqlutils.RowMap) error {
		n = m.GetInt("c")
		return nil
	}); err != nil {
		t.Fatalf("tenant %s: counting %s: %v", tn.ID, table, err)
	}
	return n
}

func execOn(t *testing.T, tn *Tenant, query string, args ...interface{}) {
	t.Helper()
	if _, err := tn.Backend.Exec(query, args...); err != nil {
		t.Fatalf("tenant %s: %s: %v", tn.ID, query, err)
	}
}

func snapshotOf(t *testing.T, m *TenantManager) []byte {
	t.Helper()
	data, err := NewSnapshotDataCreatorApplier(m).GetData()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func restoreInto(t *testing.T, m *TenantManager, data []byte) {
	t.Helper()
	if err := NewSnapshotDataCreatorApplier(m).Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
}

// The tables that only the applier writes are emptied of what the snapshot does not hold; the
// tables the node also writes by itself (and the control data in access_token) are only added to.
func TestSnapshotRestoreKeepsWhatTheNodeWritesItself(t *testing.T) {
	leader, _ := newTestManager(t, []string{"a"}, nil)
	stale, _ := newTestManager(t, []string{"a"}, nil)
	for _, m := range []*TenantManager{leader, stale} {
		tenantWriteInstance(t, mustTenant(t, m, "a"), "db1", 3306)
	}
	st := mustTenant(t, stale, "a")
	tenantWriteInstance(t, st, "found-locally", 3306) // recently discovered, not in the snapshot
	execOn(t, st, `insert into hostname_resolve (hostname, resolved_hostname) values ('h', 'r')`)
	execOn(t, st, `insert into host_attributes (hostname, attribute_name, attribute_value) values ('h', 'n', 'v')`)
	execOn(t, st, `insert into cluster_domain_name (cluster_name, domain_name) values ('c', 'd')`)
	execOn(t, st, `insert into cluster_alias (cluster_name, alias) values ('c', 'al')`)
	execOn(t, st, `insert into access_token (public_token, secret_token, generated_by) values ('p', 's', 'me')`)
	// raft-only rows the leader does not have
	tagCommand(t, NewCommandApplier(stale), "a", "db1", "doomed", "x")
	execOn(t, st, `insert into kv_store (store_key, store_value) values ('k', 'v')`)
	execOn(t, st, `insert into database_instance_pool (hostname, port, pool) values ('db1', 3306, 'p')`)

	restoreInto(t, stale, snapshotOf(t, leader))

	if !tenantHasInstance(t, st, "found-locally", 3306) {
		t.Error("a recently discovered instance was forgotten")
	}
	for _, table := range []string{"hostname_resolve", "host_attributes", "cluster_domain_name", "cluster_alias", "access_token"} {
		if n := countRows(t, st, table); n != 1 {
			t.Errorf("%s (written by the node itself): %d rows after the restore, want the 1 it had", table, n)
		}
	}
	for _, table := range []string{"database_instance_tags", "kv_store", "database_instance_pool"} {
		if n := countRows(t, st, table); n != 0 {
			t.Errorf("%s (written by Raft only): %d rows after the restore, want 0 (the leader has none)", table, n)
		}
	}
}

// The restore of the tables is one transaction: when it fails (here, a table of the snapshot with a
// column this schema does not have) nothing of it is applied, so the node is not left half restored.
func TestSnapshotRestoreOfTablesIsAtomic(t *testing.T) {
	src, _ := newTestManager(t, []string{"a"}, nil)
	dst, _ := newTestManager(t, []string{"a"}, nil)
	for _, m := range []*TenantManager{src, dst} {
		tenantWriteInstance(t, mustTenant(t, m, "a"), "db1", 3306)
	}
	tagCommand(t, NewCommandApplier(dst), "a", "db1", "mine", "1")
	tagCommand(t, NewCommandApplier(src), "a", "db1", "other", "2")
	tagCommand(t, NewCommandApplier(src), "a", "db1", "other2", "2")

	snapshot, err := decodeSnapshot(bytes.NewReader(snapshotOf(t, src)))
	if err != nil {
		t.Fatal(err)
	}
	ts := snapshot.Tenants["a"]
	// kv_store comes after the tags: a failure there must undo the tags it had already replaced
	ts.KVStore = sqlutils.NamedResultData{
		Columns: []string{"store_key", "no_such_column"},
		Data:    sqlutils.ResultData{sqlutils.RowData{{String: "k", Valid: true}, {String: "v", Valid: true}}},
	}
	if err := mustTenant(t, dst, "a").restoreSnapshot(ts); err == nil || !strings.Contains(err.Error(), "kv_store") {
		t.Fatalf("restoring a snapshot with a bad table must fail naming it, got %v", err)
	}
	dt := mustTenant(t, dst, "a")
	if n := countRows(t, dt, "database_instance_tags"); n != 1 {
		t.Errorf("%d tags after a failed restore, want the 1 the node had (nothing applied)", n)
	}
	if n := countRows(t, dt, "kv_store"); n != 0 {
		t.Errorf("%d kv rows after a failed restore, want 0", n)
	}
}

// A table the snapshot does not carry at all (no columns: a snapshot of an older version) is not touched.
func TestSnapshotRestoreLeavesAbsentTablesAlone(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	tn := mustTenant(t, m, "a")
	tenantWriteInstance(t, tn, "db1", 3306)
	tagCommand(t, NewCommandApplier(m), "a", "db1", "t", "v")
	restoreInto(t, m, mustJSONGzip(t, map[string]interface{}{"FormatVersion": 1, "MinimalInstances": []inst.MinimalInstance{{Key: inst.InstanceKey{Hostname: "db1", Port: 3306}}}}))
	if n := countRows(t, tn, "database_instance_tags"); n != 1 {
		t.Errorf("a table absent from the snapshot was emptied: %d tags", n)
	}
}

func mustJSONGzip(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := io.ReadAll(gzipJSON(t, v))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A node that was down while rows were deleted (end-downtime, delete-instance-tag,
// submit-pool-instances of fewer instances, ...) and comes back through a snapshot RESTORE
// must not keep those rows: nothing replays a delete that is behind the snapshot. (It was
// the "additive restore" finding of rt-prop: the tables were written with REPLACE only.)

func TestSnapshotRestoreDropsRowsDeletedWhileTheNodeWasDown(t *testing.T) {
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
