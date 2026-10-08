package logic

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/inst"
)

// populateTenant fills the backend of a tenant with n instances and the auxiliary tables a
// production tenant carries (tags, downtimes, pool, candidates, resolves, KV, recoveries and their steps).
func populateTenant(tb testing.TB, tn *Tenant, n int) {
	tb.Helper()
	tenantWriteInstance(tb.(*testing.T), tn, "template", 3306)
	rows, err := tn.Backend.Open()
	if err != nil {
		tb.Fatal(err)
	}
	var cols []string
	if err := tn.Backend.QueryRowsMap("select * from database_instance limit 1", func(m sqlutils.RowMap) error {
		for c := range m {
			if c != "hostname" && c != "port" {
				cols = append(cols, c)
			}
		}
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
	list := ""
	for _, c := range cols {
		list += "," + c
	}
	gen := func(stmt string, count int) {
		tb.Helper()
		q := fmt.Sprintf("with recursive cnt(n) as (select 1 union all select n+1 from cnt where n < %d) %s", count, stmt)
		if _, err := rows.Exec(q); err != nil {
			tb.Fatalf("%v\n%s", err, q)
		}
	}
	gen(fmt.Sprintf("insert into database_instance (hostname, port%s) select 'db'||n||'.example.com', 3306%s from cnt, (select * from database_instance where hostname='template') t", list, list), n)
	gen("insert into database_instance_tags (hostname, port, tag_name, tag_value) select 'db'||n||'.example.com', 3306, 'role', 'replica-'||n from cnt", n/2)
	gen("insert into database_instance_pool (hostname, port, pool) select 'db'||n||'.example.com', 3306, 'pool'||(n%10) from cnt", n/4)
	gen("insert into database_instance_downtime (hostname, port, downtime_active, begin_timestamp, end_timestamp, owner, reason) select 'db'||n||'.example.com', 3306, 1, datetime('now'), datetime('now','+1 hour'), 'me', 'maintenance window' from cnt", n/10)
	gen("insert into candidate_database_instance (hostname, port, promotion_rule) select 'db'||n||'.example.com', 3306, 'prefer' from cnt", n/20)
	gen("insert into hostname_resolve (hostname, resolved_hostname) select 'alias'||n||'.example.com', 'db'||n||'.example.com' from cnt", n/2)
	gen("insert into kv_store (store_key, store_value) select 'mysql/master/cluster'||n, 'db'||n||'.example.com:3306' from cnt", n/20)
	gen("insert into topology_recovery (hostname, port, in_active_period, end_active_period_unixtime, uid, analysis, cluster_name, cluster_alias, successor_hostname, all_errors, processing_node_hostname, processcing_node_token) select 'db'||n||'.example.com', 3306, 0, n, 'uid-'||n, 'DeadMaster', 'cluster'||(n%100), 'alias', 'dbx', '', 'orch1.example.com', 'tokentokentoken' from cnt", n/10)
	gen("insert into topology_recovery_steps (recovery_uid, message) select 'uid-'||(n%(1+"+strconv.Itoa(n/10)+")+1), '- RecoverDeadMaster: relocating replicas of the failed master, step '||n from cnt", n)
}

// measureSequential is the cost of a snapshot as it was before it was made concurrent: the tenants read one
// after the other, then one json.Marshal and one gzip of everything.
func measureSequential(tb testing.TB, m *TenantManager) (read, marshal, compress time.Duration, jsonBytes, gzBytes int) {
	tb.Helper()
	start := time.Now()
	data := NewSnapshotData()
	for _, tn := range m.All() {
		ts, err := tn.createSnapshot()
		if err != nil {
			tb.Fatal(err)
		}
		data.Tenants[tn.ID] = ts
	}
	read = time.Since(start)
	start = time.Now()
	b, err := json.Marshal(data)
	if err != nil {
		tb.Fatal(err)
	}
	marshal = time.Since(start)
	start = time.Now()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(b)
	_ = zw.Close()
	compress = time.Since(start)
	return read, marshal, compress, len(b), buf.Len()
}

func snapshotCostManager(tb testing.TB, instances int) *TenantManager {
	m, _ := newTestManager(tb.(*testing.T), []string{"a", "b", "c"}, nil)
	for _, tn := range m.All() {
		populateTenant(tb, tn, instances)
	}
	return m
}

// Run: ORCH_SNAPSHOT_COST=20000,50000 go test ./go/logic/ -run TestSnapshotCost -v
func TestSnapshotCost(t *testing.T) {
	sizes := os.Getenv("ORCH_SNAPSHOT_COST")
	if testing.Short() || sizes == "" {
		t.Skip("set ORCH_SNAPSHOT_COST=<instances per tenant>[,..] to measure the cost of a snapshot")
	}
	for _, field := range splitCSV(sizes) {
		n, err := strconv.Atoi(field)
		if err != nil {
			t.Fatal(err)
		}
		m := snapshotCostManager(t, n)
		// the first read is cold; report the best of 3
		var best time.Duration
		var marshal, compress time.Duration
		var jsonBytes, gzBytes int
		for i := 0; i < 3; i++ {
			read, mar, comp, jb, gb := measureSequential(t, m)
			if i == 0 || read < best {
				best, marshal, compress, jsonBytes, gzBytes = read, mar, comp, jb, gb
			}
		}
		t.Logf("SNAPSHOT COST sequential (before): 3 tenants x %d instances: read %v + json.Marshal %v + gzip %v = %v, json %.1f MB, gzip %.1f MB",
			n, best, marshal, compress, best+marshal+compress, float64(jsonBytes)/1e6, float64(gzBytes)/1e6)
		// what the FSM actually waits for: GetData (concurrent per tenant)
		var bestGet time.Duration
		var encoded []byte
		for i := 0; i < 3; i++ {
			start := time.Now()
			data, err := NewSnapshotDataCreatorApplier(m).GetData()
			if err != nil {
				t.Fatal(err)
			}
			if d := time.Since(start); i == 0 || d < bestGet {
				bestGet, encoded = d, data
			}
		}
		decoded, err := decodeSnapshot(bytes.NewReader(encoded))
		if err != nil || len(decoded.Tenants) != 3 || len(decoded.Tenants["a"].MinimalInstances) != n+1 {
			t.Fatalf("GetData does not decode back: %v", err)
		}
		t.Logf("SNAPSHOT COST concurrent per tenant (now): GetData, the FSM blocked for %v, snapshot %.1f MB", bestGet, float64(len(encoded))/1e6)
		// per tenant
		for _, tn := range m.All() {
			start := time.Now()
			if _, err := tn.createSnapshot(); err != nil {
				t.Fatal(err)
			}
			t.Logf("  tenant %s read: %v", tn.ID, time.Since(start))
		}
	}
}

func splitCSV(s string) (out []string) {
	cur := ""
	for _, r := range s {
		if r == ',' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	return append(out, cur)
}

var _ = inst.InstanceKey{}
