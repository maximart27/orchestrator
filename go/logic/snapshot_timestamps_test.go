package logic

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"
)

const activeRecoveries = `select count(*) as c from topology_recovery where in_active_period = 1`

// The recoveries a node restores from a snapshot must leave their active period like the ones it
// wrote itself: a restored leader that kept them forever would refuse to fail the cluster over.
func TestRestoredRecoveriesLeaveTheirActivePeriod(t *testing.T) {
	leaderM, _ := newTestManager(t, []string{"a"}, nil)
	restoredM, _ := newTestManager(t, []string{"a"}, nil)
	leader, restored := mustTenant(t, leaderM, "a"), mustTenant(t, restoredM, "a")

	execOn(t, leader, `insert into topology_recovery
		(hostname, port, in_active_period, start_active_period, end_active_period_unixtime, processing_node_hostname, processcing_node_token,
		 uid, analysis, cluster_name, cluster_alias, count_affected_slaves, slave_hosts, acknowledged_by, acknowledge_comment,
		 participating_instances, lost_slaves, all_errors, last_detection_id)
		values ('db1', 3306, 1, now() - interval 2 hour, 0, 'n1', 'tok', 'uid-1', '', '', '', 0, '', '', '', '', '', '', 0)`)
	restoreInto(t, restoredM, snapshotOf(t, leaderM))

	if n := countRowsOf(t, restored, activeRecoveries); n != 1 {
		t.Fatalf("the restored recovery should be active, got %d", n)
	}
	if err := restored.ClearActiveRecoveries(); err != nil {
		t.Fatal(err)
	}
	if n := countRowsOf(t, restored, activeRecoveries); n != 0 {
		t.Fatalf("a recovery older than RecoveryPeriodBlockSeconds restored from a snapshot is still active (%d)", n)
	}
}

// Defence in depth: rows already restored in RFC3339 (by a node without the fix) are still cleared.
func TestClearActiveRecoveriesReadsRFC3339Timestamps(t *testing.T) {
	skipOnMySQL(t, "RFC3339 text in a time column is a SQLite-driver artefact; MySQL rejects it")
	m, _ := newTestManager(t, []string{"a"}, nil)
	a := mustTenant(t, m, "a")
	// a minute ago, on the same day as now(): only then does the text comparison depend on the 'T'
	a.Cfg.RecoveryPeriodBlockSeconds = 30
	a.Cfg.FailureDetectionPeriodBlockMinutes = 0
	oneMinuteAgo := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	execOn(t, a, `insert into topology_recovery
		(hostname, port, in_active_period, start_active_period, end_active_period_unixtime, processing_node_hostname, processcing_node_token)
		values ('db1', 3306, 1, ?, 0, 'n1', 'tok')`, oneMinuteAgo)
	execOn(t, a, `insert into topology_failure_detection
		(hostname, port, in_active_period, start_active_period, end_active_period_unixtime, processing_node_hostname, processcing_node_token, analysis, cluster_name, cluster_alias, count_affected_slaves, slave_hosts)
		values ('db1', 3306, 1, ?, 0, 'n1', 'tok', 'DeadMaster', 'c', 'c', 0, '')`, oneMinuteAgo)
	if err := a.ClearActiveRecoveries(); err != nil {
		t.Fatal(err)
	}
	if err := a.ClearActiveFailureDetections(); err != nil {
		t.Fatal(err)
	}
	if n := countRowsOf(t, a, activeRecoveries); n != 0 {
		t.Fatalf("recovery in RFC3339 not cleared (%d)", n)
	}
	if n := countRowsOf(t, a, `select count(*) as c from topology_failure_detection where in_active_period = 1`); n != 0 {
		t.Fatalf("detection in RFC3339 not cleared (%d)", n)
	}
}

// For each table a snapshot carries: whatever time columns it has, a snapshot that holds them as
// RFC3339 (what every node has written so far) is restored as text that compares with now().
func TestRestoredTimestampsAreComparableWithNowInEveryTable(t *testing.T) {
	skipOnMySQL(t, "inspects the SQLite schema (pragma table_info) and its RFC3339 time columns")
	m, _ := newTestManager(t, []string{"a"}, nil)
	a := mustTenant(t, m, "a")
	orcdb, err := a.Backend.Open()
	if err != nil {
		t.Fatal(err)
	}
	probe := &TenantSnapshot{}
	for _, table := range probe.tables() {
		timeColumns, err := backendTimeColumns(orcdb, true, table.name)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(table.name, func(t *testing.T) {
			rows, err := orcdb.Query(fmt.Sprintf("pragma table_info(%s)", table.name))
			if err != nil {
				t.Fatal(err)
			}
			var columns []string
			var row sqlutils.RowData
			for rows.Next() {
				var cid, notNull, pk int
				var name, declaredType string
				var dflt *string
				if err := rows.Scan(&cid, &name, &declaredType, &notNull, &dflt, &pk); err != nil {
					t.Fatal(err)
				}
				columns = append(columns, name)
				value := "1"
				if name == "promotion_rule" {
					value = "neutral" // CHECK constraint
				}
				if timeColumns[strings.ToLower(name)] {
					value = "2026-10-08T23:22:36Z"
				}
				row = append(row, sqlutils.CellData{String: value, Valid: true})
			}
			_ = rows.Close()
			if len(timeColumns) == 0 {
				t.Skipf("%s has no time column", table.name)
			}
			table.data.Columns = columns
			table.data.Data = sqlutils.ResultData{row}
			if err := a.writeTables([]snapshotTable{table}); err != nil {
				t.Fatal(err)
			}
			for column := range timeColumns {
				var stored string
				// cast: the driver would turn a TIMESTAMP column into a time.Time and back into RFC3339
				query := fmt.Sprintf("select cast(%s as text) from %s limit 1", column, table.name)
				if err := orcdb.QueryRow(query).Scan(&stored); err != nil {
					t.Fatalf("%s: %v", query, err)
				}
				if stored != "2026-10-08 23:22:36" {
					t.Errorf("%s.%s restored as %q, want the canonical \"2026-10-08 23:22:36\"", table.name, column, stored)
				}
				// comparable with now(): plain text comparison, as the DAOs do
				var older int
				query = fmt.Sprintf("select count(*) from %s where %s < datetime('now')", table.name, column)
				if err := orcdb.QueryRow(query).Scan(&older); err != nil || older != 1 {
					t.Errorf("%s.%s: not older than now() as text (%d, %v)", table.name, column, older, err)
				}
			}
		})
	}
}

// A snapshot taken from SQLite no longer carries RFC3339 text either, and what is not a time column
// (a tag value that merely looks like a timestamp) is never rewritten.
func TestSnapshotCreationWritesCanonicalTimestamps(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	a := mustTenant(t, m, "a")
	execOn(t, a, `insert into candidate_database_instance (hostname, port, last_suggested) values ('db1', 3306, now())`)
	execOn(t, a, `insert into database_instance_tags (hostname, port, tag_name, tag_value) values ('db1', 3306, 'n', '2026-10-08T23:22:36Z')`)
	ts, err := a.createSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := ts.Candidates.Data[0][indexOf(ts.Candidates.Columns, "last_suggested")].String; strings.Contains(got, "T") {
		t.Fatalf("snapshot carries last_suggested as %q", got)
	}
	if got := ts.InstanceTags.Data[0][indexOf(ts.InstanceTags.Columns, "tag_value")].String; got != "2026-10-08T23:22:36Z" {
		t.Fatalf("a tag value was rewritten to %q", got)
	}
}

func indexOf(columns []string, name string) int {
	for i, c := range columns {
		if c == name {
			return i
		}
	}
	return -1
}
