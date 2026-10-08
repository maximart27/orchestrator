package logic

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/proxysql/golib/sqlutils"
)

// dumpTables are the tables whose content the Raft commands (and the snapshots) carry.
var dumpTables = []string{
	"database_instance", "cluster_alias", "cluster_alias_override", "cluster_domain_name", "host_attributes",
	"database_instance_tags", "database_instance_pool", "hostname_resolve", "hostname_unresolve",
	"database_instance_downtime", "candidate_database_instance", "topology_failure_detection", "kv_store",
	"topology_recovery", "topology_recovery_steps", "cluster_injected_pseudo_gtid", "global_recovery_disable",
}

// nodeSpecificColumn matches the columns that legitimately differ between the nodes that applied
// the same command: they are stamped with the clock of the node when it applies it.
var nodeSpecificColumn = regexp.MustCompile(`(?i)(timestamp|_at$|^last_|_time$|time$|unixtime|first_seen|start_active_period|end_recovery|expiry|registered)`)

// tenantDump renders the replicated state of a tenant, canonically and without node-specific stamps.
func tenantDump(t testing.TB, tn *Tenant) string {
	t.Helper()
	var out strings.Builder
	for _, table := range dumpTables {
		var rows []string
		err := tn.Backend.QueryRowsMap("select * from "+table, func(m sqlutils.RowMap) error {
			var cols []string
			for c := range m {
				if !nodeSpecificColumn.MatchString(c) {
					cols = append(cols, c)
				}
			}
			sort.Strings(cols)
			var cells []string
			for _, c := range cols {
				cells = append(cells, fmt.Sprintf("%s=%s", c, m.GetString(c)))
			}
			rows = append(rows, strings.Join(cells, " "))
			return nil
		})
		if err != nil {
			t.Fatalf("tenant %s: dumping %s: %v", tn.ID, table, err)
		}
		sort.Strings(rows)
		fmt.Fprintf(&out, "[%s] %d\n", table, len(rows))
		for _, r := range rows {
			out.WriteString("  " + r + "\n")
		}
	}
	return out.String()
}

// auditRows counts the rows of the audit table (node-local log of what the node did).
func auditRows(t testing.TB, tn *Tenant) int {
	t.Helper()
	n := 0
	if err := tn.Backend.QueryRowsMap("select count(*) as c from audit", func(m sqlutils.RowMap) error {
		n = m.GetInt("c")
		return nil
	}); err != nil {
		t.Fatalf("tenant %s: counting audit rows: %v", tn.ID, err)
	}
	return n
}
