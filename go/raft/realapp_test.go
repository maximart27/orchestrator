package orcraft_test

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/proxysql/golib/sqlutils"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/logic"
	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// realApp is a Raft state machine backed by the real logic.TenantManager: the real
// CommandApplier and SnapshotDataCreatorApplier over one SQLite backend per tenant.
// Its data (the SQLite files) survives Restarted, like a node whose process restarts.
type realApp struct {
	t    *testing.T
	base *config.Configuration
	tcs  map[string]*config.TenantConfig // every tenant that has a backend file, active or not

	mu     sync.RWMutex
	active []string
	m      *logic.TenantManager
	ca     *logic.CommandApplier
	sc     *logic.SnapshotDataCreatorApplier

	// rebuildOnRestart simulates a full process restart (new TenantManager over the same files)
	rebuildOnRestart bool
	leader           atomic.Bool
}

type leadership struct{ l *atomic.Bool }

func (l leadership) IsLeader() bool         { return l.l.Load() }
func (l leadership) IsLeaderOrActive() bool { return l.l.Load() }

// newRealApp creates a node whose manager runs the tenants `active` out of `all`.
func newRealApp(t *testing.T, all []string, active []string) *realApp {
	t.Helper()
	base := config.Config.Clone()
	base.BackendDB = "sqlite3"
	base.SQLite3DataFile = filepath.Join(t.TempDir(), "control.sqlite3")
	base.HostnameResolveMethod = "none"
	base.InstanceFlushIntervalMilliseconds = 20
	base.InstanceWriteBufferSize = 10
	base.DiscoveryMaxConcurrency = 3
	base.DeadInstanceDiscoveryMaxConcurrency = 2
	dir := t.TempDir()
	var entries []string
	for _, id := range all {
		entries = append(entries, fmt.Sprintf(`{"ID": %q, "Provider": "mysql",
			"Backend": {"BackendDB": "sqlite3", "SQLite3DataFile": %q},
			"Config": {"KVClusterMasterPrefix": "%s/master/"}}`, id, filepath.Join(dir, id+".sqlite3"), id))
	}
	tcs, err := config.ParseTenantConfigs(base, []byte(`{"Tenants": [`+strings.Join(entries, ",")+`]}`))
	if err != nil {
		t.Fatalf("tenant configuration: %v", err)
	}
	a := &realApp{t: t, base: base, tcs: map[string]*config.TenantConfig{}}
	for _, tc := range tcs {
		a.tcs[tc.ID] = tc
	}
	a.build(active)
	t.Cleanup(a.stop)
	return a
}

// build (re)creates the manager for the given tenants over the existing backend files.
func (a *realApp) build(active []string) {
	a.t.Helper()
	var tcs []*config.TenantConfig
	for _, id := range active {
		tcs = append(tcs, a.tcs[id])
	}
	m, err := logic.NewTenantManagerFor(a.base, tcs, logic.WithoutProcessBootstrap(), logic.WithLeadership(leadership{&a.leader}))
	if err != nil {
		a.t.Fatalf("building the tenant manager for %v: %v", active, err)
	}
	a.mu.Lock()
	a.active, a.m = append([]string(nil), active...), m
	a.ca, a.sc = logic.NewCommandApplier(m), logic.NewSnapshotDataCreatorApplier(m)
	a.mu.Unlock()
}

func (a *realApp) stop() {
	a.mu.RLock()
	m := a.m
	a.mu.RUnlock()
	if m != nil {
		m.Stop()
	}
}

// rebuild stops the manager and creates one for another set of tenants (a restart with a new tenants file).
func (a *realApp) rebuild(active []string) {
	a.stop()
	a.build(active)
}

func (a *realApp) manager() *logic.TenantManager {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.m
}

func (a *realApp) tenant(id string) *logic.Tenant {
	tn, ok := a.manager().Get(id)
	if !ok {
		a.t.Fatalf("tenant %s is not running on this node", id)
	}
	return tn
}

func (a *realApp) ApplyCommand(op string, value []byte) interface{} {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.ca.ApplyCommand(op, value)
}

func (a *realApp) ApplyTenantCommand(tenantID, op string, value []byte) interface{} {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.ca.ApplyTenantCommand(tenantID, op, value)
}

func (a *realApp) GetData() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.sc.GetData()
}

func (a *realApp) Restore(rc io.ReadCloser) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.sc.Restore(rc)
}

func (a *realApp) Restarted() {
	if a.rebuildOnRestart {
		a.rebuild(a.activeTenants())
	}
}

func (a *realApp) activeTenants() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return append([]string(nil), a.active...)
}

// dumpTables are the tables the raft commands and the snapshots carry.
var dumpTables = []string{
	"database_instance", "cluster_alias", "cluster_alias_override", "cluster_domain_name", "host_attributes",
	"database_instance_tags", "database_instance_pool", "hostname_resolve", "hostname_unresolve",
	"database_instance_downtime", "candidate_database_instance", "topology_failure_detection", "kv_store",
	"topology_recovery", "topology_recovery_steps", "cluster_injected_pseudo_gtid", "global_recovery_disable",
}

// nodeSpecificColumn matches the columns stamped with the clock of the node that applies a command.
var nodeSpecificColumn = regexp.MustCompile(`(?i)(timestamp|_at$|^last_|^time_|_time$|time$|unixtime|first_seen|start_active_period|end_recovery|expiry|registered)`)

// dumpTenant renders the replicated state of a tenant without node-specific stamps; tables listed in skip are left out.
func dumpTenant(t testing.TB, tn *logic.Tenant, skip ...string) string {
	t.Helper()
	skipped := map[string]bool{}
	for _, s := range skip {
		skipped[s] = true
	}
	var out strings.Builder
	for _, table := range dumpTables {
		if skipped[table] {
			continue
		}
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
			return fmt.Sprintf("<cannot dump %s: %v>", table, err)
		}
		sort.Strings(rows)
		fmt.Fprintf(&out, "[%s] %d\n", table, len(rows))
		for _, r := range rows {
			out.WriteString("  " + r + "\n")
		}
	}
	return out.String()
}

// Digest implements nodeApp. The recovery steps are left out of the comparison: they are
// not idempotent (see logic's TestApplierReapplyingACommandIsIdempotentOrDocumented) and a
// node that replays its log after a restart legitimately holds duplicates.
func (a *realApp) Digest(tenantID string) string {
	a.mu.RLock()
	m := a.m
	a.mu.RUnlock()
	tn, ok := m.Get(tenantID)
	if !ok {
		return "<tenant not running on this node>"
	}
	return dumpTenant(a.t, tn, "topology_recovery_steps")
}

// seedInstances writes the same instances in every tenant of the node: the state every
// node of a cluster already has (it discovered the same topology) before the commands start.
func (a *realApp) seedInstances(hosts ...string) {
	a.t.Helper()
	for _, id := range a.activeTenants() {
		tn := a.tenant(id)
		for _, h := range hosts {
			if err := tn.Store.WriteInstance(&inst.Instance{Key: inst.InstanceKey{Hostname: h, Port: 3306}}, false, nil); err != nil {
				a.t.Fatalf("tenant %s: WriteInstance(%s): %v", id, h, err)
			}
		}
	}
}

func newRealApps(t *testing.T, n int, all, active []string, seedHosts ...string) ([]nodeApp, []*realApp) {
	t.Helper()
	var apps []nodeApp
	var reals []*realApp
	for i := 0; i < n; i++ {
		a := newRealApp(t, all, active)
		a.seedInstances(seedHosts...)
		apps = append(apps, a)
		reals = append(reals, a)
	}
	return apps, reals
}

func jsonOf(t testing.TB, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var _ = orcraft.LegacyTenantID
