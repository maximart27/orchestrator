package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/config"
)

func sqliteConfig(t *testing.T, file string) *config.Configuration {
	t.Helper()
	cfg := *config.Config
	cfg.BackendDB = "sqlite3"
	cfg.SQLite3DataFile = file
	return &cfg
}

func newTestBackend(t *testing.T, id string) *Backend {
	t.Helper()
	b, err := NewBackend(id, sqliteConfig(t, filepath.Join(t.TempDir(), id+".sqlite3")))
	if err != nil {
		t.Fatalf("NewBackend(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.EnsureSchema(); err != nil {
		t.Fatalf("EnsureSchema(%s): %v", id, err)
	}
	return b
}

func countRows(t *testing.T, b *Backend, table string) int {
	t.Helper()
	n := -1
	err := b.Query(fmt.Sprintf("select count(*) as c from %s", table), nil, func(m sqlutils.RowMap) error {
		n = m.GetInt("c")
		return nil
	})
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestBackendsAreIsolated(t *testing.T) {
	a := newTestBackend(t, "a")
	b := newTestBackend(t, "b")

	for _, be := range []*Backend{a, b} {
		// schema exists in both
		if n := countRows(t, be, "database_instance"); n != 0 {
			t.Fatalf("%s: expected empty database_instance, got %d", be.ID(), n)
		}
	}
	if _, err := a.Exec(`insert into cluster_alias (cluster_name, alias, last_registered) values (?, ?, now())`, "c1", "alias-a"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if n := countRows(t, a, "cluster_alias"); n != 1 {
		t.Errorf("a: expected 1 row, got %d", n)
	}
	if n := countRows(t, b, "cluster_alias"); n != 0 {
		t.Errorf("write on a leaked into b: %d rows", n)
	}
	dbA, _ := a.Open()
	dbB, _ := b.Open()
	if dbA == dbB {
		t.Errorf("backends share a *sql.DB")
	}
}

func TestEnsureSchemaIdempotentAndRecordsVersion(t *testing.T) {
	a := newTestBackend(t, "a")
	if err := a.EnsureSchema(); err != nil {
		t.Fatalf("second EnsureSchema: %v", err)
	}
	if n := countRows(t, a, "orchestrator_db_deployments"); n < 1 {
		t.Errorf("deployment not registered: %d rows", n)
	}
}

func TestBackendsConcurrent(t *testing.T) {
	a := newTestBackend(t, "a")
	b := newTestBackend(t, "b")
	var wg sync.WaitGroup
	const perBackend = 8
	const writes = 25
	for _, be := range []*Backend{a, b} {
		for g := 0; g < perBackend; g++ {
			wg.Add(1)
			go func(be *Backend, g int) {
				defer wg.Done()
				for i := 0; i < writes; i++ {
					if _, err := be.Exec(`replace into cluster_alias (cluster_name, alias, last_registered) values (?, ?, now())`, fmt.Sprintf("c-%d-%d", g, i), fmt.Sprintf("%s-%d-%d", be.ID(), g, i)); err != nil {
						t.Errorf("%s exec: %v", be.ID(), err)
						return
					}
					if _, err := be.ReadTimeNow(); err != nil {
						t.Errorf("%s ReadTimeNow: %v", be.ID(), err)
						return
					}
				}
			}(be, g)
		}
	}
	wg.Wait()
	for _, be := range []*Backend{a, b} {
		if n := countRows(t, be, "cluster_alias"); n != perBackend*writes {
			t.Errorf("%s: expected %d rows, got %d", be.ID(), perBackend*writes, n)
		}
	}
}

func TestNewBackendErrors(t *testing.T) {
	good := sqliteConfig(t, filepath.Join(t.TempDir(), "x.sqlite3"))

	bad := sqliteConfig(t, filepath.Join(t.TempDir(), "no", "such", "dir", "x.sqlite3"))
	if b, err := NewBackend("bad", bad); err == nil {
		_ = b.Close()
		t.Errorf("impossible sqlite path: expected error")
	}
	noFile := sqliteConfig(t, "")
	if _, err := NewBackend("nofile", noFile); err == nil {
		t.Errorf("empty SQLite3DataFile: expected error")
	}
	weird := *good
	weird.BackendDB = "oracle"
	if _, err := NewBackend("weird", &weird); err == nil {
		t.Errorf("unsupported BackendDB: expected error")
	}
	if _, err := NewBackend("", good); err == nil {
		t.Errorf("empty id: expected error")
	}
	if _, err := NewBackend("nilcfg", nil); err == nil {
		t.Errorf("nil cfg: expected error")
	}
	// MySQL needs no network at construction
	my := *config.Config
	my.BackendDB = "mysql"
	b, err := NewBackend("mysql", &my)
	if err != nil {
		t.Fatalf("mysql NewBackend: %v", err)
	}
	_ = b.Close()
}

func TestSchemaFailureReturnsError(t *testing.T) {
	cfg := sqliteConfig(t, filepath.Join(t.TempDir(), "x.sqlite3"))
	cfg.PanicIfDifferentDatabaseDeploy = true
	prev := config.RuntimeCLIFlags.ConfiguredVersion
	config.RuntimeCLIFlags.ConfiguredVersion = "mt-test-version"
	defer func() { config.RuntimeCLIFlags.ConfiguredVersion = prev }()
	b, err := NewBackend("p", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.EnsureSchema(); err == nil {
		t.Errorf("expected PanicIfDifferentDatabaseDeploy to be reported as an error")
	}
}

func TestClosedBackend(t *testing.T) {
	b := newTestBackend(t, "a")
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if _, err := b.Exec("select 1"); !errors.Is(err, ErrBackendClosed) {
		t.Errorf("expected ErrBackendClosed, got %v", err)
	}
	if _, err := b.OpenTopology("127.0.0.1", 3306); !errors.Is(err, ErrBackendClosed) {
		t.Errorf("OpenTopology on closed backend: %v", err)
	}
}

func TestTLSNamesAreUniquePerBackend(t *testing.T) {
	a := newTestBackend(t, "a")
	b := newTestBackend(t, "b")
	if a.topologyTLSName() == b.topologyTLSName() || a.orchestratorTLSName() == b.orchestratorTLSName() {
		t.Fatalf("TLS names collide: %s/%s %s/%s", a.topologyTLSName(), b.topologyTLSName(), a.orchestratorTLSName(), b.orchestratorTLSName())
	}
	if a.topologyTLSName() != "topology-a" || b.orchestratorTLSName() != "orchestrator-b" {
		t.Errorf("unexpected names %s %s", a.topologyTLSName(), b.orchestratorTLSName())
	}
	if DefaultBackend().topologyTLSName() != "topology" || DefaultBackend().orchestratorTLSName() != "orchestrator" {
		t.Errorf("default backend must keep the legacy TLS names")
	}
	for _, be := range []*Backend{a, b} {
		uri, err := be.setupTopologyTLS("u:p@tcp(h:3306)/?x=1")
		if err != nil {
			t.Fatalf("setupTopologyTLS: %v", err)
		}
		if !strings.HasSuffix(uri, "&tls=topology-"+be.ID()) {
			t.Errorf("topology uri %q does not reference its own TLS config", uri)
		}
		uri, err = be.setupOrchestratorTLS("u:p@tcp(h:3306)/?x=1")
		if err != nil {
			t.Fatalf("setupOrchestratorTLS: %v", err)
		}
		if !strings.HasSuffix(uri, "&tls=orchestrator-"+be.ID()) {
			t.Errorf("orchestrator uri %q does not reference its own TLS config", uri)
		}
	}
	// state is per backend
	if !a.topologyTLSConfigured || !b.topologyTLSConfigured || DefaultBackend().topologyTLSConfigured {
		t.Errorf("TLS configured flags must be per backend")
	}
}

func TestTLSCacheIsPerBackendWithConfigTTL(t *testing.T) {
	a := newTestBackend(t, "a")
	b := newTestBackend(t, "b")
	if a.tlsCache() == b.tlsCache() {
		t.Errorf("requireTLS cache shared between backends")
	}
	a.tlsCache().SetDefault("h:1", true)
	if _, found := b.tlsCache().Get("h:1"); found {
		t.Errorf("requireTLS entry leaked between backends")
	}
}

func TestTopologyPoolsArePerBackendAndTenantCredentials(t *testing.T) {
	a := newTestBackend(t, "a")
	a.cfg.MySQLTopologyUser = "usera"
	b := newTestBackend(t, "b")
	b.cfg.MySQLTopologyUser = "userb"
	b.cfg.MySQLTopologyMaxAllowedPacket = 4242

	da, err := a.OpenTopology("127.0.0.1", 3306)
	if err != nil {
		t.Fatal(err)
	}
	da2, _ := a.OpenTopology("127.0.0.1", 3306)
	db, err := b.OpenTopology("127.0.0.1", 3306)
	if err != nil {
		t.Fatal(err)
	}
	if da != da2 {
		t.Errorf("same backend, same server: expected the pool to be reused")
	}
	if da == db {
		t.Errorf("pools shared between backends")
	}
	// Discovery uses its own read timeout, hence its own DSN/pool.
	dd, _ := a.OpenDiscovery("127.0.0.1", 3306)
	if dd == da {
		t.Errorf("discovery and topology should not share a pool (different readTimeout)")
	}
	_ = a.Close()
	if err := da.Ping(); err == nil {
		t.Errorf("topology pool should be closed together with its backend")
	}
}

// The package-level functions must keep behaving as before: they drive the
// global config.Config and follow its changes.
func TestShimsFollowGlobalConfig(t *testing.T) {
	origDB, origFile := config.Config.BackendDB, config.Config.SQLite3DataFile
	defer func() { config.Config.BackendDB, config.Config.SQLite3DataFile = origDB, origFile }()

	config.Config.BackendDB = "sqlite"
	first := filepath.Join(t.TempDir(), "one.sqlite3")
	config.Config.SQLite3DataFile = first
	if !IsSQLite() {
		t.Fatalf("IsSQLite should follow config.Config")
	}
	// schema is deployed lazily, on first use, like it always was
	if _, err := ExecOrchestrator(`insert into cluster_alias (cluster_name, alias, last_registered) values (?, ?, now())`, "c", "one"); err != nil {
		t.Fatalf("ExecOrchestrator: %v", err)
	}
	rows := 0
	if err := QueryOrchestrator(`select alias from cluster_alias where cluster_name = ?`, sqlutils.Args("c"), func(m sqlutils.RowMap) error {
		rows++
		if m.GetString("alias") != "one" {
			t.Errorf("unexpected alias %q", m.GetString("alias"))
		}
		return nil
	}); err != nil || rows != 1 {
		t.Fatalf("QueryOrchestrator: rows=%d err=%v", rows, err)
	}
	if err := QueryOrchestratorRowsMap(`select * from cluster_alias`, func(sqlutils.RowMap) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := QueryOrchestratorBuffered(`select * from cluster_alias`, nil, func(sqlutils.RowMap) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := QueryOrchestratorRowsMapBuffered(`select * from cluster_alias`, func(sqlutils.RowMap) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if now, err := ReadTimeNow(); err != nil || now == "" {
		t.Fatalf("ReadTimeNow: %q %v", now, err)
	}
	h1, err := OpenOrchestrator()
	if err != nil {
		t.Fatal(err)
	}
	if h2, _ := DefaultBackend().Open(); h1 != h2 {
		t.Errorf("OpenOrchestrator should return the default backend's handle")
	}
	if h3, _ := OpenOrchestrator(); h1 != h3 {
		t.Errorf("OpenOrchestrator should reuse the handle")
	}

	// pointing config.Config elsewhere switches the backend, with a fresh schema
	config.Config.SQLite3DataFile = filepath.Join(t.TempDir(), "two.sqlite3")
	n := -1
	if err := QueryOrchestrator(`select count(*) as c from cluster_alias`, nil, func(m sqlutils.RowMap) error {
		n = m.GetInt("c")
		return nil
	}); err != nil || n != 0 {
		t.Fatalf("after switching file: n=%d err=%v", n, err)
	}
	if h4, _ := OpenOrchestrator(); h4 == h1 {
		t.Errorf("expected a different handle after changing SQLite3DataFile")
	}
	// and going back reaches the first file again, with its data
	config.Config.SQLite3DataFile = first
	if h5, _ := OpenOrchestrator(); h5 != h1 {
		t.Errorf("expected the first handle back")
	}
	n = -1
	if err := QueryOrchestrator(`select count(*) as c from cluster_alias`, nil, func(m sqlutils.RowMap) error {
		n = m.GetInt("c")
		return nil
	}); err != nil || n != 1 {
		t.Fatalf("back on first file: n=%d err=%v", n, err)
	}
}

func TestDefaultBackendIsSingleton(t *testing.T) {
	var wg sync.WaitGroup
	got := make([]*Backend, 16)
	for i := range got {
		wg.Add(1)
		go func(i int) { defer wg.Done(); got[i] = DefaultBackend() }(i)
	}
	wg.Wait()
	for _, g := range got {
		if g != got[0] {
			t.Fatalf("DefaultBackend returned different instances")
		}
	}
	if got[0].ID() != DefaultBackendID {
		t.Errorf("unexpected default id %q", got[0].ID())
	}
}
