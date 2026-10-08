package inst

import (
	"path/filepath"
	"testing"

	"github.com/proxysql/golib/sqlutils"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
)

func storedProviderType(t *testing.T, s *Store, key InstanceKey) string {
	t.Helper()
	found := false
	providerType := ""
	err := s.Backend().Query(`select provider_type from database_instance where hostname = ? and port = ?`,
		sqlutils.Args(key.Hostname, key.Port), func(m sqlutils.RowMap) error {
			found = true
			providerType = m.GetString("provider_type")
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("%s: seed %+v was not stored", s.ID(), key)
	}
	return providerType
}

func newLegacyLikeStore(t *testing.T, providerType string) *Store {
	t.Helper()
	cfg := config.Config.Clone()
	cfg.BackendDB = "sqlite3"
	cfg.SQLite3DataFile = filepath.Join(t.TempDir(), "legacy.sqlite3")
	cfg.ProviderType = providerType
	cfg.HostnameResolveMethod = "none"
	backend, err := db.NewBackend("legacy-like", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	if err := backend.EnsureSchema(); err != nil {
		t.Fatal(err)
	}
	s, err := NewStore("legacy-like", cfg, backend, withLegacyMixedProviders())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestInjectSeedStoresTheEngineOfTheStore(t *testing.T) {
	pgTenant := newTestStore(t, "seed-pg", func(c *config.Configuration) { c.ProviderType = config.ProviderPostgreSQL })
	myTenant := newTestStore(t, "seed-my", nil)
	legacyPG := newLegacyLikeStore(t, config.ProviderPostgreSQL)
	legacyMy := newLegacyLikeStore(t, config.ProviderMySQL)

	for _, tc := range []struct {
		name string
		s    *Store
		port int
		want string
	}{
		{"tenant postgresql", pgTenant, 3306, "postgresql"},
		{"tenant mysql", myTenant, 3306, "mysql"},
		{"tenant mysql on 5432", myTenant, 5432, "mysql"},
		{"legacy configured as postgresql", legacyPG, 3306, "postgresql"},
		{"legacy mysql", legacyMy, 3306, "mysql"},
		{"legacy mysql, port heuristic", legacyMy, 5432, "postgresql"},
	} {
		key := InstanceKey{Hostname: "seed-host", Port: tc.port}
		if err := tc.s.InjectSeed(&key); err != nil {
			t.Fatalf("%s: InjectSeed: %v", tc.name, err)
		}
		if got := storedProviderType(t, tc.s, key); got != tc.want {
			t.Errorf("%s: seed stored with provider_type %q, want %q", tc.name, got, tc.want)
		}
		if got := tc.s.detectProviderType(&key); got != tc.want {
			t.Errorf("%s: detectProviderType = %q, want %q", tc.name, got, tc.want)
		}
	}
}
