package inst

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
)

// newTestStore builds a Store over its own SQLite file, like a tenant would.
func newTestStore(t *testing.T, id string, mutate func(*config.Configuration)) *Store {
	t.Helper()
	cfg := config.Config.Clone()
	cfg.BackendDB = "sqlite3"
	cfg.SQLite3DataFile = filepath.Join(t.TempDir(), id+".sqlite3")
	cfg.ProviderType = config.ProviderMySQL
	cfg.HostnameResolveMethod = "none"
	cfg.InstanceFlushIntervalMilliseconds = 20
	cfg.InstanceWriteBufferSize = 10
	if mutate != nil {
		mutate(cfg)
	}
	backend, err := db.NewBackend(id, cfg)
	if err != nil {
		t.Fatalf("NewBackend(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	if err := backend.EnsureSchema(); err != nil {
		t.Fatalf("EnsureSchema(%s): %v", id, err)
	}
	store, err := NewStore(id, cfg, backend)
	if err != nil {
		t.Fatalf("NewStore(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testInstance(key InstanceKey) *Instance {
	instance := NewInstance()
	instance.Key = key
	instance.Version = "10.6.0-MariaDB"
	instance.ServerID = 1
	return instance
}

func mustWriteInstance(t *testing.T, s *Store, key InstanceKey) {
	t.Helper()
	if err := s.WriteInstance(testInstance(key), true, nil); err != nil {
		t.Fatalf("%s: WriteInstance(%+v): %v", s.ID(), key, err)
	}
}

func instanceExists(t *testing.T, s *Store, key InstanceKey) bool {
	t.Helper()
	_, found, err := s.ReadInstance(&key)
	if err != nil {
		t.Fatalf("%s: ReadInstance(%+v): %v", s.ID(), key, err)
	}
	return found
}

func TestStoreInstancesAreIsolatedBetweenStores(t *testing.T) {
	a := newTestStore(t, "iso-a", nil)
	b := newTestStore(t, "iso-b", nil)
	key := InstanceKey{Hostname: "db1.example.com", Port: 3306}

	mustWriteInstance(t, a, key)
	if !instanceExists(t, a, key) {
		t.Fatalf("instance written in A not found in A")
	}
	if instanceExists(t, b, key) {
		t.Fatalf("instance written in A is visible in B (same host:port)")
	}

	// the same host:port can live in B with its own content
	other := testInstance(key)
	other.Version = "16.2"
	if err := b.WriteInstance(other, true, nil); err != nil {
		t.Fatalf("B WriteInstance: %v", err)
	}
	ia, _, _ := a.ReadInstance(&key)
	ib, _, _ := b.ReadInstance(&key)
	if ia.Version == ib.Version {
		t.Fatalf("A and B returned the same instance row: %q", ia.Version)
	}
}

func TestStoreForgetAndDeadInstancesFilterAreIsolated(t *testing.T) {
	a := newTestStore(t, "forget-a", nil)
	b := newTestStore(t, "forget-b", nil)
	key := InstanceKey{Hostname: "db1.example.com", Port: 3306}
	mustWriteInstance(t, a, key)
	mustWriteInstance(t, b, key)

	// forget in A
	if err := a.ForgetInstance(&key); err != nil {
		t.Fatalf("A ForgetInstance: %v", err)
	}
	if !a.InstanceIsForgotten(&key) {
		t.Errorf("A does not remember it forgot the instance")
	}
	if b.InstanceIsForgotten(&key) {
		t.Errorf("forgetting in A made B treat the instance as forgotten")
	}
	if instanceExists(t, a, key) {
		t.Errorf("forgotten instance still in A")
	}
	if !instanceExists(t, b, key) {
		t.Errorf("forgetting in A deleted the instance from B")
	}

	// dead instances filter
	a.DeadInstancesFilter.RegisterInstance(&key)
	if registered, _ := a.DeadInstancesFilter.InstanceRecheckNeeded(&key); !registered {
		t.Errorf("A did not register the dead instance")
	}
	if registered, _ := b.DeadInstancesFilter.InstanceRecheckNeeded(&key); registered {
		t.Errorf("dead instance registered in A is known by B")
	}
	a.DeadInstancesFilter.UnregisterInstance(&key)

	// the other caches are separate containers too
	a.recentInstantAnalysis.Set("k", "v", 0)
	a.clusterInjectedPseudoGTIDCache.Set("c", true, 0)
	a.instanceKeyInformativeClusterName.Set("i", "n", 0)
	a.instanceBinlogEntryCache.Set("b", 1, 0)
	a.supportedAutoPseudoGTIDWriters.Set("w", true, 0)
	a.hostnameIPsCache.Set("h", "1.2.3.4", 0)
	a.getHostnameResolvesLightweightCache().Set("r", "x", 0)
	for name, c := range map[string]interface {
		Get(string) (interface{}, bool)
	}{
		"recentInstantAnalysis":             b.recentInstantAnalysis,
		"clusterInjectedPseudoGTIDCache":    b.clusterInjectedPseudoGTIDCache,
		"instanceKeyInformativeClusterName": b.instanceKeyInformativeClusterName,
		"instanceBinlogEntryCache":          b.instanceBinlogEntryCache,
		"supportedAutoPseudoGTIDWriters":    b.supportedAutoPseudoGTIDWriters,
		"hostnameIPsCache":                  b.hostnameIPsCache,
		"hostnameResolvesLightweightCache":  b.getHostnameResolvesLightweightCache(),
	} {
		for _, k := range []string{"k", "c", "i", "b", "w", "h", "r"} {
			if _, found := c.Get(k); found {
				t.Errorf("B.%s sees an entry (%q) set in A", name, k)
			}
		}
	}
}

func TestStoreHonoursItsOwnConfiguration(t *testing.T) {
	a := newTestStore(t, "cfg-a", func(c *config.Configuration) {
		c.InstancePollSeconds = 10
		c.DefaultInstancePort = 3306
		c.InstanceWriteBufferSize = 7
	})
	b := newTestStore(t, "cfg-b", func(c *config.Configuration) {
		c.InstancePollSeconds = 1000
		c.DefaultInstancePort = 5432
		c.InstanceWriteBufferSize = 33
	})

	keyA, err := a.ParseRawInstanceKey("db.example.com")
	if err != nil {
		t.Fatalf("A ParseInstanceKey: %v", err)
	}
	keyB, err := b.ParseRawInstanceKey("db.example.com")
	if err != nil {
		t.Fatalf("B ParseInstanceKey: %v", err)
	}
	if keyA.Port != 3306 || keyB.Port != 5432 {
		t.Errorf("DefaultInstancePort not per store: A=%d B=%d", keyA.Port, keyB.Port)
	}

	if cap(a.instanceWriteBuffer) != 7 || cap(b.instanceWriteBuffer) != 33 {
		t.Errorf("InstanceWriteBufferSize not per store: A=%d B=%d", cap(a.instanceWriteBuffer), cap(b.instanceWriteBuffer))
	}

	// TTL of the forgotten keys cache is 3*InstancePollSeconds, per store
	key := InstanceKey{Hostname: "x.example.com", Port: 3306}
	mustWriteInstance(t, a, key)
	mustWriteInstance(t, b, key)
	_ = a.ForgetInstance(&key)
	_ = b.ForgetInstance(&key)
	expirationOf := func(s *Store) time.Duration {
		item, found := s.forgetInstanceKeys.Items()[key.StringCode()]
		if !found {
			t.Fatalf("%s: forgotten key not cached", s.ID())
		}
		return time.Until(time.Unix(0, item.Expiration))
	}
	if ea, eb := expirationOf(a), expirationOf(b); ea > 31*time.Second || eb < 2000*time.Second {
		t.Errorf("forget TTL not per store: A=%v B=%v", ea, eb)
	}

	if a.Config() == b.Config() {
		t.Errorf("stores share a configuration")
	}
}

func TestStoreCloseOfOneStoreDoesNotAffectTheOther(t *testing.T) {
	a := newTestStore(t, "close-a", nil)
	b := newTestStore(t, "close-b", nil)
	key := InstanceKey{Hostname: "db1.example.com", Port: 3306}

	a.enqueueInstanceWrite(testInstance(key), true, nil)
	if err := a.Close(); err != nil {
		t.Fatalf("A Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("A second Close: %v", err)
	}
	// the write buffered in A was flushed to A when it closed, never to B
	if !instanceExists(t, a, key) {
		t.Errorf("closing A lost its buffered write")
	}
	if instanceExists(t, b, key) {
		t.Errorf("A's buffered write ended in B")
	}

	// a closed store drops asynchronous writes instead of blocking forever
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			a.enqueueInstanceWrite(testInstance(InstanceKey{Hostname: fmt.Sprintf("late%d", i), Port: 1}), true, nil)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("enqueueInstanceWrite blocks on a closed store")
	}

	// B keeps working, including its asynchronous flusher
	b.enqueueInstanceWrite(testInstance(InstanceKey{Hostname: "db2.example.com", Port: 3306}), true, nil)
	deadline := time.Now().Add(5 * time.Second)
	for !instanceExists(t, b, InstanceKey{Hostname: "db2.example.com", Port: 3306}) {
		if time.Now().After(deadline) {
			t.Fatalf("B's write buffer was not flushed after A closed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if instanceExists(t, a, InstanceKey{Hostname: "db2.example.com", Port: 3306}) {
		t.Errorf("B's write ended in A")
	}
	mustWriteInstance(t, b, key)

	// metrics: A's are gone, B's remain
	if metrics.Get("tenants.close-a.instance.write") != nil {
		t.Errorf("A's metrics still registered after Close")
	}
	if metrics.Get("tenants.close-b.instance.write") == nil {
		t.Errorf("B's metrics unregistered by A.Close")
	}
}

func TestStoreMetricsAreNamedPerTenant(t *testing.T) {
	a := newTestStore(t, "metric-a", nil)
	if metrics.Get("tenants.metric-a.instance.read") == nil {
		t.Errorf("tenant metric not registered with its prefix")
	}
	if metrics.Get("tenants.metric-a.discoveries.dead_instances") == nil {
		t.Errorf("dead instances counter not registered with the tenant prefix")
	}
	// a second store of the same tenant replaces the metric instead of silently leaving the old one
	cfg := a.Config().Clone()
	b, err := NewStore("metric-a", cfg, a.Backend())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer func() { _ = b.Close() }()
	if metrics.Get("tenants.metric-a.instance.read") != b.readInstanceCounter {
		t.Errorf("the registry does not hold the metric of the newest store")
	}
	// the legacy default store keeps the historical, un-prefixed names
	DefaultStore()
	if metrics.Get("instance.read") == nil {
		t.Errorf("default store does not register the historical metric names")
	}
}

func TestStoreConcurrentUseOfTwoStores(t *testing.T) {
	a := newTestStore(t, "race-a", nil)
	b := newTestStore(t, "race-b", nil)
	var wg sync.WaitGroup
	for _, s := range []*Store{a, b} {
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func(s *Store, g int) {
				defer wg.Done()
				for i := 0; i < 15; i++ {
					key := InstanceKey{Hostname: fmt.Sprintf("host%d-%d", g, i), Port: 3306}
					s.enqueueInstanceWrite(testInstance(key), true, nil)
					s.DeadInstancesFilter.RegisterInstance(&key)
					s.DeadInstancesFilter.InstanceRecheckNeeded(&key)
					s.forgetInstanceKeys.Set("unrelated-"+key.StringCode(), true, 0)
					_, _, _ = s.ReadInstance(&key)
					_ = s.getHostnameResolvesLightweightCache()
					s.DeadInstancesFilter.UnregisterInstance(&key)
				}
			}(s, g)
		}
	}
	wg.Wait()
	// everything enqueued has reached the right backend by the time the stores close
	_ = a.Close()
	_ = b.Close()
	for _, s := range []*Store{a, b} {
		instances, err := s.FindInstances(".")
		if err != nil {
			t.Fatalf("%s FindInstances: %v", s.ID(), err)
		}
		if len(instances) != 4*15 {
			t.Errorf("%s: expected %d instances, got %d", s.ID(), 4*15, len(instances))
		}
	}
}

func TestStoreHasASingleEngine(t *testing.T) {
	pg := newTestStore(t, "engine-pg", func(c *config.Configuration) { c.ProviderType = config.ProviderPostgreSQL })
	my := newTestStore(t, "engine-my", nil)
	if pg.ProviderType() != config.ProviderPostgreSQL || pg.GetProvider().ProviderName() != "postgresql" {
		t.Errorf("PostgreSQL store: type %q provider %q", pg.ProviderType(), pg.GetProvider().ProviderName())
	}
	if my.ProviderType() != config.ProviderMySQL || my.GetProvider().ProviderName() != "mysql" {
		t.Errorf("MySQL store: type %q provider %q", my.ProviderType(), my.GetProvider().ProviderName())
	}

	// no per-instance dispatch: neither the port nor a stored provider_type changes the engine
	for _, port := range []int{3306, 5432} {
		key := InstanceKey{Hostname: "db.example.com", Port: port}
		if got := pg.detectProviderType(&key); got != "postgresql" {
			t.Errorf("PostgreSQL store: port %d dispatched to %q", port, got)
		}
		if got := my.detectProviderType(&key); got != "mysql" {
			t.Errorf("MySQL store: port %d dispatched to %q", port, got)
		}
	}
	key := InstanceKey{Hostname: "stored.example.com", Port: 3306}
	stored := testInstance(key)
	stored.ProviderType = "postgresql"
	if err := pg.WriteInstance(stored, true, nil); err != nil {
		t.Fatalf("WriteInstance: %v", err)
	}
	stored.ProviderType = "mysql"
	if err := my.WriteInstance(stored, true, nil); err != nil {
		t.Fatalf("WriteInstance: %v", err)
	}
	if got := my.detectProviderType(&key); got != "mysql" {
		t.Errorf("MySQL store follows a stored provider_type: %q", got)
	}

	// the engine decides the analysis path without probing the backend for the other engine
	if !pg.analyzesPostgreSQLOnly("") {
		t.Errorf("a PostgreSQL store must run the PostgreSQL analysis only")
	}
	if my.analyzesPostgreSQLOnly("") {
		t.Errorf("a MySQL store must not run the PostgreSQL analysis")
	}

	// an unknown engine is rejected
	cfg := my.Config().Clone()
	cfg.ProviderType = "oracle"
	if _, err := NewStore("engine-bad", cfg, my.Backend()); err == nil {
		t.Errorf("NewStore accepted an unknown provider type")
	}
}

func TestLegacyStoreKeepsMixedEngines(t *testing.T) {
	cfg := config.Config.Clone()
	cfg.BackendDB = "sqlite3"
	cfg.SQLite3DataFile = filepath.Join(t.TempDir(), "legacy.sqlite3")
	cfg.ProviderType = config.ProviderMySQL
	backend, err := db.NewBackend("legacy-like", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backend.Close() }()
	if err := backend.EnsureSchema(); err != nil {
		t.Fatal(err)
	}
	s, err := NewStore("legacy-like", cfg, backend, withLegacyMixedProviders())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if got := s.detectProviderType(&InstanceKey{Hostname: "h", Port: 5432}); got != "postgresql" {
		t.Errorf("legacy port heuristic lost: %q", got)
	}
	if got := s.detectProviderType(&InstanceKey{Hostname: "h", Port: 3306}); got != "mysql" {
		t.Errorf("legacy default engine: %q", got)
	}
	key := InstanceKey{Hostname: "pg", Port: 3306}
	stored := testInstance(key)
	stored.ProviderType = "postgresql"
	if err := s.WriteInstance(stored, true, nil); err != nil {
		t.Fatal(err)
	}
	if got := s.detectProviderType(&key); got != "postgresql" {
		t.Errorf("legacy stored provider_type lost: %q", got)
	}
}

func TestDefaultStoreFollowsTheGlobalConfiguration(t *testing.T) {
	d := DefaultStore()
	if d != DefaultStore() {
		t.Fatalf("DefaultStore is not a singleton")
	}
	if d.ID() != config.DefaultTenantID || d.Config() != config.Config {
		t.Errorf("DefaultStore is not driven by config.Config")
	}
	previous := config.Config.DefaultInstancePort
	defer func() { config.Config.DefaultInstancePort = previous }()
	config.Config.DefaultInstancePort = 4406
	key, err := ParseRawInstanceKey("legacy.example.com")
	if err != nil || key.Port != 4406 {
		t.Errorf("package-level ParseInstanceKey does not follow config.Config: %+v %v", key, err)
	}
}
