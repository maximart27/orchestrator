package kv

import (
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
)

// fakeBackend is an in-memory stand-in for a tenant's relational backend: it understands
// the two statements the internal KV store issues (replace into / select by key).
type fakeBackend struct {
	mu   sync.Mutex
	rows map[string]string
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{rows: map[string]string{}}
}

func (f *fakeBackend) Exec(query string, args ...interface{}) (sql.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[args[0].(string)] = args[1].(string)
	return nil, nil
}

func (f *fakeBackend) Query(query string, argsArray []interface{}, onRow func(sqlutils.RowMap) error) error {
	f.mu.Lock()
	value, found := f.rows[argsArray[0].(string)]
	f.mu.Unlock()
	if !found {
		return nil
	}
	return onRow(sqlutils.RowMap{"store_value": sqlutils.CellData{String: value, Valid: true}})
}

func TestStoresInternalBackendsAreIsolated(t *testing.T) {
	backendA, backendB := newFakeBackend(), newFakeBackend()
	a, err := NewStores(&config.Configuration{}, "tenant-a", WithBackend(backendA))
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewStores(&config.Configuration{}, "tenant-b", WithBackend(backendB))
	if err != nil {
		t.Fatal(err)
	}
	if a.TenantID() != "tenant-a" || b.TenantID() != "tenant-b" {
		t.Fatalf("unexpected tenant IDs %q %q", a.TenantID(), b.TenantID())
	}

	if err := a.PutKVPairs([]*KVPair{NewKVPair("mysql/master/c1", "10.0.0.5:3306")}); err != nil {
		t.Fatal(err)
	}
	if v, found, err := a.GetValue("mysql/master/c1"); err != nil || !found || v != "10.0.0.5:3306" {
		t.Fatalf("a: got %q found=%v err=%v", v, found, err)
	}
	if _, found, err := b.GetValue("mysql/master/c1"); err != nil || found {
		t.Fatalf("b must not see a's key: found=%v err=%v", found, err)
	}

	if err := b.PutValue("mysql/master/c1", "10.0.0.9:3306"); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := a.GetValue("mysql/master/c1"); v != "10.0.0.5:3306" {
		t.Fatalf("a's value was overwritten by b: %q", v)
	}
}

func TestStoresNilConfig(t *testing.T) {
	if _, err := NewStores(nil, "x"); err == nil {
		t.Fatal("expected an error for a nil configuration")
	}
}

func TestClusterMasterKeyUsesTenantPrefix(t *testing.T) {
	a, _ := NewStores(&config.Configuration{KVClusterMasterPrefix: "tenant-a/master/"}, "tenant-a")
	b, _ := NewStores(&config.Configuration{KVClusterMasterPrefix: "tenant-b/master/"}, "tenant-b")
	if got := a.ClusterMasterKey("c1"); got != "tenant-a/master/c1" {
		t.Errorf("a: got %q", got)
	}
	if got := b.ClusterMasterKey("c1"); got != "tenant-b/master/c1" {
		t.Errorf("b: got %q", got)
	}
	if got := ClusterMasterKey("mysql/master/", "c1"); got != "mysql/master/c1" {
		t.Errorf("got %q", got)
	}
}

// consulFake is a minimal Consul HTTP API that records KV writes and can hold the
// datacenters listing (the first step of DistributePairs) until released.
type consulFake struct {
	*httptest.Server
	mu           sync.Mutex
	puts         map[string]string
	dcCalls      int32
	dcEntered    chan struct{}
	dcRelease    chan struct{}
	blockFirstDC bool
}

func newConsulFake(blockFirstDC bool) *consulFake {
	f := &consulFake{
		puts:         map[string]string{},
		dcEntered:    make(chan struct{}, 8),
		dcRelease:    make(chan struct{}),
		blockFirstDC: blockFirstDC,
	}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/catalog/datacenters":
			if atomic.AddInt32(&f.dcCalls, 1) == 1 && f.blockFirstDC {
				f.dcEntered <- struct{}{}
				<-f.dcRelease
			}
			_, _ = io.WriteString(w, `["dc1"]`)
		case strings.HasPrefix(r.URL.Path, "/v1/kv/") && r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			f.mu.Lock()
			f.puts[strings.TrimPrefix(r.URL.Path, "/v1/kv/")] = string(body)
			f.mu.Unlock()
			_, _ = io.WriteString(w, "true")
		default: // KV get: not found
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return f
}

func (f *consulFake) config() *config.Configuration {
	return &config.Configuration{
		ConsulAddress:                     strings.TrimPrefix(f.URL, "http://"),
		ConsulScheme:                      "http",
		ConsulCrossDataCenterDistribution: true,
	}
}

func (f *consulFake) putValue(key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.puts[key]
	return v, ok
}

// TestStoresDistributionGuardIsPerInstance checks that a distribution in flight for one tenant
// neither blocks nor is skipped by another tenant's, while re-entry on the same instance is skipped.
func TestStoresDistributionGuardIsPerInstance(t *testing.T) {
	serverA, serverB := newConsulFake(true), newConsulFake(false)
	defer serverA.Close()
	defer serverB.Close()

	a, err := NewStores(serverA.config(), "tenant-a", WithBackend(newFakeBackend()))
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewStores(serverB.config(), "tenant-b", WithBackend(newFakeBackend()))
	if err != nil {
		t.Fatal(err)
	}

	aDone := make(chan error, 1)
	go func() { aDone <- a.DistributePairs([]*KVPair{NewKVPair("k", "from-a")}) }()
	select {
	case <-serverA.dcEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("a never started distributing")
	}

	// a is in flight: a second distribution on the same instance is skipped
	if err := a.DistributePairs([]*KVPair{NewKVPair("k", "second")}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&serverA.dcCalls); got != 1 {
		t.Fatalf("re-entrant distribution on a must be skipped, datacenter calls=%d", got)
	}

	// b is unaffected by a's in-flight distribution
	if err := b.DistributePairs([]*KVPair{NewKVPair("k", "from-b")}); err != nil {
		t.Fatal(err)
	}
	if v, ok := serverB.putValue("k"); !ok || v != "from-b" {
		t.Fatalf("b did not distribute: %q ok=%v", v, ok)
	}
	if _, ok := serverA.putValue("k"); ok {
		t.Fatal("a wrote before being released")
	}

	close(serverA.dcRelease)
	select {
	case err := <-aDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a never finished distributing")
	}
	if v, ok := serverA.putValue("k"); !ok || v != "from-a" {
		t.Fatalf("a did not distribute its own value: %q ok=%v", v, ok)
	}
}

// TestStoresConsulTxnProvider checks that the provider is chosen per tenant config.
func TestStoresConsulTxnProvider(t *testing.T) {
	a, err := NewStores(&config.Configuration{ConsulKVStoreProvider: "consul-txn"}, "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewStores(&config.Configuration{}, "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.stores[2].(*consulTxnStore); !ok {
		t.Errorf("a: expected consulTxnStore, got %T", a.stores[2])
	}
	if _, ok := b.stores[2].(*consulStore); !ok {
		t.Errorf("b: expected consulStore, got %T", b.stores[2])
	}
}

// TestStoresDistributionWithoutConsulIsNoop used to dereference a nil Consul client.
func TestStoresDistributionWithoutConsulIsNoop(t *testing.T) {
	for _, provider := range []string{"", "consul-txn"} {
		s, err := NewStores(&config.Configuration{ConsulCrossDataCenterDistribution: true, ConsulKVStoreProvider: provider}, "x")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.DistributePairs([]*KVPair{NewKVPair("k", "v")}); err != nil {
			t.Fatalf("provider %q: %v", provider, err)
		}
	}
}

// *db.Backend is what a tenant hands to WithBackend: keep the two in sync at compile time.
var _ Backend = (*db.Backend)(nil)
