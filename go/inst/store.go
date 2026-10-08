/*
   Copyright 2026 Orchestrator Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package inst

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/proxysql/golib/log"
	"github.com/rcrowley/go-metrics"
	"github.com/sjmudd/stopwatch"

	"github.com/proxysql/orchestrator/go/collection"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
)

// Store is everything the inst package keeps for ONE tenant: its configuration,
// its orchestrator backend, its database provider (a tenant runs a single
// engine) and every cache, buffer, queue and metric that used to be package
// level state. Two Stores never share mutable state, so the same host:port or
// cluster name can live in two tenants without interfering.
//
// The DAOs and the topology operations of the package are methods of Store.
// The package-level functions that predate it (deprecated, *_compat.go) run
// against DefaultStore().
//
// A Store is safe for concurrent use. Close it when the tenant is stopped; the
// Store does not own the db.Backend it was given.
type Store struct {
	id      string
	cfg     *config.Configuration
	backend *db.Backend

	// providerType is the engine of the tenant: config.ProviderMySQL or
	// config.ProviderPostgreSQL.
	providerType string
	// legacyMixedProviders is true only for DefaultStore(): the engine is then
	// decided per instance (database_instance.provider_type, port 5432
	// heuristic, configured ProviderType), as it always was.
	legacyMixedProviders bool

	providerMu sync.RWMutex
	provider   DatabaseProvider

	// DeadInstancesFilter holds the instances this tenant considers dead, to
	// back their rediscovery off.
	DeadInstancesFilter *deadInstancesFilter

	instanceReadChan        chan bool
	instanceWriteChan       chan bool
	topologyConcurrencyChan chan bool

	instanceWriteBuffer           chan instanceUpdateObject
	forceFlushInstanceWriteBuffer chan bool
	writeBufferLatency            *stopwatch.NamedStopwatch
	backendWrites                 *collection.Collection
	writeBufferMetrics            *collection.Collection

	// instanceKeyInformativeClusterName is a non-authoritative cache; used for auditing or general purpose.
	instanceKeyInformativeClusterName *cache.Cache
	forgetInstanceKeys                *cache.Cache
	clusterInjectedPseudoGTIDCache    *cache.Cache
	recentInstantAnalysis             *cache.Cache
	instanceBinlogEntryCache          *cache.Cache
	supportedAutoPseudoGTIDWriters    *cache.Cache
	hostnameIPsCache                  *cache.Cache

	hostnameResolvesLightweightCache                 *cache.Cache
	hostnameResolvesLightweightCacheInit             sync.Mutex
	hostnameResolvesLightweightCacheLoadedOnceFromDB bool

	accessDeniedCounter               metrics.Counter
	readTopologyInstanceCounter       metrics.Counter
	readInstanceCounter               metrics.Counter
	writeInstanceCounter              metrics.Counter
	analysisChangeWriteAttemptCounter metrics.Counter
	analysisChangeWriteCounter        metrics.Counter
	auditOperationCounter             metrics.Counter
	deadInstancesCounter              metrics.Counter
	writeResolvedHostnameCounter      metrics.Counter
	writeUnresolvedHostnameCounter    metrics.Counter
	readResolvedHostnameCounter       metrics.Counter
	readUnresolvedHostnameCounter     metrics.Counter
	readAllResolvedHostnamesCounter   metrics.Counter

	registeredMetrics map[string]interface{}

	closed      atomic.Bool
	stop        chan struct{}
	flusherDone chan struct{}
	closeOnce   sync.Once
}

// StoreOption tunes NewStore.
type StoreOption func(*Store)

// WithProviderType sets the engine of the tenant ("mysql", "mariadb",
// "postgresql" or "postgres"), overriding cfg.ProviderType.
func WithProviderType(name string) StoreOption {
	return func(s *Store) { s.providerType = name }
}

// withLegacyMixedProviders makes the Store dispatch the engine per instance
// instead of fixing it per tenant. Only DefaultStore() uses it.
func withLegacyMixedProviders() StoreOption {
	return func(s *Store) { s.legacyMixedProviders = true }
}

// NewStore builds the Store of tenant id. cfg is the tenant's effective
// configuration (it must not be mutated by the caller afterwards) and backend
// its orchestrator backend. The engine of the tenant is cfg.ProviderType unless
// WithProviderType says otherwise. The flusher goroutine of the instance write
// buffer starts here and stops in Close. No process-wide state is touched
// except the metrics registry, where the tenant's metrics are registered as
// "tenant.<id>.<name>".
func NewStore(id string, cfg *config.Configuration, backend *db.Backend, opts ...StoreOption) (*Store, error) {
	if id == "" {
		return nil, errors.New("inst: store id must not be empty")
	}
	if cfg == nil {
		return nil, fmt.Errorf("inst: store %q: nil configuration", id)
	}
	if backend == nil {
		return nil, fmt.Errorf("inst: store %q: nil backend", id)
	}
	s := &Store{id: id, cfg: cfg, backend: backend, providerType: cfg.ProviderType}
	for _, opt := range opts {
		opt(s)
	}
	if !s.legacyMixedProviders {
		providerType, err := config.NormalizeProvider(s.providerType)
		if err != nil {
			return nil, fmt.Errorf("inst: store %q: %w", id, err)
		}
		s.providerType = providerType
		if cfg.ExpiryHostnameResolvesMinutes < 1 {
			cfg.ExpiryHostnameResolvesMinutes = 1
		}
	} else if providerType, err := config.NormalizeProvider(s.providerType); err == nil {
		s.providerType = providerType
	} else {
		s.providerType = config.ProviderMySQL
	}
	s.build()
	return s, nil
}

func (s *Store) build() {
	s.stop = make(chan struct{})
	s.registeredMetrics = make(map[string]interface{})

	if s.providerType == config.ProviderPostgreSQL && !s.legacyMixedProviders {
		s.provider = NewPostgreSQLProvider(s.cfg)
	} else {
		mysqlProvider := NewMySQLProvider(s.cfg)
		mysqlProvider.store = s
		s.provider = mysqlProvider
	}

	s.instanceReadChan = make(chan bool, backendDBConcurrency)
	s.instanceWriteChan = make(chan bool, backendDBConcurrency)
	s.topologyConcurrencyChan = make(chan bool, topologyConcurrency)

	s.accessDeniedCounter = s.newCounter("instance.access_denied")
	s.readTopologyInstanceCounter = s.newCounter("instance.read_topology")
	s.readInstanceCounter = s.newCounter("instance.read")
	s.writeInstanceCounter = s.newCounter("instance.write")
	s.analysisChangeWriteAttemptCounter = s.newCounter("analysis.change.write.attempt")
	s.analysisChangeWriteCounter = s.newCounter("analysis.change.write")
	s.auditOperationCounter = s.newCounter("audit.write")
	s.deadInstancesCounter = s.newCounter("discoveries.dead_instances")
	s.writeResolvedHostnameCounter = s.newCounter("resolve.write_resolved")
	s.writeUnresolvedHostnameCounter = s.newCounter("resolve.write_unresolved")
	s.readResolvedHostnameCounter = s.newCounter("resolve.read_resolved")
	s.readUnresolvedHostnameCounter = s.newCounter("resolve.read_unresolved")
	s.readAllResolvedHostnamesCounter = s.newCounter("resolve.read_resolved_all")

	s.DeadInstancesFilter = newDeadInstancesFilter(s.cfg, s.deadInstancesCounter)

	s.backendWrites = s.newCollection("BACKEND_WRITES")
	s.writeBufferMetrics = s.newCollection("WRITE_BUFFER")
	s.writeBufferLatency = stopwatch.NewNamedStopwatch()
	_ = s.writeBufferLatency.AddMany([]string{"wait", "write"})
	s.writeBufferLatency.Start("wait")

	s.instanceWriteBuffer = make(chan instanceUpdateObject, s.cfg.InstanceWriteBufferSize)
	s.forceFlushInstanceWriteBuffer = make(chan bool)

	s.instanceKeyInformativeClusterName = cache.New(time.Duration(s.cfg.InstancePollSeconds/2)*time.Second, time.Second)
	s.forgetInstanceKeys = cache.New(time.Duration(s.cfg.InstancePollSeconds*3)*time.Second, time.Second)
	s.clusterInjectedPseudoGTIDCache = cache.New(time.Minute, time.Second)
	s.recentInstantAnalysis = cache.New(time.Duration(config.RecoveryPollSeconds*2)*time.Second, time.Second)
	s.instanceBinlogEntryCache = cache.New(time.Duration(10)*time.Minute, time.Minute)
	s.supportedAutoPseudoGTIDWriters = cache.New(config.CheckAutoPseudoGTIDGrantsIntervalSeconds*time.Second, time.Second)
	s.hostnameIPsCache = cache.New(10*time.Minute, time.Minute)

	s.startInstanceWriteBufferFlusher()
}

// startInstanceWriteBufferFlusher spins off the goroutine that flushes the
// instance write buffer; Close stops it.
func (s *Store) startInstanceWriteBufferFlusher() {
	s.flusherDone = make(chan struct{})
	var flushTick <-chan time.Time
	if interval := time.Duration(s.cfg.InstanceFlushIntervalMilliseconds) * time.Millisecond; interval > 0 {
		ticker := time.NewTicker(interval)
		flushTick = ticker.C
		go s.runInstanceWriteBufferFlusher(flushTick, ticker.Stop)
		return
	}
	go s.runInstanceWriteBufferFlusher(flushTick, func() {})
}

func (s *Store) runInstanceWriteBufferFlusher(flushTick <-chan time.Time, stopTicker func()) {
	defer close(s.flusherDone)
	defer stopTicker()
	for {
		select {
		case <-flushTick:
			s.flushInstanceWriteBuffer()
		case <-s.forceFlushInstanceWriteBuffer:
			s.flushInstanceWriteBuffer()
		case <-s.stop:
			return
		}
	}
}

// Close stops the Store: it flushes the buffered instance writes, stops the
// flusher and unregisters the tenant's metrics. It does not close the
// db.Backend, which belongs to whoever created it. Close is idempotent; the
// Store must not be used afterwards.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.stop)
		<-s.flusherDone
		s.flushInstanceWriteBuffer()
		for name, m := range s.registeredMetrics {
			if metrics.Get(name) == m {
				metrics.Unregister(name)
			}
		}
	})
	return nil
}

// ID returns the tenant id of the Store.
func (s *Store) ID() string { return s.id }

// Config returns the tenant configuration the Store was built from. Read-only.
func (s *Store) Config() *config.Configuration { return s.cfg }

// Backend returns the orchestrator backend of the tenant.
func (s *Store) Backend() *db.Backend { return s.backend }

// ProviderType returns the engine of the tenant: config.ProviderMySQL or
// config.ProviderPostgreSQL. For the legacy default store, which mixes engines
// per instance, it is the configured one.
func (s *Store) ProviderType() string { return s.providerType }

// SetProvider replaces the database provider of the Store. A tenant's provider
// comes from its configuration; this exists for the legacy default store and
// tests.
func (s *Store) SetProvider(p DatabaseProvider) {
	s.providerMu.Lock()
	defer s.providerMu.Unlock()
	s.provider = p
}

// GetProvider returns the database provider of the Store.
func (s *Store) GetProvider() DatabaseProvider {
	s.providerMu.RLock()
	defer s.providerMu.RUnlock()
	return s.provider
}

// BackendWrites returns the collection of backend write metrics of the tenant.
func (s *Store) BackendWrites() *collection.Collection { return s.backendWrites }

// WriteBufferMetrics returns the collection of write buffer flush metrics of the tenant.
func (s *Store) WriteBufferMetrics() *collection.Collection { return s.writeBufferMetrics }

// scopedName gives the tenant-qualified name of a metric or collection: the
// legacy default tenant keeps the historical bare names.
func (s *Store) scopedName(name string) string {
	if s.id == db.DefaultBackendID {
		return name
	}
	return "tenant." + s.id + "." + name
}

// newCounter creates a counter and registers it in the metrics registry. A
// metric already registered under that name (a previous Store of the same tenant)
// is replaced rather than silently left in place.
func (s *Store) newCounter(name string) metrics.Counter {
	c := metrics.NewCounter()
	s.registerMetric(name, c)
	return c
}

func (s *Store) registerMetric(name string, m interface{}) {
	full := s.scopedName(name)
	if metrics.Get(full) != nil {
		metrics.Unregister(full)
	}
	if err := metrics.Register(full, m); err != nil {
		_ = log.Errorf("inst: store %q: cannot register metric %s: %v", s.id, full, err)
		return
	}
	s.registeredMetrics[full] = m
}

func (s *Store) newCollection(name string) *collection.Collection {
	c := collection.CreateOrReturnCollection(s.scopedName(name))
	if s.id != db.DefaultBackendID {
		c.SetExpirePeriod(time.Duration(s.cfg.DiscoveryCollectionRetentionSeconds) * time.Second)
	}
	return c
}
