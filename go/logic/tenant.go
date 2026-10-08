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

package logic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/proxysql/golib/log"
	"github.com/rcrowley/go-metrics"

	"github.com/proxysql/orchestrator/go/attributes"
	"github.com/proxysql/orchestrator/go/collection"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
	"github.com/proxysql/orchestrator/go/discovery"
	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/kv"
	ometrics "github.com/proxysql/orchestrator/go/metrics"
	"github.com/proxysql/orchestrator/go/proxysql"
)

const (
	discoveryMetricsName = "DISCOVERY_METRICS"

	// discoveryQueueName and deadInstancesQueueName are the names of the two
	// discovery queues of a tenant (also the legacy process-wide registry names).
	discoveryQueueName     = "DEFAULT"
	deadInstancesQueueName = "DEADINSTANCES"

	// snapshotDiscoveryKeysCapacity is the capacity of the channel through which a
	// raft snapshot restore hands instance keys to the discovery of its tenant.
	snapshotDiscoveryKeysCapacity = 10
)

// Tenant is the runtime of one tenant: its configuration, its provider (one
// database engine), its backend, and everything the discovery, analysis and
// recovery logic keeps in memory for it. Two Tenants never share mutable state,
// so the same host:port or cluster name can live in two tenants without
// interfering. Raft, the leadership and the node health are global to the
// process and live in the TenantManager.
//
// The operations of the package (recoveries, discovery, KV, snapshots...) are
// methods of Tenant; the package-level functions that predate it (deprecated,
// *_compat.go) run against DefaultTenant().
//
// A Tenant is created by a TenantManager (or newTenant in tests). Run drives its
// periodic work until the context is done; Stop stops it and releases what it owns.
type Tenant struct {
	// countPendingRecoveries is accessed atomically; it stays first so that it is
	// 64-bit aligned on every platform.
	countPendingRecoveries int64
	// recoveryEntrance is the CAS guard that keeps CheckAndRecover non re-entrant.
	recoveryEntrance int64

	// ID is the tenant ID; config.DefaultTenantID in the legacy single-tenant setup.
	ID string
	// Cfg is the effective configuration of the tenant. Read-only: it is the global
	// config.Config itself only for the legacy default tenant.
	Cfg *config.Configuration
	// Provider is the database engine of the tenant: config.ProviderMySQL or
	// config.ProviderPostgreSQL.
	Provider string

	Store      *inst.Store
	Backend    *db.Backend
	Attributes *attributes.Store
	KV         *kv.Stores
	ProxySQL   *proxysql.Hook
	// Metrics are the Prometheus series of the tenant (label tenant=<ID>).
	Metrics *ometrics.TenantMetrics

	// shared is true for the legacy default tenant, which uses the process-wide
	// defaults (db.DefaultBackend(), inst.DefaultStore(), the registered discovery
	// queues and collections...) so that the pre-multi-tenant API keeps seeing the
	// very same state. A shared tenant releases none of them.
	shared bool
	// ownsBackend tells whether Stop closes Backend (the manager owns it otherwise).
	ownsBackend bool

	leader Leadership

	// discovery
	discoveryQueue              *discovery.Queue
	deadInstancesDiscoveryQueue *discovery.Queue
	// snapshotDiscoveryKeys carries instance keys from a raft snapshot restore to
	// the health tick of the tenant.
	snapshotDiscoveryKeys        chan inst.InstanceKey
	snapshotDiscoveryKeysMutex   sync.Mutex
	recentDiscoveryOperationKeys *cache.Cache
	discoveryMetrics             *collection.Collection
	seedOnce                     sync.Once
	pseudoGTIDPublishCache       *cache.Cache
	kvFoundCache                 *cache.Cache

	// emergency operations of the recovery logic
	emergencyReadTopologyInstanceMap           *cache.Cache
	emergencyRestartReplicaTopologyInstanceMap *cache.Cache
	emergencyOperationGracefulPeriodMap        *cache.Cache

	// go-metrics, registered under the tenant's names
	discoveriesCounter                              metrics.Counter
	failedDiscoveriesCounter                        metrics.Counter
	instancePollSecondsExceededCounter              metrics.Counter
	discoveryQueueLengthGauge                       metrics.Gauge
	discoveryRecentCountGauge                       metrics.Gauge
	deadInstancesDiscoveryQueueLengthGauge          metrics.Gauge
	countPendingRecoveriesGauge                     metrics.Gauge
	recoverDeadMasterCounter                        metrics.Counter
	recoverDeadMasterSuccessCounter                 metrics.Counter
	recoverDeadMasterFailureCounter                 metrics.Counter
	recoverDeadIntermediateMasterCounter            metrics.Counter
	recoverDeadIntermediateMasterSuccessCounter     metrics.Counter
	recoverDeadIntermediateMasterFailureCounter     metrics.Counter
	recoverDeadCoMasterCounter                      metrics.Counter
	recoverDeadCoMasterSuccessCounter               metrics.Counter
	recoverDeadCoMasterFailureCounter               metrics.Counter
	recoverDeadReplicationGroupMemberCounter        metrics.Counter
	recoverDeadReplicationGroupMemberSuccessCounter metrics.Counter
	recoverDeadReplicationGroupMemberFailureCounter metrics.Counter

	// seams of the recovery audit/persistence, overridable in unit tests
	auditTopologyRecoveryFn   func(topologyRecovery *TopologyRecovery, message string) error
	persistResolvedRecoveryFn func(topologyRecovery *TopologyRecovery) error

	// opObserver, when set (tests), is told the name of every leader-only
	// operation the tick loops of this tenant launch.
	opObserver func(op string)

	// wasLeader remembers whether the node was the leader at the previous health
	// tick of this tenant, to run the "just became leader" work once.
	wasLeader atomic.Bool

	unregisterMetricsTicks []func()

	// quit is closed when the tenant is released: it ends what would otherwise wait forever.
	quit     chan struct{}
	quitOnce sync.Once

	lifecycleMu sync.Mutex
	running     bool
	cancelRun   context.CancelFunc
	runDone     chan struct{}
	stopOnce    sync.Once
	// wg tracks the goroutines the tick loops launch, so that Stop waits for them.
	wg sync.WaitGroup
}

// tenantOptions tunes newTenant.
type tenantOptions struct {
	leader Leadership
	// shared builds the legacy default tenant on the process-wide defaults.
	shared bool
	// backend is the already created backend of the tenant; nil means newTenant
	// creates (and owns) one from cfg.
	backend *db.Backend
}

// newTenant builds the runtime of tenant tc. It does not start anything; call Run.
// On error nothing is left open.
func newTenant(tc *config.TenantConfig, opts tenantOptions) (t *Tenant, err error) {
	if tc == nil || tc.Cfg == nil {
		return nil, errors.New("nil tenant configuration")
	}
	provider, err := config.NormalizeProvider(tc.Provider)
	if err != nil {
		return nil, err
	}
	t = &Tenant{
		ID:       tc.ID,
		Cfg:      tc.Cfg,
		Provider: provider,
		shared:   opts.shared,
		leader:   opts.leader,
	}
	if t.leader == nil {
		t.leader = processLeadership{}
	}
	defer func() {
		if err != nil {
			t.release()
			t = nil
		}
	}()

	if opts.shared {
		t.Backend = db.DefaultBackend()
		t.Store = inst.DefaultStore()
		t.Attributes = attributes.DefaultStore()
		kv.InitKVStores()
		t.KV = kv.DefaultStores()
		proxysql.InitHook()
		t.ProxySQL = proxysql.GetHook()
	} else {
		t.Backend = opts.backend
		if t.Backend == nil {
			if t.Backend, err = db.NewBackend(t.ID, t.Cfg); err != nil {
				return nil, err
			}
			t.ownsBackend = true
			if err = t.Backend.EnsureSchema(); err != nil {
				return nil, fmt.Errorf("deploying the backend schema: %w", err)
			}
		}
		if t.Store, err = inst.NewStore(t.ID, t.Cfg, t.Backend, inst.WithProviderType(provider)); err != nil {
			return nil, err
		}
		t.Attributes = attributes.NewStore(t.Backend)
		if t.KV, err = kv.NewStores(t.Cfg, t.ID, kv.WithBackend(t.Backend)); err != nil {
			return nil, err
		}
		t.ProxySQL = proxysql.NewHookFromConfig(t.Cfg)
	}
	t.Metrics = ometrics.ForTenant(t.ID)
	t.initState()
	t.registerMetricsTicks()
	return t, nil
}

// initState creates the in-memory state of the tenant (what used to be package
// level variables and init() functions).
func (t *Tenant) initState() {
	t.quit = make(chan struct{})
	t.snapshotDiscoveryKeys = make(chan inst.InstanceKey, snapshotDiscoveryKeysCapacity)
	t.recentDiscoveryOperationKeys = cache.New(t.instancePollSecondsDuration(), time.Second)
	t.pseudoGTIDPublishCache = cache.New(time.Minute, time.Second)
	t.kvFoundCache = cache.New(10*time.Minute, time.Minute)

	t.emergencyReadTopologyInstanceMap = cache.New(time.Second, time.Millisecond*250)
	t.emergencyRestartReplicaTopologyInstanceMap = cache.New(time.Second*30, time.Second)
	t.emergencyOperationGracefulPeriodMap = cache.New(time.Second*5, time.Millisecond*500)

	retention := time.Duration(t.Cfg.DiscoveryCollectionRetentionSeconds) * time.Second
	if t.shared {
		t.discoveryMetrics = collection.CreateOrReturnCollection(discoveryMetricsName)
		t.discoveryQueue = discovery.CreateOrReturnQueue(discoveryQueueName)
	} else {
		t.discoveryMetrics = collection.New(retention)
		t.discoveryQueue = discovery.NewQueueFromConfig(discoveryQueueName, t.Cfg)
	}
	if t.Cfg.DeadInstanceDiscoveryMaxConcurrency > 0 {
		if t.shared {
			t.deadInstancesDiscoveryQueue = discovery.CreateOrReturnQueue(deadInstancesQueueName)
		} else {
			t.deadInstancesDiscoveryQueue = discovery.NewQueueFromConfig(deadInstancesQueueName, t.Cfg)
		}
	} else {
		t.deadInstancesDiscoveryQueue = t.discoveryQueue
	}

	t.discoveriesCounter = t.counter("discoveries.attempt")
	t.failedDiscoveriesCounter = t.counter("discoveries.fail")
	t.instancePollSecondsExceededCounter = t.counter("discoveries.instance_poll_seconds_exceeded")
	t.discoveryQueueLengthGauge = t.gauge("discoveries.queue_length")
	t.discoveryRecentCountGauge = t.gauge("discoveries.recent_count")
	t.deadInstancesDiscoveryQueueLengthGauge = metrics.NewGauge()
	if t.deadInstancesDiscoveryQueue != t.discoveryQueue {
		// registered only if the dead instances queue exists
		t.deadInstancesDiscoveryQueueLengthGauge = t.gauge("discoveries.dead_instances_queue_length")
	}

	t.recoverDeadMasterCounter = t.counter("recover.dead_master.start")
	t.recoverDeadMasterSuccessCounter = t.counter("recover.dead_master.success")
	t.recoverDeadMasterFailureCounter = t.counter("recover.dead_master.fail")
	t.recoverDeadIntermediateMasterCounter = t.counter("recover.dead_intermediate_master.start")
	t.recoverDeadIntermediateMasterSuccessCounter = t.counter("recover.dead_intermediate_master.success")
	t.recoverDeadIntermediateMasterFailureCounter = t.counter("recover.dead_intermediate_master.fail")
	t.recoverDeadCoMasterCounter = t.counter("recover.dead_co_master.start")
	t.recoverDeadCoMasterSuccessCounter = t.counter("recover.dead_co_master.success")
	t.recoverDeadCoMasterFailureCounter = t.counter("recover.dead_co_master.fail")
	t.recoverDeadReplicationGroupMemberCounter = t.counter("recover.dead_replication_group_member.start")
	t.recoverDeadReplicationGroupMemberSuccessCounter = t.counter("recover.dead_replication_group_member.success")
	t.recoverDeadReplicationGroupMemberFailureCounter = t.counter("recover.dead_replication_group_member.fail")
	t.countPendingRecoveriesGauge = t.gauge("recover.pending")
}

// counter returns the go-metrics counter of the tenant registered under name.
// The legacy default tenant keeps the historical bare names, which are shared by
// every Tenant built for it.
func (t *Tenant) counter(name string) metrics.Counter {
	return ometrics.GetOrRegisterTenantMetric(t.ID, name, metrics.NewCounter()).(metrics.Counter)
}

func (t *Tenant) gauge(name string) metrics.Gauge {
	return ometrics.GetOrRegisterTenantMetric(t.ID, name, metrics.NewGauge()).(metrics.Gauge)
}

// registerMetricsTicks registers the callbacks that refresh the gauges of the
// tenant on every metrics tick; release unregisters them.
func (t *Tenant) registerMetricsTicks() {
	reg := func(f func()) {
		t.unregisterMetricsTicks = append(t.unregisterMetricsTicks, ometrics.RegisterMetricsTick(f))
	}
	reg(func() {
		queueLen := t.discoveryQueue.QueueLen()
		t.discoveryQueueLengthGauge.Update(int64(queueLen))
		t.Metrics.DiscoveryQueueLength.Set(float64(queueLen))
	})
	reg(func() {
		t.discoveryRecentCountGauge.Update(int64(t.recentDiscoveryOperationKeys.ItemCount()))
	})
	reg(func() {
		t.countPendingRecoveriesGauge.Update(t.getCountPendingRecoveries())
	})
	if t.deadInstancesDiscoveryQueue != t.discoveryQueue {
		reg(func() {
			queueLen := t.deadInstancesDiscoveryQueue.QueueLen()
			t.deadInstancesDiscoveryQueueLengthGauge.Update(int64(queueLen))
			t.Metrics.DeadInstancesDiscoveryQueueLength.Set(float64(queueLen))
		})
	}
}

// DiscoveryQueue returns the queue of instances to discover of the tenant.
func (t *Tenant) DiscoveryQueue() *discovery.Queue { return t.discoveryQueue }

// DeadInstancesDiscoveryQueue returns the queue of presumably dead instances to
// rediscover; it is DiscoveryQueue when DeadInstanceDiscoveryMaxConcurrency is 0.
func (t *Tenant) DeadInstancesDiscoveryQueue() *discovery.Queue {
	return t.deadInstancesDiscoveryQueue
}

// DiscoveryMetrics returns the collection of discovery metrics of the tenant.
func (t *Tenant) DiscoveryMetrics() *collection.Collection { return t.discoveryMetrics }

// Stop stops the tenant: it ends Run (waiting for the work Run launched) and
// releases what the tenant owns: queues, collections, metrics callbacks, Store
// and, if it created it, Backend. It is idempotent. The tenant must not be used
// afterwards.
func (t *Tenant) Stop() {
	t.stopOnce.Do(func() {
		t.lifecycleMu.Lock()
		cancel, done := t.cancelRun, t.runDone
		t.lifecycleMu.Unlock()
		if cancel != nil {
			cancel()
			<-done
		}
		t.wg.Wait()
		t.release()
	})
}

// release closes what newTenant created and the tenant owns. A shared (legacy
// default) tenant owns none of the process-wide defaults it uses.
func (t *Tenant) release() {
	if t.quit != nil {
		t.quitOnce.Do(func() { close(t.quit) })
	}
	for _, unregister := range t.unregisterMetricsTicks {
		unregister()
	}
	t.unregisterMetricsTicks = nil
	if t.shared {
		return
	}
	if t.discoveryQueue != nil {
		t.discoveryQueue.Close()
	}
	if t.deadInstancesDiscoveryQueue != nil && t.deadInstancesDiscoveryQueue != t.discoveryQueue {
		t.deadInstancesDiscoveryQueue.Close()
	}
	if t.discoveryMetrics != nil {
		t.discoveryMetrics.Close()
	}
	if t.Store != nil {
		_ = t.Store.Close()
	}
	if t.ownsBackend && t.Backend != nil {
		if err := t.Backend.Close(); err != nil {
			_ = log.Errorf("tenant %s: closing the backend: %v", t.ID, err)
		}
	}
	if t.Metrics != nil && t.ID != config.DefaultTenantID {
		t.Metrics.Forget()
	}
	ometrics.UnregisterTenantMetrics(t.ID)
}
