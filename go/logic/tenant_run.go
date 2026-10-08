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
	"sync/atomic"
	"time"

	"github.com/proxysql/golib/log"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
	orcraft "github.com/proxysql/orchestrator/go/raft"
)

const (
	caretakingInterval     = time.Minute
	raftCaretakingInterval = 10 * time.Minute
	dbMetricsInterval      = 30 * time.Second
)

// ticker is a time.Ticker that may be absent (interval <= 0 never ticks, as the
// nil channel the former time.Tick returned).
type ticker struct{ t *time.Ticker }

func newTicker(d time.Duration) ticker {
	if d <= 0 {
		return ticker{}
	}
	return ticker{time.NewTicker(d)}
}

func (tk ticker) C() <-chan time.Time {
	if tk.t == nil {
		return nil
	}
	return tk.t.C
}

func (tk ticker) stop() {
	if tk.t != nil {
		tk.t.Stop()
	}
}

// isLeader and isLeaderOrActive are the leadership of the node: global to the
// process, there is a single leader for every tenant.
func (t *Tenant) isLeader() bool         { return t.leader.IsLeader() }
func (t *Tenant) isLeaderOrActive() bool { return t.leader.IsLeaderOrActive() }

// observeOp tells the test observer, if any, that a leader-only operation is being launched.
func (t *Tenant) observeOp(op string) {
	if t.opObserver != nil {
		t.opObserver(op)
	}
}

// spawn runs f in a goroutine that Stop waits for. Nothing is started once the
// tenant is stopping.
func (t *Tenant) spawn(ctx context.Context, f func()) {
	if ctx.Err() != nil {
		return
	}
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		f()
	}()
}

// Run drives the periodic work of the tenant -- instance polling and discovery,
// recoveries, the maintenance of its backend -- until ctx is done or Stop is
// called, and returns once everything it started has finished. It is called
// once; a second concurrent call returns immediately.
//
// The leadership is the global one of the node: every operation that only the
// leader may run is run for this tenant exactly when it is run for the others.
func (t *Tenant) Run(ctx context.Context) {
	t.lifecycleMu.Lock()
	if t.running || t.runDone != nil {
		t.lifecycleMu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	t.running = true
	t.cancelRun = cancel
	t.runDone = make(chan struct{})
	done := t.runDone
	t.lifecycleMu.Unlock()
	defer func() {
		cancel()
		t.wg.Wait()
		t.lifecycleMu.Lock()
		t.running = false
		t.lifecycleMu.Unlock()
		close(done)
	}()

	log.Infof("tenant %s: continuous discovery: setting up (provider=%s)", t.ID, t.Provider)
	continuousDiscoveryStartTime := time.Now()
	checkAndRecoverWaitPeriod := 3 * t.instancePollSecondsDuration()
	runCheckAndRecoverOperationsTimeRipe := func() bool {
		return time.Since(continuousDiscoveryStartTime) >= checkAndRecoverWaitPeriod
	}

	_ = t.Store.LoadHostnameResolveCache()
	t.handleDiscoveryRequests(runCtx)

	healthTick := newTicker(config.HealthPollSeconds * time.Second)
	instancePollTick := newTicker(t.instancePollSecondsDuration())
	caretakingTick := newTicker(caretakingInterval)
	raftCaretakingTick := newTicker(raftCaretakingInterval)
	recoveryTick := newTicker(time.Duration(config.RecoveryPollSeconds) * time.Second)
	autoPseudoGTIDTick := newTicker(time.Duration(config.PseudoGTIDIntervalSeconds) * time.Second)
	dbMetricsTick := newTicker(dbMetricsInterval)
	snapshotTopologiesTick := newTicker(time.Duration(t.Cfg.SnapshotTopologiesIntervalHours) * time.Hour)
	for _, tk := range []ticker{healthTick, instancePollTick, caretakingTick, raftCaretakingTick, recoveryTick, autoPseudoGTIDTick, dbMetricsTick, snapshotTopologiesTick} {
		defer tk.stop()
	}

	log.Infof("tenant %s: continuous discovery: starting", t.ID)
	for {
		select {
		case <-runCtx.Done():
			return
		case <-dbMetricsTick.C():
			t.spawn(runCtx, t.onDBMetricsTick)
		case <-healthTick.C():
			t.spawn(runCtx, t.onHealthTick)
		case <-instancePollTick.C():
			t.spawn(runCtx, func() { t.onInstancePollTick(runCtx) })
		case <-autoPseudoGTIDTick.C():
			t.spawn(runCtx, func() { t.onAutoPseudoGTIDTick(runCtx) })
		case <-caretakingTick.C():
			t.spawn(runCtx, func() { t.onCaretakingTick(runCtx, runCheckAndRecoverOperationsTimeRipe()) })
		case <-raftCaretakingTick.C():
			t.spawn(runCtx, func() { t.onRaftCaretakingTick(runCtx) })
		case <-recoveryTick.C():
			t.spawn(runCtx, func() { t.onRecoveryTick(runCtx, runCheckAndRecoverOperationsTimeRipe) })
		case <-snapshotTopologiesTick.C():
			t.spawn(runCtx, func() { t.onSnapshotTopologiesTick(runCtx) })
		}
	}
}

// launch runs op in its own goroutine, as the tick handlers always did for each
// of their independent maintenance operations.
func (t *Tenant) launch(ctx context.Context, name string, op func()) {
	t.observeOp(name)
	t.spawn(ctx, op)
}

func (t *Tenant) onDBMetricsTick() {
	if instances, err := t.Store.ReadDowntimedInstances(""); err == nil {
		t.Metrics.DowntimedInstances.Set(float64(len(instances)))
	}
	if recoveries, err := t.ReadActiveRecoveries(); err == nil {
		t.Metrics.ActiveRecoveries.Set(float64(len(recoveries)))
	}
	// Topology issue gauges: reuse discovery analysis. Cost is similar to a
	// recovery tick; failures are ignored so metrics never block the loop.
	if analysis, err := t.Store.GetReplicationAnalysis("", &inst.ReplicationAnalysisHints{}); err == nil {
		issuesCount := make(map[string]int)
		for _, a := range analysis {
			issuesCount[string(a.Analysis)]++
		}
		// only this tenant's series: another tenant's are not ours to drop
		t.Metrics.ResetActiveTopologyIssues()
		for issueType, count := range issuesCount {
			t.Metrics.ActiveTopologyIssues.WithLabelValues(issueType).Set(float64(count))
		}
	}
	if clusters, err := t.Store.ReadClustersInfo(""); err == nil {
		t.Metrics.ClustersTotal.Set(float64(len(clusters)))
		var instancesTotal uint
		for _, c := range clusters {
			instancesTotal += c.CountInstances
		}
		t.Metrics.InstancesTotal.Set(float64(instancesTotal))
	}
}

// onHealthTick is the tenant part of the health tick: it queues for discovery
// the instances of this tenant that are outdated. The node part (election, Raft
// and backend health) is the TenantManager's.
func (t *Tenant) onHealthTick() {
	wasAlreadyElected := t.wasLeader.Swap(t.isLeader())
	if !t.isLeaderOrActive() {
		return
	}
	instanceKeys, err := t.Store.ReadOutdatedInstanceKeys()
	if err != nil {
		_ = log.Errore(err)
	}

	if !wasAlreadyElected {
		// Just turned to be leader!
		go func() { _ = t.expireMaintenance() }()
	}

	func() {
		// Normally onHealthTick() shouldn't run concurrently. It is kicked by a ticker.
		// However it _is_ invoked inside a goroutine. I like to be safe here.
		t.snapshotDiscoveryKeysMutex.Lock()
		defer t.snapshotDiscoveryKeysMutex.Unlock()

		countSnapshotKeys := len(t.snapshotDiscoveryKeys)
		for i := 0; i < countSnapshotKeys; i++ {
			instanceKeys = append(instanceKeys, <-t.snapshotDiscoveryKeys)
		}
	}()
	// avoid any logging unless there's something to be done
	for _, instanceKey := range instanceKeys {
		if instanceKey.IsValid() {
			dead, recheck := t.Store.DeadInstancesFilter.InstanceRecheckNeeded(&instanceKey)
			if dead {
				if recheck {
					// this is a dead instance that needs recheck
					t.deadInstancesDiscoveryQueue.Push(instanceKey)
				}
				// If the instance is dead, but it is not a time to recheck it
				// it will be filtered out here.
				continue
			}
			// this is a healthy instance that needs recheck
			t.discoveryQueue.Push(instanceKey)
		}
	}
}

// onInstancePollTick runs the routine operations that need to be as (or roughly
// as) frequent as instance poll. It does NOT poll instances: that is what the
// discovery workers do, from the queue.
func (t *Tenant) onInstancePollTick(ctx context.Context) {
	if !t.isLeaderOrActive() {
		return
	}
	t.launch(ctx, "update-cluster-aliases", func() { _ = t.Store.UpdateClusterAliases() })
	t.launch(ctx, "expire-downtime", func() { _ = t.Store.ExpireDowntime() })
	t.launch(ctx, "inject-seeds", t.injectSeeds)
}

func (t *Tenant) onAutoPseudoGTIDTick(ctx context.Context) {
	if t.Cfg.AutoPseudoGTID && t.isLeader() {
		t.launch(ctx, "inject-pseudo-gtid", func() { _ = t.InjectPseudoGTIDOnWriters() })
	}
}

// onCaretakingTick runs the various periodic internal maintenance tasks of the tenant.
func (t *Tenant) onCaretakingTick(ctx context.Context, checkAndRecoverTimeRipe bool) {
	if !t.isLeaderOrActive() {
		// Take this opportunity to refresh yourself
		t.launch(ctx, "load-hostname-resolve-cache", func() { _ = t.Store.LoadHostnameResolveCache() })
		return
	}
	for _, op := range []struct {
		name string
		f    func() error
	}{
		{"record-instance-coordinates-history", t.Store.RecordInstanceCoordinatesHistory},
		{"review-unseen-instances", t.Store.ReviewUnseenInstances},
		{"inject-unseen-masters", t.Store.InjectUnseenMasters},
		{"forget-long-unseen-instances", t.Store.ForgetLongUnseenInstances},
		{"forget-long-unseen-cluster-aliases", t.Store.ForgetLongUnseenClusterAliases},
		{"forget-unseen-instances-differently-resolved", t.Store.ForgetUnseenInstancesDifferentlyResolved},
		{"forget-expired-hostname-resolves", t.Store.ForgetExpiredHostnameResolves},
		{"delete-invalid-hostname-resolves", t.Store.DeleteInvalidHostnameResolves},
		{"resolve-unknown-master-hostname-resolves", t.Store.ResolveUnknownMasterHostnameResolves},
		{"expire-maintenance", t.expireMaintenance},
		{"expire-candidate-instances", t.Store.ExpireCandidateInstances},
		{"expire-hostname-unresolve", t.Store.ExpireHostnameUnresolve},
		{"expire-cluster-domain-name", t.Store.ExpireClusterDomainName},
		{"expire-audit", t.Store.ExpireAudit},
		{"expire-master-position-equivalence", t.Store.ExpireMasterPositionEquivalence},
		{"expire-pool-instances", t.Store.ExpirePoolInstances},
		{"flush-nontrivial-resolve-cache-to-database", t.Store.FlushNontrivialResolveCacheToDatabase},
		{"expire-injected-pseudo-gtid", t.Store.ExpireInjectedPseudoGTID},
		{"expire-stale-instance-binlog-coordinates", t.Store.ExpireStaleInstanceBinlogCoordinates},
		{"expire-failure-detection-history", t.ExpireFailureDetectionHistory},
		{"expire-topology-recovery-history", t.ExpireTopologyRecoveryHistory},
		{"expire-topology-recovery-steps-history", t.ExpireTopologyRecoveryStepsHistory},
	} {
		op := op
		t.launch(ctx, op.name, func() { _ = op.f() })
	}
	if checkAndRecoverTimeRipe && t.isLeader() {
		t.launch(ctx, "submit-masters-to-kv-stores", func() { _, _, _ = t.SubmitMastersToKvStores("", false) })
	}
}

func (t *Tenant) onRaftCaretakingTick(ctx context.Context) {
	if orcraft.IsRaftEnabled() && t.isLeader() {
		t.launch(ctx, "publish-discover-masters", func() { _ = t.publishDiscoverMasters() })
	}
}

func (t *Tenant) onRecoveryTick(ctx context.Context, timeRipe func() bool) {
	if !t.isLeaderOrActive() {
		return
	}
	t.launch(ctx, "clear-active-failure-detections", func() { _ = t.ClearActiveFailureDetections() })
	t.launch(ctx, "clear-active-recoveries", func() { _ = t.ClearActiveRecoveries() })
	t.launch(ctx, "expire-blocked-recoveries", func() { _ = t.ExpireBlockedRecoveries() })
	t.launch(ctx, "acknowledge-crashed-recoveries", func() { _, _ = t.acknowledgeCrashedRecoveries() })
	t.launch(ctx, "expire-instance-analysis-changelog", func() { _ = t.Store.ExpireInstanceAnalysisChangelog() })

	t.launch(ctx, "check-and-recover", func() {
		// This function is non re-entrant (it can only be running once at any point in time)
		if atomic.CompareAndSwapInt64(&t.recoveryEntrance, 0, 1) {
			defer atomic.StoreInt64(&t.recoveryEntrance, 0)
		} else {
			return
		}
		if timeRipe() {
			_, _, _ = t.CheckAndRecover(nil, nil, false)
		} else {
			log.Debugf("tenant %s: waiting before running failure detection/recovery", t.ID)
		}
	})
}

func (t *Tenant) onSnapshotTopologiesTick(ctx context.Context) {
	if t.isLeaderOrActive() {
		t.launch(ctx, "snapshot-topologies", func() { _ = t.Store.SnapshotTopologies() })
	}
}

// mirrorFreshness is how recent the heartbeat of this node in a tenant backend
// must be for the operations that rely on node_health. Rows older than
// 5*HealthPollSeconds are purged, so this stays under that.
var mirrorFreshness = 3 * config.HealthPollSeconds * time.Second

// nodeHealthMirrored tells whether this node's heartbeat in the backend of the
// tenant (its node_health mirror) is recent. ExpireMaintenance and
// AcknowledgeCrashedRecoveries treat whatever is owned by a node that is NOT in
// node_health as owned by a dead node and delete / acknowledge it: with a
// lagging or failing mirror that would wipe the live maintenances and recoveries
// of this very node, so they only run while the mirror is known to be fresh.
func (t *Tenant) nodeHealthMirrored() bool {
	if t.control == nil {
		return true // a tenant outside a TenantManager has no mirror to wait for
	}
	for _, h := range t.control.TenantsHealth() {
		if h.TenantID == t.ID {
			return h.Healthy && !h.LastGood.IsZero() && time.Since(h.LastGood) <= mirrorFreshness
		}
	}
	return false
}

// expireMaintenance is Store.ExpireMaintenance, deferred while the node_health
// mirror of the tenant is not fresh (see nodeHealthMirrored).
func (t *Tenant) expireMaintenance() error {
	if !t.nodeHealthMirrored() {
		log.Warningf("tenant %s: not expiring maintenances: this node's heartbeat in the tenant backend is not recent", t.ID)
		return nil
	}
	return t.Store.ExpireMaintenance()
}

// acknowledgeCrashedRecoveries is AcknowledgeCrashedRecoveries, deferred while
// the node_health mirror of the tenant is not fresh (see nodeHealthMirrored).
func (t *Tenant) acknowledgeCrashedRecoveries() (int64, error) {
	if !t.nodeHealthMirrored() {
		log.Debugf("tenant %s: not acknowledging crashed recoveries: this node's heartbeat in the tenant backend is not recent", t.ID)
		return 0, nil
	}
	return t.AcknowledgeCrashedRecoveries()
}

// handleDiscoveryRequests starts the pools of workers that take instance keys
// off the discovery queues of the tenant and discover them, until ctx is done.
func (t *Tenant) handleDiscoveryRequests(ctx context.Context) {
	t.startDiscoveryWorkers(ctx, t.discoveryQueue, t.Cfg.DiscoveryMaxConcurrency)
	if t.deadInstancesDiscoveryQueue != t.discoveryQueue {
		t.startDiscoveryWorkers(ctx, t.deadInstancesDiscoveryQueue, t.Cfg.DeadInstanceDiscoveryMaxConcurrency)
	}
}

func (t *Tenant) startDiscoveryWorkers(ctx context.Context, queue interface {
	ConsumeContext(context.Context) (inst.InstanceKey, bool)
	Release(inst.InstanceKey)
	QueueLen() int
}, workers uint) {
	for i := uint(0); i < workers; i++ {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			for {
				instanceKey, ok := queue.ConsumeContext(ctx)
				if !ok {
					return
				}
				// Possibly this used to be the elected node, but has
				// been demoted, while still the queue is full.
				if !t.isLeaderOrActive() {
					log.Debugf("Node apparently demoted. Skipping discovery of %+v. "+
						"Remaining queue size: %+v", instanceKey, queue.QueueLen())
					queue.Release(instanceKey)
					continue
				}

				t.DiscoverInstance(instanceKey)
				queue.Release(instanceKey)
			}
		}()
	}
}

// injectSeeds discovers the configured seeds, once in the lifetime of the tenant.
func (t *Tenant) injectSeeds() {
	t.seedOnce.Do(func() {
		for _, seed := range t.Cfg.DiscoverySeeds {
			instanceKey, err := t.Store.ParseRawInstanceKey(seed)
			if err == nil {
				_ = t.Store.InjectSeed(instanceKey)
			} else {
				_ = log.Errorf("Error parsing seed %s: %+v", seed, err)
			}
		}
	})
}
