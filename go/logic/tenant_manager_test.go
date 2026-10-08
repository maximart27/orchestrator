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
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/kv"
	"github.com/proxysql/orchestrator/go/process"
)

func startAndStop(t *testing.T, ids []string) {
	t.Helper()
	base := testBase(t)
	m, err := NewTenantManagerFor(base, testTenantConfigs(t, base, ids, nil), WithoutProcessBootstrap(), WithLeadership(&testLeadership{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err == nil {
		t.Error("a second Start must fail")
	}
	time.Sleep(150 * time.Millisecond)
	m.Stop()
	m.Stop() // idempotent
	select {
	case <-m.Done():
	default:
		t.Error("Done must be closed once the manager is stopped")
	}
	if err := m.Start(context.Background()); err == nil {
		t.Error("a stopped manager must not start again")
	}
}

func TestManagerStartStopLeavesNoGoroutines(t *testing.T) {
	ids := []string{"alpha", "beta", "gamma"}
	// the named registries of other packages (collections, queues) keep what they
	// registered the first time a tenant ID is seen: warm them up
	startAndStop(t, ids)
	waitGoroutines(t, runtime.NumGoroutine())
	baseline := runtime.NumGoroutine()

	startAndStop(t, ids)
	waitGoroutines(t, baseline)
}

func TestManagerStopWithoutStartReleasesEverything(t *testing.T) {
	base := testBase(t)
	m, err := NewTenantManagerFor(base, testTenantConfigs(t, base, []string{"a", "b"}, nil), WithoutProcessBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	a := mustTenant(t, m, "a")
	m.Stop()
	m.Stop()
	if _, err := a.Backend.Exec("select 1"); err == nil {
		t.Error("the backend of a stopped tenant must be closed")
	}
}

func TestManagerAllIsSortedAndDefaultResolves(t *testing.T) {
	m, _ := newTestManager(t, []string{"zeta", "alpha", "mid"}, nil)
	var ids []string
	for _, tn := range m.All() {
		ids = append(ids, tn.ID)
	}
	if strings.Join(ids, ",") != "alpha,mid,zeta" {
		t.Errorf("All() = %v, want sorted by ID", ids)
	}
	if m.Default() != nil {
		t.Error("with several tenants and none named default there is no default tenant")
	}
	if _, ok := m.Get("nope"); ok {
		t.Error("Get of an unknown tenant must report not found")
	}

	m2, _ := newTestManager(t, []string{"only"}, nil)
	if d := m2.Default(); d == nil || d.ID != "only" {
		t.Errorf("the only tenant is the default one, got %v", d)
	}
	m3, _ := newTestManager(t, []string{"x", "default"}, nil)
	if d := m3.Default(); d == nil || d.ID != "default" {
		t.Errorf("the tenant named default is the default one, got %v", d)
	}
}

func TestManagerFailsFastNamingTheTenant(t *testing.T) {
	base := testBase(t)
	good := testTenantConfigs(t, base, []string{"good"}, nil)
	bad := testTenantConfigs(t, base, []string{"bad"}, nil)
	bad[0].Cfg.SQLite3DataFile = filepath.Join(t.TempDir(), "no-such-dir", "deeper", "bad.sqlite3")

	waitGoroutines(t, runtime.NumGoroutine())
	before := runtime.NumGoroutine()
	m, err := NewTenantManagerFor(base, append(good, bad...), WithoutProcessBootstrap())
	if err == nil {
		m.Stop()
		t.Fatal("a tenant that cannot be initialised must make NewTenantManager fail")
	}
	if m != nil {
		t.Error("no manager may be returned on error")
	}
	if !strings.Contains(err.Error(), `"bad"`) {
		t.Errorf("the error must name the tenant: %v", err)
	}
	// "good" had been built before "bad" failed: nothing may be left running
	waitGoroutines(t, before+2)
}

func TestManagerMultiTenantNeedsRaft(t *testing.T) {
	base := testBase(t)
	tenants := testTenantConfigs(t, base, []string{"a", "b"}, nil)
	if _, err := NewTenantManagerFor(base, tenants); err == nil || !strings.Contains(err.Error(), "Raft") {
		t.Errorf("two tenants without Raft must be rejected, got %v", err)
	}
	// a single tenant needs no Raft
	single := testTenantConfigs(t, base, []string{"a"}, nil)
	m, err := NewTenantManagerFor(base, single, WithoutProcessBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	m.Stop()
}

func TestControlBackendIsTheDefaultTenantsOrADedicatedOne(t *testing.T) {
	withDefault, _ := newTestManager(t, []string{"default", "other"}, nil)
	if got, want := withDefault.Control().Backend(), mustTenant(t, withDefault, "default").Backend; got != want {
		t.Error("the control backend must be the one of the default tenant")
	}
	ids := withDefault.Control().MirrorIDs()
	if strings.Join(ids, ",") != "default,other" {
		t.Errorf("every tenant must be a mirror of the control, got %v", ids)
	}

	without, _ := newTestManager(t, []string{"a", "b"}, nil)
	control := without.Control().Backend()
	for _, tn := range without.All() {
		if control == tn.Backend {
			t.Errorf("without a default tenant the control backend must be dedicated, not tenant %s's", tn.ID)
		}
	}
	if control.Config().SQLite3DataFile == "" || control.ID() != controlBackendID {
		t.Errorf("the dedicated control backend is built from the global backend settings, got %q (%s)", control.Config().SQLite3DataFile, control.ID())
	}

	// without a default tenant and without a usable global backend there is nothing to host the control
	base := testBase(t)
	base.SQLite3DataFile = ""
	if _, err := NewTenantManagerFor(base, testTenantConfigs(t, base, []string{"a", "b"}, nil), WithoutProcessBootstrap()); err == nil {
		t.Error("an unusable control backend must be an error")
	}
}

func TestControlHeartbeatReachesEveryTenantBackendAndReportsABrokenOne(t *testing.T) {
	m, _ := newTestManager(t, []string{"default", "a", "b"}, nil)
	nodeHealth := process.NewNodeHealth()
	res := m.Control().Heartbeat(nodeHealth)
	if !res.Control.OK() {
		t.Fatalf("control heartbeat: %+v", res.Control)
	}
	for id, hb := range res.Tenants {
		if !hb.OK() {
			t.Errorf("tenant %s heartbeat: %+v", id, hb)
		}
	}
	if len(res.Tenants) != 3 {
		t.Errorf("expected the 3 tenants in the heartbeat, got %d", len(res.Tenants))
	}

	// a broken backend of a tenant is reported for that tenant only
	_ = mustTenant(t, m, "b").Backend.Close()
	m.Control().Heartbeat(nodeHealth)
	health := m.Control().Health()
	unhealthy := health.UnhealthyTenants(0)
	if len(unhealthy) != 1 || unhealthy[0].TenantID != "b" {
		t.Errorf("expected only tenant b unhealthy, got %+v", unhealthy)
	}
	if !health.NodeHealthy {
		t.Errorf("a broken tenant backend must not make the node unhealthy: %v", health.NodeErr)
	}
}

func TestTenantsDoNotShareState(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, nil)
	a, b := mustTenant(t, m, "a"), mustTenant(t, m, "b")
	key := inst.InstanceKey{Hostname: "db1", Port: 3306}

	// backend / store
	tenantWriteInstance(t, a, "db1", 3306)
	if !tenantHasInstance(t, a, "db1", 3306) || tenantHasInstance(t, b, "db1", 3306) {
		t.Error("an instance written in tenant a must not exist in tenant b")
	}

	// discovery queues: same host:port, deduplicated per tenant only
	a.discoveryQueue.Push(key)
	queuedInA := a.discoveryQueue.QueueLen()
	if queuedInA == 0 || b.discoveryQueue.QueueLen() != 0 {
		t.Errorf("queues: a=%d b=%d", queuedInA, b.discoveryQueue.QueueLen())
	}
	b.discoveryQueue.Push(key)
	b.deadInstancesDiscoveryQueue.Push(key)
	if b.discoveryQueue.QueueLen() != queuedInA {
		t.Errorf("b must accept the same key a already queued, got %d (a has %d)", b.discoveryQueue.QueueLen(), queuedInA)
	}
	if a.discoveryQueue == b.discoveryQueue || a.deadInstancesDiscoveryQueue == b.deadInstancesDiscoveryQueue {
		t.Error("queues are shared")
	}
	if a.deadInstancesDiscoveryQueue == a.discoveryQueue {
		t.Error("DeadInstanceDiscoveryMaxConcurrency > 0 must give a queue of its own")
	}

	// caches
	if err := a.recentDiscoveryOperationKeys.Add(key.DisplayString(), true, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := b.recentDiscoveryOperationKeys.Add(key.DisplayString(), true, time.Minute); err != nil {
		t.Errorf("the recent discovery keys of a hide the same host:port of b: %v", err)
	}
	for name, caches := range map[string][2]interface {
		Add(string, interface{}, time.Duration) error
	}{
		"emergencyRead":        {a.emergencyReadTopologyInstanceMap, b.emergencyReadTopologyInstanceMap},
		"emergencyRestart":     {a.emergencyRestartReplicaTopologyInstanceMap, b.emergencyRestartReplicaTopologyInstanceMap},
		"emergencyGracePeriod": {a.emergencyOperationGracefulPeriodMap, b.emergencyOperationGracefulPeriodMap},
		"pseudoGTIDPublish":    {a.pseudoGTIDPublishCache, b.pseudoGTIDPublishCache},
		"kvFound":              {a.kvFoundCache, b.kvFoundCache},
	} {
		if err := caches[0].Add(key.StringCode(), true, time.Minute); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := caches[1].Add(key.StringCode(), true, time.Minute); err != nil {
			t.Errorf("%s: tenant b sees the entry of tenant a: %v", name, err)
		}
	}
	a.beginEmergencyOperationGracefulPeriod(&key)
	if !a.isInEmergencyOperationGracefulPeriod(&key) || b.isInEmergencyOperationGracefulPeriod(&inst.InstanceKey{Hostname: "db9", Port: 1}) {
		t.Error("emergency grace period")
	}
	b.emergencyOperationGracefulPeriodMap.Flush()
	if !a.isInEmergencyOperationGracefulPeriod(&key) {
		t.Error("clearing the state of b must not clear the one of a")
	}

	// counters
	a.countPendingRecoveries = 3
	if b.getCountPendingRecoveries() != 0 {
		t.Error("pending recoveries are shared")
	}

	// the recovery state: failure detections, recoveries and the disable flag
	analysis := inst.ReplicationAnalysis{
		AnalyzedInstanceKey: key,
		Analysis:            inst.DeadMaster,
		ClusterDetails:      inst.ClusterInfo{ClusterName: key.StringCode()},
	}
	if ok, err := a.AttemptFailureDetectionRegistration(&analysis); err != nil || !ok {
		t.Fatalf("registering the detection in a: %v %v", ok, err)
	}
	if ok, err := b.AttemptFailureDetectionRegistration(&analysis); err != nil || !ok {
		t.Errorf("the detection already registered in a must not block b: %v %v", ok, err)
	}
	if recovery, err := a.AttemptRecoveryRegistration(&analysis, true, true); err != nil || recovery == nil {
		t.Fatalf("registering the recovery in a: %v %v", recovery, err)
	}
	if recovery, err := b.AttemptRecoveryRegistration(&analysis, true, true); err != nil || recovery == nil {
		t.Errorf("the recovery of a blocks the one of b on the same host:port: %v %v", recovery, err)
	}
	recoveriesOfA, _ := a.ReadActiveRecoveries()
	recoveriesOfB, _ := b.ReadActiveRecoveries()
	if len(recoveriesOfA) != 1 || len(recoveriesOfB) != 1 {
		t.Fatalf("active recoveries: a=%d b=%d", len(recoveriesOfA), len(recoveriesOfB))
	}
	if n, _ := a.AcknowledgeAllRecoveries("test", "all"); n != 1 {
		t.Errorf("acknowledging all recoveries of a: %d", n)
	}
	if recoveries, _ := b.ReadActiveRecoveries(); len(recoveries) != 1 || recoveries[0].Acknowledged {
		t.Error("AcknowledgeAllRecoveries of a must not touch b")
	}

	// KV
	if a.Store.GetClusterMasterKVKey("alias") == b.Store.GetClusterMasterKVKey("alias") {
		t.Error("the master KV key of two tenants with the same cluster alias must differ")
	}
	if err := a.KV.PutKVPairs([]*kv.KVPair{kv.NewKVPair("k", "v")}); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := b.KV.GetValue("k"); found {
		t.Error("the internal KV store of b sees the one of a")
	}
	if v, found, _ := a.KV.GetValue("k"); !found || v != "v" {
		t.Error("the internal KV store lost the pair")
	}
}

func TestRecoveryDisableIsPerTenant(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, nil)
	a, b := mustTenant(t, m, "a"), mustTenant(t, m, "b")
	if err := a.DisableRecovery(); err != nil {
		t.Fatal(err)
	}
	if disabled, err := a.IsRecoveryDisabled(); err != nil || !disabled {
		t.Errorf("a: %v %v", disabled, err)
	}
	if disabled, err := b.IsRecoveryDisabled(); err != nil || disabled {
		t.Errorf("disabling the recoveries of a must not disable b's: %v %v", disabled, err)
	}
	if err := a.EnableRecovery(); err != nil {
		t.Fatal(err)
	}
	if disabled, _ := a.IsRecoveryDisabled(); disabled {
		t.Error("EnableRecovery")
	}
}

func TestTenantMetricsAreNamespacedAndReleased(t *testing.T) {
	m, _ := newTestManager(t, []string{"metrics-a", "metrics-b"}, nil)
	a, b := mustTenant(t, m, "metrics-a"), mustTenant(t, m, "metrics-b")
	a.discoveriesCounter.Inc(2)
	b.discoveriesCounter.Inc(5)
	counter := func(name string) metrics.Counter {
		c, _ := metrics.Get(name).(metrics.Counter)
		return c
	}
	if c := counter("tenants.metrics-a.discoveries.attempt"); c == nil || c.Count() != 2 {
		t.Errorf("tenants.metrics-a.discoveries.attempt = %v", c)
	}
	if c := counter("tenants.metrics-b.discoveries.attempt"); c == nil || c.Count() != 5 {
		t.Errorf("tenants.metrics-b.discoveries.attempt = %v", c)
	}
	a.Metrics.DiscoveriesTotal.Inc()
	if a.Metrics == b.Metrics {
		t.Error("tenants share their Prometheus series")
	}
	m.Stop()
	if metrics.Get("tenants.metrics-a.discoveries.attempt") != nil {
		t.Error("the go-metrics of a stopped tenant must be unregistered")
	}
}

func TestDefaultTenantIsTheLegacySingleTenantOnTheProcessDefaults(t *testing.T) {
	dt := DefaultTenant()
	if dt.ID != config.DefaultTenantID {
		t.Fatalf("default tenant id %q", dt.ID)
	}
	if dt.Cfg != config.Config {
		t.Error("the legacy tenant is driven by config.Config itself")
	}
	if dt.Backend != db.DefaultBackend() || dt.Store != inst.DefaultStore() {
		t.Error("the legacy tenant must share the backend and the store the legacy API uses")
	}
	if DefaultManager().Default() != dt || DefaultTenant() != dt {
		t.Error("DefaultTenant must be stable")
	}
	if process.DefaultControl() != DefaultManager().Control() {
		t.Error("the legacy manager runs on the default control")
	}
}
