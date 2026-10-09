package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	gometrics "github.com/rcrowley/go-metrics"
)

// series returns the samples of a metric family keyed by "label=value/label=value" (labels sorted by name).
func series(t *testing.T, reg prometheus.Gatherer, family string) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			key := ""
			for _, l := range m.GetLabel() {
				if key != "" {
					key += "/"
				}
				key += l.GetName() + "=" + l.GetValue()
			}
			switch {
			case m.GetCounter() != nil:
				out[key] = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				out[key] = m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				out[key] = float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	return out
}

func gatherer(t *testing.T) prometheus.Gatherer {
	t.Helper()
	reg := prometheus.NewRegistry()
	// the single, process-wide registration: must not panic with any number of tenants
	registerPrometheusUnchecked(reg)
	return reg
}

func TestTenantMetricsDoNotCollide(t *testing.T) {
	a, b := ForTenant("iso-a"), ForTenant("iso-b")
	t.Cleanup(a.Forget)
	t.Cleanup(b.Forget)
	if a == b || ForTenant("iso-a") != a {
		t.Fatal("ForTenant must be memoized per tenant and distinct across tenants")
	}

	a.DiscoveriesTotal.Inc()
	a.DiscoveriesTotal.Inc()
	b.DiscoveriesTotal.Inc()
	a.InstancesTotal.Set(7)
	b.InstancesTotal.Set(3)
	a.RecoveriesTotal.WithLabelValues("DeadMaster", "success").Inc()
	b.RecoveriesTotal.WithLabelValues("DeadMaster", "success").Add(5)
	a.RecoveryDurationSeconds.Observe(1.5)

	reg := gatherer(t)
	if got := series(t, reg, "orchestrator_discoveries_total"); got["tenant=iso-a"] != 2 || got["tenant=iso-b"] != 1 {
		t.Errorf("discoveries: %v", got)
	}
	if got := series(t, reg, "orchestrator_instances_total"); got["tenant=iso-a"] != 7 || got["tenant=iso-b"] != 3 {
		t.Errorf("instances: %v", got)
	}
	rec := series(t, reg, "orchestrator_recoveries_total")
	if rec["result=success/tenant=iso-a/type=DeadMaster"] != 1 || rec["result=success/tenant=iso-b/type=DeadMaster"] != 5 {
		t.Errorf("recoveries: %v", rec)
	}
	hist := series(t, reg, "orchestrator_recovery_duration_seconds")
	if hist["tenant=iso-a"] != 1 || hist["tenant=iso-b"] != 0 {
		t.Errorf("recovery duration samples: %v", hist)
	}
}

func TestLegacyVariablesAreDefaultTenantSeries(t *testing.T) {
	def := ForTenant(DefaultTenant)
	before := series(t, gatherer(t), "orchestrator_discoveries_total")["tenant="+DefaultTenant]

	DiscoveriesTotal.Inc()
	RecoveriesTotal.WithLabelValues("DeadMaster", "failure").Inc()
	ActiveTopologyIssues.WithLabelValues("dead_master").Set(2)

	reg := gatherer(t)
	if got := series(t, reg, "orchestrator_discoveries_total")["tenant="+DefaultTenant]; got != before+1 {
		t.Errorf("legacy DiscoveriesTotal must feed the default tenant series: before=%v got=%v", before, got)
	}
	if got := series(t, reg, "orchestrator_recoveries_total")["result=failure/tenant=default/type=DeadMaster"]; got < 1 {
		t.Errorf("legacy RecoveriesTotal.WithLabelValues(type, result) must work: %v", got)
	}
	if got := series(t, reg, "orchestrator_active_topology_issues")["issue_type=dead_master/tenant=default"]; got != 2 {
		t.Errorf("legacy ActiveTopologyIssues.WithLabelValues(issue): %v", got)
	}
	def.ResetActiveTopologyIssues()
}

func TestResetActiveTopologyIssuesIsPerTenant(t *testing.T) {
	a, b := ForTenant("reset-a"), ForTenant("reset-b")
	t.Cleanup(a.Forget)
	t.Cleanup(b.Forget)

	a.ActiveTopologyIssues.WithLabelValues("dead_master").Set(1)
	a.ActiveTopologyIssues.WithLabelValues("unreachable").Set(4)
	b.ActiveTopologyIssues.WithLabelValues("dead_master").Set(9)

	a.ResetActiveTopologyIssues()

	got := series(t, gatherer(t), "orchestrator_active_topology_issues")
	if _, found := got["issue_type=dead_master/tenant=reset-a"]; found {
		t.Errorf("tenant a series survived the reset: %v", got)
	}
	if got["issue_type=dead_master/tenant=reset-b"] != 9 {
		t.Errorf("tenant b series must survive a's reset: %v", got)
	}

	// the reset tenant can publish again
	a.ActiveTopologyIssues.WithLabelValues("dead_master").Set(2)
	got = series(t, gatherer(t), "orchestrator_active_topology_issues")
	if got["issue_type=dead_master/tenant=reset-a"] != 2 {
		t.Errorf("tenant a could not republish: %v", got)
	}
}

func TestForgetRemovesOnlyThatTenant(t *testing.T) {
	a, b := ForTenant("forget-a"), ForTenant("forget-b")
	t.Cleanup(b.Forget)
	a.InstancesTotal.Set(1)
	b.InstancesTotal.Set(2)
	a.RecoveriesTotal.WithLabelValues("x", "y").Inc()

	a.Forget()

	reg := gatherer(t)
	inst := series(t, reg, "orchestrator_instances_total")
	if _, found := inst["tenant=forget-a"]; found || inst["tenant=forget-b"] != 2 {
		t.Errorf("instances after Forget: %v", inst)
	}
	for k := range series(t, reg, "orchestrator_recoveries_total") {
		if k == "result=y/tenant=forget-a/type=x" {
			t.Errorf("recoveries of a survived Forget")
		}
	}
	if ForTenant("forget-a") == a {
		t.Error("ForTenant after Forget must create fresh metrics")
	}
	ForTenant("forget-a").Forget()
}

func TestProcessWideMetricsHaveNoTenantLabel(t *testing.T) {
	reg := gatherer(t)
	families, _ := reg.Gather()
	for _, mf := range families {
		switch mf.GetName() {
		case "orchestrator_health_is_ready", "orchestrator_health_is_live", "orchestrator_raft_is_healthy",
			"orchestrator_raft_is_leader", "orchestrator_raft_peers_total", "orchestrator_raft_is_part_of_quorum":
			for _, m := range mf.GetMetric() {
				if len(m.GetLabel()) != 0 {
					t.Errorf("%s must not have labels: %v", mf.GetName(), m.GetLabel())
				}
			}
		}
	}
}

func TestRegisteringTwiceIntoSameRegistryPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	registerPrometheusUnchecked(reg)
	defer func() {
		if recover() == nil {
			t.Error("expected a panic on re-registration (this is why tenants must never register)")
		}
	}()
	registerPrometheusUnchecked(reg)
}

func TestTenantName(t *testing.T) {
	for _, tc := range []struct{ tenant, name, want string }{
		{"", "discoveries.attempt", "discoveries.attempt"},
		{DefaultTenant, "discoveries.attempt", "discoveries.attempt"},
		{"mariadb-a", "discoveries.attempt", "tenants.mariadb-a.discoveries.attempt"},
	} {
		if got := TenantName(tc.tenant, tc.name); got != tc.want {
			t.Errorf("TenantName(%q,%q)=%q want %q", tc.tenant, tc.name, got, tc.want)
		}
	}
}

func TestRegisterTenantMetricReportsDuplicates(t *testing.T) {
	t.Cleanup(func() { UnregisterTenantMetrics("gm-a"); UnregisterTenantMetrics("gm-b") })

	c1, c2 := gometrics.NewCounter(), gometrics.NewCounter()
	if err := RegisterTenantMetric("gm-a", "discoveries.attempt", c1); err != nil {
		t.Fatal(err)
	}
	if err := RegisterTenantMetric("gm-a", "discoveries.attempt", c2); err == nil {
		t.Fatal("duplicate registration must be reported")
	}
	// the same name for another tenant does not collide
	if err := RegisterTenantMetric("gm-b", "discoveries.attempt", gometrics.NewCounter()); err != nil {
		t.Fatalf("other tenant must not collide: %v", err)
	}
	// idempotent form returns the existing metric
	if got := GetOrRegisterTenantMetric("gm-a", "discoveries.attempt", gometrics.NewCounter()); got != c1 {
		t.Error("GetOrRegisterTenantMetric must return the already registered metric")
	}

	UnregisterTenantMetrics("gm-a")
	if gometrics.Get(TenantName("gm-a", "discoveries.attempt")) != nil {
		t.Error("UnregisterTenantMetrics left a metric behind")
	}
	if gometrics.Get(TenantName("gm-b", "discoveries.attempt")) == nil {
		t.Error("UnregisterTenantMetrics removed another tenant's metric")
	}
}

func TestMetricsTickRegistration(t *testing.T) {
	called := make(chan string, 4)
	unregisterA := RegisterMetricsTick(func() { called <- "a" })
	unregisterB := RegisterMetricsTick(func() { called <- "b" })
	defer unregisterB()

	for _, f := range snapshotTickCallbacks() {
		f()
	}
	if len(called) != 2 {
		t.Fatalf("expected both callbacks, got %d", len(called))
	}
	for len(called) > 0 {
		<-called
	}

	unregisterA()
	unregisterA() // idempotent
	for _, f := range snapshotTickCallbacks() {
		f()
	}
	if len(called) != 1 || <-called != "b" {
		t.Fatal("only b should remain registered")
	}
}

func TestRaftUnknownTenantCommandsIsExported(t *testing.T) {
	if got := series(t, gatherer(t), "orchestrator_raft_unknown_tenant_commands_total"); len(got) == 0 {
		t.Error("orchestrator_raft_unknown_tenant_commands_total is not registered")
	}
}
