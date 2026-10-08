package logic

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
	"github.com/proxysql/orchestrator/go/inst"
	ometrics "github.com/proxysql/orchestrator/go/metrics"
)

// legacyDefaultsDisabled tells whether db.DefaultBackend() is the inert one.
func legacyDefaultsDisabled() bool {
	_, err := db.DefaultBackend().Open()
	return errors.Is(err, db.ErrDefaultBackendDisabled)
}

// While a manager built from a tenants file is alive nothing may reach the
// legacy backend/store: a lost call must fail with an error.
func TestManagerFromTenantsDisablesTheLegacyDefaults(t *testing.T) {
	if legacyDefaultsDisabled() {
		t.Fatal("precondition: the legacy defaults must be enabled")
	}
	base := testBase(t)
	m1, err := NewTenantManagerFor(base, testTenantConfigs(t, base, []string{"a", "b"}, nil), WithoutProcessBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	if !legacyDefaultsDisabled() {
		t.Fatal("db.DefaultBackend() must be disabled while a multi-tenant manager lives")
	}
	if _, err := inst.ReadAllInstanceKeys(); !errors.Is(err, db.ErrDefaultBackendDisabled) {
		t.Errorf("a legacy inst call must fail with ErrDefaultBackendDisabled, got %v", err)
	}
	// the tenants themselves keep working
	tenantWriteInstance(t, mustTenant(t, m1, "a"), "h", 3306)

	// a second manager: the defaults stay disabled until the last one stops
	base2 := testBase(t)
	m2, err := NewTenantManagerFor(base2, testTenantConfigs(t, base2, []string{"c"}, nil), WithoutProcessBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	m1.Stop()
	if !legacyDefaultsDisabled() {
		t.Error("the defaults must stay disabled while another manager lives")
	}
	m2.Stop()
	m2.Stop() // idempotent: must not release twice
	if legacyDefaultsDisabled() {
		t.Error("the legacy defaults must come back once the last manager is stopped")
	}
}

func TestFailedManagerConstructionReenablesTheLegacyDefaults(t *testing.T) {
	base := testBase(t)
	bad := testTenantConfigs(t, base, []string{"bad"}, nil)
	bad[0].Cfg.SQLite3DataFile = filepath.Join(t.TempDir(), "no-such-dir", "deeper", "bad.sqlite3")
	if _, err := NewTenantManagerFor(base, bad, WithoutProcessBootstrap()); err == nil {
		t.Fatal("expected the construction to fail")
	}
	if legacyDefaultsDisabled() {
		t.Error("a failed construction must not leave the legacy defaults disabled")
	}
}

// The legacy metric variables are the series of the tenant "default": in a
// multi-tenant process without such a tenant they must not be exported.
func TestNoPhantomDefaultTenantSeriesInMultiTenantProcess(t *testing.T) {
	prev := config.Config.PrometheusEnabled
	config.Config.PrometheusEnabled = true
	ometrics.InitPrometheus() // registers the vecs in the default registry, once
	config.Config.PrometheusEnabled = prev
	newTestManager(t, []string{"a", "b"}, nil)
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, f := range families {
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == "tenant" {
					seen[l.GetValue()] = true
				}
			}
		}
	}
	if seen[config.DefaultTenantID] {
		t.Errorf("series of the tenant %q exported although there is no such tenant", config.DefaultTenantID)
	}
	if !seen["a"] {
		t.Errorf("series of tenant a missing; tenants seen: %v", seen)
	}
}
