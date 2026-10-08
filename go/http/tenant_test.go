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

package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/logic"
)

// testLeadership makes every tenant think the node leads, which is irrelevant to
// the HTTP layer: the managers of these tests are never started.
type testLeadership struct{}

func (testLeadership) IsLeader() bool         { return true }
func (testLeadership) IsLeaderOrActive() bool { return true }

// newTestManager builds a TenantManager over one SQLite backend per tenant (and
// one for the node control when no tenant is "default"). It is not started.
func newTestManager(t *testing.T, ids ...string) *logic.TenantManager {
	t.Helper()
	dir := t.TempDir()
	newCfg := func(id string) *config.Configuration {
		cfg := config.Config.Clone()
		cfg.BackendDB = "sqlite3"
		cfg.SQLite3DataFile = filepath.Join(dir, id+".sqlite3")
		cfg.ProviderType = config.ProviderMySQL
		cfg.HostnameResolveMethod = "none"
		cfg.RaftEnabled = false
		return cfg
	}
	base := newCfg("control")
	var tcs []*config.TenantConfig
	for _, id := range ids {
		tcs = append(tcs, &config.TenantConfig{ID: id, Provider: config.ProviderMySQL, Cfg: newCfg(id)})
	}
	m, err := logic.NewTenantManagerFor(base, tcs, logic.WithoutProcessBootstrap(), logic.WithLeadership(testLeadership{}))
	if err != nil {
		t.Fatalf("NewTenantManagerFor(%v): %v", ids, err)
	}
	t.Cleanup(m.Stop)
	return m
}

// newTestRouter registers API v1, the web pages and API v2 on a router that
// resolves tenants in m.
func newTestRouter(m *logic.TenantManager) chi.Router {
	router := chi.NewRouter()
	api := &HttpAPI{Manager: m}
	api.RegisterRequests(router)
	RegisterV2RoutesFor(router, m)
	web := &HttpWeb{Manager: m}
	web.RegisterRequests(router)
	return router
}

type response struct {
	Code   int
	Header http.Header
	Body   []byte
}

func doGet(t *testing.T, router http.Handler, target string, headers ...string) response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return response{Code: rec.Code, Header: rec.Header(), Body: rec.Body.Bytes()}
}

// envelope is the APIResponse as a client sees it.
type envelope struct {
	Code     string
	Message  string
	Details  json.RawMessage
	TenantId string
}

func (r response) envelope(t *testing.T) envelope {
	t.Helper()
	var e envelope
	if err := json.Unmarshal(r.Body, &e); err != nil {
		t.Fatalf("not an API envelope (status %d): %v: %s", r.Code, err, r.Body)
	}
	return e
}

func writeTenantInstance(t *testing.T, tenant *logic.Tenant, host string, port int, cluster string) {
	t.Helper()
	instance := inst.NewInstance()
	instance.Key = inst.InstanceKey{Hostname: host, Port: port}
	instance.Version = "10.6.0-MariaDB"
	instance.ServerID = 1
	instance.ClusterName = cluster
	if err := tenant.Store.WriteInstance(instance, true, nil); err != nil {
		t.Fatalf("tenant %s: WriteInstance: %v", tenant.ID, err)
	}
}

func mustTenant(t *testing.T, m *logic.TenantManager, id string) *logic.Tenant {
	t.Helper()
	tenant, ok := m.Get(id)
	if !ok {
		t.Fatalf("no tenant %q", id)
	}
	return tenant
}

func TestTenantResolutionPriority(t *testing.T) {
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)
	const route = "/api/check-global-recoveries"

	tests := []struct {
		name    string
		target  string
		headers []string
		want    string
	}{
		{"path", "/api/tenants/alpha/check-global-recoveries", nil, "alpha"},
		{"path, other tenant", "/api/tenants/beta/check-global-recoveries", nil, "beta"},
		{"header", route, []string{TenantHeader, "beta"}, "beta"},
		{"query", route + "?tenant=alpha", nil, "alpha"},
		{"path over header", "/api/tenants/alpha/check-global-recoveries", []string{TenantHeader, "beta"}, "alpha"},
		{"header over query", route + "?tenant=alpha", []string{TenantHeader, "beta"}, "beta"},
		{"path over query", "/api/tenants/beta/check-global-recoveries?tenant=alpha", nil, "beta"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doGet(t, router, tt.target, tt.headers...)
			if resp.Code != http.StatusOK {
				t.Fatalf("status %d: %s", resp.Code, resp.Body)
			}
			if got := resp.envelope(t).TenantId; got != tt.want {
				t.Errorf("TenantId = %q, want %q", got, tt.want)
			}
			if got := resp.Header.Get(TenantHeader); got != tt.want {
				t.Errorf("response header %s = %q, want %q", TenantHeader, got, tt.want)
			}
		})
	}
}

func TestTenantResolutionAmbiguousAndUnknown(t *testing.T) {
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)

	// no tenant, no "default", several tenants: 400 listing the tenants
	resp := doGet(t, router, "/api/check-global-recoveries")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous request: status %d, want 400: %s", resp.Code, resp.Body)
	}
	e := resp.envelope(t)
	if e.Code != "ERROR" || !strings.Contains(e.Message, "tenantId required") {
		t.Errorf("message %q does not say tenantId is required", e.Message)
	}
	var details struct{ Tenants []string }
	if err := json.Unmarshal(e.Details, &details); err != nil || len(details.Tenants) != 2 || details.Tenants[0] != "alpha" || details.Tenants[1] != "beta" {
		t.Errorf("Details = %s, want the list of tenants alpha, beta", e.Details)
	}

	// unknown tenant, by every means: 404
	for _, target := range []string{"/api/tenants/gamma/check-global-recoveries", "/api/check-global-recoveries?tenant=gamma"} {
		if resp := doGet(t, router, target); resp.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404: %s", target, resp.Code, resp.Body)
		}
	}
	if resp := doGet(t, router, "/api/check-global-recoveries", TenantHeader, "gamma"); resp.Code != http.StatusNotFound {
		t.Errorf("unknown tenant by header: status %d, want 404", resp.Code)
	}
	// the web pages and v2 fail the same way
	if resp := doGet(t, router, "/web/search"); resp.Code != http.StatusBadRequest {
		t.Errorf("ambiguous web page: status %d, want 400", resp.Code)
	}
	if resp := doGet(t, router, "/api/v2/clusters"); resp.Code != http.StatusBadRequest {
		t.Errorf("ambiguous v2 request: status %d, want 400", resp.Code)
	}
	if resp := doGet(t, router, "/api/v2/tenants/gamma/clusters"); resp.Code != http.StatusNotFound {
		t.Errorf("unknown tenant in v2: status %d, want 404", resp.Code)
	}
}

func TestImplicitTenant(t *testing.T) {
	t.Run("default tenant among several", func(t *testing.T) {
		router := newTestRouter(newTestManager(t, "alpha", config.DefaultTenantID, "beta"))
		resp := doGet(t, router, "/api/check-global-recoveries")
		if resp.Code != http.StatusOK || resp.envelope(t).TenantId != config.DefaultTenantID {
			t.Fatalf("status %d, body %s: want the %q tenant", resp.Code, resp.Body, config.DefaultTenantID)
		}
		if resp := doGet(t, router, "/api/check-global-recoveries?tenant=beta"); resp.envelope(t).TenantId != "beta" {
			t.Errorf("an explicit tenant must win over the implicit one: %s", resp.Body)
		}
	})
	t.Run("single tenant", func(t *testing.T) {
		router := newTestRouter(newTestManager(t, "solo"))
		resp := doGet(t, router, "/api/check-global-recoveries")
		if resp.Code != http.StatusOK || resp.envelope(t).TenantId != "solo" {
			t.Fatalf("status %d, body %s: want the only tenant", resp.Code, resp.Body)
		}
	})
}

func TestSameRouteDifferentTenantsDifferentData(t *testing.T) {
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)

	// the very same host:port, only in alpha
	writeTenantInstance(t, mustTenant(t, m, "alpha"), "db1.example.com", 3306, "alpha-cluster")

	resp := doGet(t, router, "/api/tenants/alpha/instance/db1.example.com/3306")
	if resp.Code != http.StatusOK {
		t.Fatalf("alpha: status %d: %s", resp.Code, resp.Body)
	}
	var instance struct {
		Key         inst.InstanceKey
		ClusterName string
		TenantId    string
	}
	if err := json.Unmarshal(resp.Body, &instance); err != nil {
		t.Fatalf("alpha: %v: %s", err, resp.Body)
	}
	if instance.Key.Hostname != "db1.example.com" || instance.ClusterName != "alpha-cluster" {
		t.Errorf("alpha: unexpected instance %+v", instance)
	}
	if instance.TenantId != "alpha" {
		t.Errorf("the raw instance must carry its tenant: TenantId = %q (%s)", instance.TenantId, resp.Body)
	}

	// beta does not know it
	resp = doGet(t, router, "/api/tenants/beta/instance/db1.example.com/3306")
	if e := resp.envelope(t); e.Code != "ERROR" || !strings.Contains(e.Message, "Cannot read instance") || e.TenantId != "beta" {
		t.Errorf("beta: %s", resp.Body)
	}

	// and when beta has the same host:port, it has its own copy
	writeTenantInstance(t, mustTenant(t, m, "beta"), "db1.example.com", 3306, "beta-cluster")
	resp = doGet(t, router, "/api/tenants/beta/instance/db1.example.com/3306")
	if err := json.Unmarshal(resp.Body, &instance); err != nil || instance.ClusterName != "beta-cluster" || instance.TenantId != "beta" {
		t.Errorf("beta: unexpected instance %+v (%v): %s", instance, err, resp.Body)
	}

	// the cluster list is per tenant too (a list of scalars: the header names the tenant)
	for id, want := range map[string]string{"alpha": "alpha-cluster", "beta": "beta-cluster"} {
		resp := doGet(t, router, "/api/tenants/"+id+"/clusters")
		var clusters []string
		if err := json.Unmarshal(resp.Body, &clusters); err != nil || len(clusters) != 1 || clusters[0] != want {
			t.Errorf("%s: clusters = %v (%v), want [%s]", id, clusters, err, want)
		}
		if resp.Header.Get(TenantHeader) != id {
			t.Errorf("%s: response header = %q", id, resp.Header.Get(TenantHeader))
		}
	}

	// v2 follows the same rules
	resp = doGet(t, router, "/api/v2/tenants/alpha/clusters")
	var v2 struct {
		Status   string
		TenantID string `json:"tenantId"`
		Data     []struct{ ClusterName string }
	}
	if err := json.Unmarshal(resp.Body, &v2); err != nil || v2.Status != "ok" || v2.TenantID != "alpha" || len(v2.Data) != 1 || v2.Data[0].ClusterName != "alpha-cluster" {
		t.Errorf("v2 alpha: %+v (%v): %s", v2, err, resp.Body)
	}
}

func TestLegacySingleTenantResponsesAreUnchanged(t *testing.T) {
	m := newTestManager(t, config.DefaultTenantID)
	router := newTestRouter(m)
	writeTenantInstance(t, mustTenant(t, m, config.DefaultTenantID), "db1.example.com", 3306, "c1")

	// the raw object of a legacy request has no new member...
	resp := doGet(t, router, "/api/instance/db1.example.com/3306")
	if strings.Contains(string(resp.Body), "TenantId") {
		t.Errorf("a legacy single-tenant raw object must not change: %s", resp.Body)
	}
	// ...but names its tenant when the request does
	resp = doGet(t, router, "/api/tenants/default/instance/db1.example.com/3306")
	if !strings.Contains(string(resp.Body), `"TenantId":"default"`) {
		t.Errorf("an explicit request must get the tenant in the object: %s", resp.Body)
	}
	// the envelope is additive
	if e := doGet(t, router, "/api/check-global-recoveries").envelope(t); e.Code != "OK" || e.TenantId != "default" {
		t.Errorf("envelope = %+v", e)
	}
}

func TestListTenantsAndHealthDetails(t *testing.T) {
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)

	check := func(t *testing.T, name string, details json.RawMessage) {
		t.Helper()
		var tenants []struct {
			ID       string
			Provider string
			Healthy  bool
			Message  string
		}
		if err := json.Unmarshal(details, &tenants); err != nil {
			t.Fatalf("%s: %v: %s", name, err, details)
		}
		if len(tenants) != 2 || tenants[0].ID != "alpha" || tenants[1].ID != "beta" {
			t.Fatalf("%s: tenants = %+v", name, tenants)
		}
		for _, tn := range tenants {
			if tn.Provider != config.ProviderMySQL || !tn.Healthy {
				t.Errorf("%s: unexpected tenant status %+v", name, tn)
			}
		}
	}

	// /api/tenants is global: it works although the request names no tenant and none is implicit
	resp := doGet(t, router, "/api/tenants")
	if resp.Code != http.StatusOK {
		t.Fatalf("/api/tenants: status %d: %s", resp.Code, resp.Body)
	}
	check(t, "/api/tenants", resp.envelope(t).Details)

	for _, target := range []string{"/api/health", "/api/status"} {
		resp := doGet(t, router, target)
		var details struct {
			Details struct {
				Healthy  bool
				Hostname string
				Tenants  json.RawMessage
			}
		}
		if err := json.Unmarshal(resp.Body, &details); err != nil {
			t.Fatalf("%s: %v: %s", target, err, resp.Body)
		}
		if details.Details.Hostname == "" {
			t.Errorf("%s: the HealthStatus fields must stay where they were: %s", target, resp.Body)
		}
		check(t, target, details.Details.Tenants)
	}

	resp = doGet(t, router, "/api/v2/tenants")
	var v2 struct{ Data json.RawMessage }
	if err := json.Unmarshal(resp.Body, &v2); err != nil {
		t.Fatal(err)
	}
	check(t, "/api/v2/tenants", v2.Data)
}

func TestGlobalRoutesHaveNoTenant(t *testing.T) {
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)

	// reachable without naming a tenant although the request would be ambiguous
	for _, target := range []string{
		"/api/health", "/api/lb-check", "/api/_ping", "/api/leader-check", "/api/leader-check/500", "/api/headers",
		"/api/tenants", "/health/live", "/health/ready", "/health/leader", "/api/v2/status", "/api/v2/tenants", "/api/raft-state",
	} {
		resp := doGet(t, router, target)
		if resp.Code == http.StatusBadRequest {
			t.Errorf("%s: global route asked for a tenant: %s", target, resp.Body)
		}
		if resp.Code == http.StatusNotFound && !strings.Contains(target, "leader") {
			t.Errorf("%s: not registered", target)
		}
		if resp.Header.Get(TenantHeader) != "" {
			t.Errorf("%s: a global response must not name a tenant", target)
		}
	}

	// ...and have no canonical form
	for _, target := range []string{
		"/api/tenants/alpha/health", "/api/tenants/alpha/lb-check", "/api/tenants/alpha/leader-check",
		"/api/tenants/alpha/raft-state", "/api/tenants/alpha/status", "/api/tenants/alpha/tenants",
	} {
		if resp := doGet(t, router, target); resp.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404 (global route)", target, resp.Code)
		}
	}
}

func TestReadinessDoesNotDependOnATenant(t *testing.T) {
	// health/ready only reflects the node; a broken tenant is reported in Details.Tenants
	// (see process.Control), never by turning the node unready.
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)
	before := doGet(t, router, "/health/ready")
	if err := mustTenant(t, m, "beta").Backend.Close(); err != nil {
		t.Fatal(err)
	}
	after := doGet(t, router, "/health/ready")
	if before.Code != after.Code {
		t.Errorf("readiness changed with a broken tenant backend: %d -> %d", before.Code, after.Code)
	}
}

func TestMutationInOneTenantDoesNotAffectTheOther(t *testing.T) {
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)
	alpha, beta := mustTenant(t, m, "alpha"), mustTenant(t, m, "beta")
	writeTenantInstance(t, alpha, "db1.example.com", 3306, "c")
	writeTenantInstance(t, beta, "db1.example.com", 3306, "c")

	recoveries := func(id string) string {
		resp := doGet(t, router, "/api/tenants/"+id+"/check-global-recoveries")
		e := resp.envelope(t)
		if e.TenantId != id {
			t.Fatalf("%s: answered for %q", id, e.TenantId)
		}
		var state string
		_ = json.Unmarshal(e.Details, &state)
		return state
	}
	if a, b := recoveries("alpha"), recoveries("beta"); a != "enabled" || b != "enabled" {
		t.Fatalf("recoveries should start enabled: %s %s", a, b)
	}
	if resp := doGet(t, router, "/api/tenants/alpha/disable-global-recoveries"); resp.envelope(t).Code != "OK" {
		t.Fatalf("disable-global-recoveries: %s", resp.Body)
	}
	if a, b := recoveries("alpha"), recoveries("beta"); a != "disabled" || b != "enabled" {
		t.Errorf("disable-global-recoveries on alpha: alpha=%s beta=%s", a, b)
	}

	downtimed := func(id string) int {
		resp := doGet(t, router, "/api/tenants/"+id+"/downtimed")
		var list []inst.Instance
		if err := json.Unmarshal(resp.Body, &list); err != nil {
			t.Fatalf("%s: %v: %s", id, err, resp.Body)
		}
		return len(list)
	}
	resp := doGet(t, router, "/api/tenants/alpha/begin-downtime/db1.example.com/3306/me/testing/1h")
	if e := resp.envelope(t); e.Code != "OK" || e.TenantId != "alpha" {
		t.Fatalf("begin-downtime: %s", resp.Body)
	}
	if a, b := downtimed("alpha"), downtimed("beta"); a != 1 || b != 0 {
		t.Errorf("begin-downtime on alpha: alpha has %d downtimed instances, beta %d", a, b)
	}
	if resp := doGet(t, router, "/api/tenants/alpha/end-downtime/db1.example.com/3306"); resp.envelope(t).Code != "OK" {
		t.Fatalf("end-downtime: %s", resp.Body)
	}
	if a := downtimed("alpha"); a != 0 {
		t.Errorf("end-downtime on alpha: %d downtimed instances left", a)
	}

	// forget in alpha leaves the same host:port of beta alone
	if resp := doGet(t, router, "/api/tenants/alpha/forget/db1.example.com/3306"); resp.envelope(t).Code != "OK" {
		t.Fatalf("forget: %s", resp.Body)
	}
	if _, found, _ := alpha.Store.ReadInstance(&inst.InstanceKey{Hostname: "db1.example.com", Port: 3306}); found {
		t.Errorf("forget did not forget the instance in alpha")
	}
	if _, found, _ := beta.Store.ReadInstance(&inst.InstanceKey{Hostname: "db1.example.com", Port: 3306}); !found {
		t.Errorf("forget in alpha forgot the instance of beta")
	}
}

func TestTenantScopedConfigurationIsUsed(t *testing.T) {
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)
	mustTenant(t, m, "alpha").Cfg.RecoverMasterClusterFilters = []string{"alpha-only"}
	mustTenant(t, m, "beta").Cfg.RecoverMasterClusterFilters = []string{"beta-only"}

	for _, id := range []string{"alpha", "beta"} {
		resp := doGet(t, router, "/api/tenants/"+id+"/automated-recovery-filters")
		if !strings.Contains(string(resp.Body), id+"-only") || strings.Contains(string(resp.Body), "alpha-only") == (id == "beta") {
			t.Errorf("%s: filters not taken from the tenant configuration: %s", id, resp.Body)
		}
	}
}

func TestWebPagesInBothForms(t *testing.T) {
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)

	// every page exists under /web/tenants/{tenantId}/...; the pages that do not depend
	// on a tenant also work in the legacy form although the request is ambiguous
	chdirToRepoRoot(t)
	clearContentTemplateCache()
	for _, page := range []string{"about", "faq", "keep-calm", "status", "home"} {
		if resp := doGet(t, router, "/web/"+page); resp.Code != http.StatusOK {
			t.Errorf("/web/%s: status %d", page, resp.Code)
		}
		resp := doGet(t, router, "/web/tenants/alpha/"+page)
		if resp.Code != http.StatusOK {
			t.Errorf("/web/tenants/alpha/%s: status %d", page, resp.Code)
		}
	}
	for _, page := range []string{"clusters", "search", "discover", "audit", "audit-recovery", "seeds"} {
		if resp := doGet(t, router, "/web/tenants/beta/"+page); resp.Code != http.StatusOK {
			t.Errorf("/web/tenants/beta/%s: status %d", page, resp.Code)
		}
		if resp := doGet(t, router, "/web/tenants/gamma/"+page); resp.Code != http.StatusNotFound {
			t.Errorf("/web/tenants/gamma/%s: status %d, want 404", page, resp.Code)
		}
	}
	// the root of a tenant workspace goes to its clusters; the root of an ambiguous
	// process to the list of tenants
	if resp := doGet(t, router, "/web/tenants/alpha/"); resp.Code != http.StatusFound || resp.Header.Get("Location") != "/web/tenants/alpha/clusters" {
		t.Errorf("tenant root: %d %s", resp.Code, resp.Header.Get("Location"))
	}
	if resp := doGet(t, router, "/"); resp.Code != http.StatusFound || resp.Header.Get("Location") != "/web/tenants" {
		t.Errorf("ambiguous root: %d %s", resp.Code, resp.Header.Get("Location"))
	}
}

func TestStaticAndOtherWebRoutesAreGlobal(t *testing.T) {
	router := newTestRouter(newTestManager(t, "alpha", "beta"))
	for _, target := range []string{"/debug/vars", "/debug/pprof"} {
		if resp := doGet(t, router, target); resp.Code == http.StatusNotFound || resp.Code == http.StatusBadRequest {
			t.Errorf("%s: status %d", target, resp.Code)
		}
	}
}

// TestRaftReverseProxyKeepsTheTenant: the Raft reverse proxy forwards the request
// untouched, so the leader resolves the same tenant by path, header or query.
func TestRaftReverseProxyKeepsTheTenant(t *testing.T) {
	var got struct{ path, query, header, auth string }
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.path, got.query, got.header, got.auth = r.URL.Path, r.URL.RawQuery, r.Header.Get(TenantHeader), r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer leader.Close()
	leaderURL, err := url.Parse(leader.URL)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct{ name, target, header string }{
		{"path", "/api/tenants/alpha/cluster/db1:3306", ""},
		{"query", "/api/cluster/db1:3306?tenant=alpha", ""},
		{"header", "/api/cluster/db1:3306", "alpha"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.target, nil)
			if tt.header != "" {
				req.Header.Set(TenantHeader, tt.header)
			}
			req.Header.Set("Authorization", "Basic abc")
			rec := httptest.NewRecorder()
			forwardToLeader(rec, req, leaderURL, http.DefaultTransport)

			wantPath, wantQuery, _ := strings.Cut(tt.target, "?")
			if rec.Code != http.StatusNoContent || got.path != wantPath || got.query != wantQuery || got.header != tt.header || got.auth != "Basic abc" {
				t.Errorf("leader saw %+v (status %d), want path %q query %q header %q", got, rec.Code, wantPath, wantQuery, tt.header)
			}
		})
	}
}

func TestTagTenant(t *testing.T) {
	type thing struct{ A int }
	cases := map[string]struct {
		in   interface{}
		want string
	}{
		"struct":      {thing{A: 1}, `{"TenantId":"t","A":1}`},
		"pointer":     {&thing{A: 1}, `{"TenantId":"t","A":1}`},
		"slice":       {[]thing{{A: 1}, {A: 2}}, `[{"TenantId":"t","A":1},{"TenantId":"t","A":2}]`},
		"ptr slice":   {[]*thing{{A: 1}}, `[{"TenantId":"t","A":1}]`},
		"empty slice": {[]thing{}, `[]`},
		"strings":     {[]string{"a"}, `["a"]`},
		"map":         {map[string]int{"a": 1}, `{"a":1}`},
		"string":      {"x", `"x"`},
		"nil":         {nil, `null`},
		"has member":  {struct{ TenantId string }{"x"}, `{"TenantId":"x"}`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(tagTenant(c.in, "t"))
			if err != nil || string(encoded) != c.want {
				t.Errorf("tagTenant = %s (%v), want %s", encoded, err, c.want)
			}
		})
	}
}

func TestRegisteredTenantRoutesMirrorTheLegacyOnes(t *testing.T) {
	router := chi.NewRouter()
	(&HttpAPI{Manager: newTestManager(t, "alpha")}).RegisterRequests(router)
	legacy, canonical := map[string]bool{}, map[string]bool{}
	err := chi.Walk(router, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if rest, ok := strings.CutPrefix(route, "/api/tenants/{tenantId}/"); ok {
			canonical[rest] = true
		} else if rest, ok := strings.CutPrefix(route, "/api/"); ok {
			legacy[rest] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(canonical) < 150 {
		t.Fatalf("only %d tenant routes registered", len(canonical))
	}
	for route := range canonical {
		if !legacy[route] {
			t.Errorf("tenant route %q has no legacy form", route)
		}
	}
	for route := range legacy {
		if !canonical[route] && !isGlobalRoute(route) {
			t.Errorf("legacy route %q is not global and has no tenant form", route)
		}
	}
}

// isGlobalRoute lists the routes that belong to the process and have no tenant form.
func isGlobalRoute(route string) bool {
	base, _, _ := strings.Cut(route, "/")
	switch base {
	case "headers", "health", "lb-check", "_ping", "leader-check", "routed-leader-check", "grab-election", "reelect",
		"reload-configuration", "status", "tenants", "all-tenants", "seeds", "agents", "agent":
		return true
	}
	return strings.HasPrefix(base, "raft-") || strings.HasPrefix(base, "agent-")
}

func TestAggregatedClustersInfoNamesEveryTenant(t *testing.T) {
	m := newTestManager(t, "alpha", "beta")
	router := newTestRouter(m)
	writeTenantInstance(t, mustTenant(t, m, "alpha"), "db1.example.com", 3306, "db1.example.com:3306")
	writeTenantInstance(t, mustTenant(t, m, "beta"), "db1.example.com", 3306, "db1.example.com:3306")

	resp := doGet(t, router, "/api/all-tenants/clusters-info")
	var clusters []struct{ TenantId, ClusterName string }
	if err := json.Unmarshal(resp.envelope(t).Details, &clusters); err != nil {
		t.Fatalf("%v: %s", err, resp.Body)
	}
	if len(clusters) != 2 || clusters[0].TenantId != "alpha" || clusters[1].TenantId != "beta" || clusters[0].ClusterName != clusters[1].ClusterName {
		t.Errorf("clusters = %+v", clusters)
	}
}
