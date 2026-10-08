package process

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
	"github.com/proxysql/orchestrator/go/util"
)

func newTestBackend(t *testing.T, id string) *db.Backend {
	t.Helper()
	cfg := *config.Config
	cfg.BackendDB = "sqlite3"
	cfg.SQLite3DataFile = filepath.Join(t.TempDir(), id+".sqlite3")
	b, err := db.NewBackend(id, &cfg)
	if err != nil {
		t.Fatalf("NewBackend(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.EnsureSchema(); err != nil {
		t.Fatalf("EnsureSchema(%s): %v", id, err)
	}
	return b
}

func count(t *testing.T, b *db.Backend, table string) int {
	t.Helper()
	n := -1
	err := b.Query(fmt.Sprintf("select count(*) as c from %s", table), nil, func(m sqlutils.RowMap) error {
		n = m.GetInt("c")
		return nil
	})
	if err != nil {
		t.Fatalf("count %s on %s: %v", table, b.ID(), err)
	}
	return n
}

func testNode(host string) *NodeHealth {
	return &NodeHealth{Hostname: host, Token: "token-" + host, AppVersion: "test"}
}

func newControlWithMirrors(t *testing.T) (c *Control, control, a, b *db.Backend) {
	t.Helper()
	control = newTestBackend(t, "control")
	a = newTestBackend(t, "tenant-a")
	b = newTestBackend(t, "tenant-b")
	c = NewControl(control)
	if err := c.AddMirror("a", a); err != nil {
		t.Fatal(err)
	}
	if err := c.AddMirror("b", b); err != nil {
		t.Fatal(err)
	}
	return c, control, a, b
}

func TestHeartbeatReachesControlAndEveryMirror(t *testing.T) {
	c, control, a, b := newControlWithMirrors(t)
	res := c.Heartbeat(testNode("h1"))
	if !res.Control.OK() {
		t.Fatalf("control heartbeat failed: %+v", res.Control)
	}
	for id, hb := range res.Tenants {
		if !hb.OK() {
			t.Errorf("tenant %s heartbeat failed: %+v", id, hb)
		}
	}
	if len(res.Tenants) != 2 {
		t.Fatalf("expected 2 tenant results, got %d", len(res.Tenants))
	}
	for _, be := range []*db.Backend{control, a, b} {
		if n := count(t, be, "node_health"); n != 1 {
			t.Errorf("%s: node_health rows = %d, want 1", be.ID(), n)
		}
		if n := count(t, be, "node_health_history"); n != 1 {
			t.Errorf("%s: node_health_history rows = %d, want 1", be.ID(), n)
		}
	}
	// Second heartbeat updates, does not duplicate.
	c.Heartbeat(testNode("h1"))
	for _, be := range []*db.Backend{control, a, b} {
		if n := count(t, be, "node_health"); n != 1 {
			t.Errorf("%s: node_health rows after 2nd heartbeat = %d, want 1", be.ID(), n)
		}
	}
	h := c.Health()
	if !h.NodeHealthy || len(h.Tenants) != 2 || len(h.UnhealthyTenants(0)) != 0 {
		t.Errorf("unexpected aggregated health: %+v", h)
	}
}

func TestBrokenMirrorDoesNotAffectControlOrOtherMirrors(t *testing.T) {
	c, control, a, b := newControlWithMirrors(t)
	if err := b.Close(); err != nil { // tenant b backend dies
		t.Fatal(err)
	}
	res := c.Heartbeat(testNode("h1"))
	if !res.Control.OK() {
		t.Fatalf("control must stay healthy: %+v", res.Control)
	}
	if !res.Tenants["a"].OK() {
		t.Errorf("tenant a must stay healthy: %+v", res.Tenants["a"])
	}
	if res.Tenants["b"].OK() || !errors.Is(res.Tenants["b"].Err, db.ErrBackendClosed) {
		t.Fatalf("tenant b should report ErrBackendClosed, got %+v", res.Tenants["b"])
	}
	terrs := res.TenantErrors()
	if len(terrs) != 1 || terrs[0].TenantID != "b" {
		t.Fatalf("TenantErrors = %+v", terrs)
	}
	if n := count(t, control, "node_health"); n != 1 {
		t.Errorf("control node_health = %d, want 1", n)
	}
	if n := count(t, a, "node_health"); n != 1 {
		t.Errorf("tenant a node_health = %d, want 1", n)
	}
	if SinceLastGoodHealthCheck() > time.Minute {
		t.Errorf("node good-health timestamp must be refreshed by control: %v", SinceLastGoodHealthCheck())
	}

	agg := c.Health()
	if !agg.NodeHealthy {
		t.Errorf("node must be healthy with only a tenant failing: %v", agg.NodeErr)
	}
	bad := agg.UnhealthyTenants(0)
	if len(bad) != 1 || bad[0].TenantID != "b" || bad[0].Healthy || !errors.Is(bad[0].Error, db.ErrBackendClosed) {
		t.Fatalf("UnhealthyTenants = %+v", bad)
	}
	time.Sleep(20 * time.Millisecond)
	if got := c.Health().UnhealthyTenants(10 * time.Millisecond); len(got) != 1 {
		t.Errorf("tenant b should have been failing for >10ms: %+v", got)
	}
	if got := c.Health().UnhealthyTenants(time.Hour); len(got) != 0 {
		t.Errorf("tenant b has not been failing for an hour: %+v", got)
	}
}

func TestTenantRecoversAfterRemoveAndReAdd(t *testing.T) {
	c, _, _, b := newControlWithMirrors(t)
	_ = b.Close()
	c.Heartbeat(testNode("h1"))
	c.RemoveMirror("b")
	if res := c.Heartbeat(testNode("h1")); len(res.Tenants) != 1 {
		t.Fatalf("removed mirror still reported: %+v", res.Tenants)
	}
	nb := newTestBackend(t, "tenant-b2")
	if err := c.AddMirror("b", nb); err != nil {
		t.Fatal(err)
	}
	if res := c.Heartbeat(testNode("h1")); !res.Tenants["b"].OK() {
		t.Fatalf("re-added tenant b: %+v", res.Tenants["b"])
	}
	if n := count(t, nb, "node_health"); n != 1 {
		t.Errorf("node_health in new backend = %d", n)
	}
}

func TestControlSharedAsTenantIsNotWrittenTwice(t *testing.T) {
	control := newTestBackend(t, "control")
	c := NewControl(control)
	if err := c.AddMirror("default", control); err != nil {
		t.Fatal(err)
	}
	res := c.Heartbeat(testNode("h1"))
	if !res.Tenants["default"].OK() {
		t.Fatalf("shared tenant should carry control outcome: %+v", res.Tenants)
	}
	if got := c.MirrorIDs(); len(got) != 1 || got[0] != "default" {
		t.Errorf("MirrorIDs = %v", got)
	}
	if n := count(t, control, "node_health"); n != 1 {
		t.Errorf("node_health = %d", n)
	}
	if h := c.TenantsHealth(); len(h) != 1 || !h[0].Healthy || h[0].TenantID != "default" {
		t.Errorf("TenantsHealth = %+v", h)
	}
	_ = control.Close()
	res = c.Heartbeat(testNode("h1"))
	if res.Control.OK() || res.Tenants["default"].OK() {
		t.Errorf("control failure must show on the tenant sharing it: %+v", res)
	}
	if agg := c.Health(); agg.NodeHealthy {
		t.Errorf("node must be unhealthy when control fails")
	}
}

func TestSlowMirrorIsReportedWithoutBlockingControl(t *testing.T) {
	c, _, _, _ := newControlWithMirrors(t)
	c.SetMirrorTimeout(50 * time.Millisecond)
	release := make(chan struct{})
	calls := make(chan string, 8)
	c.writeMirror = func(tg *target, nh *NodeHealth) (bool, error) {
		calls <- tg.id
		if tg.id == "b" {
			<-release
		}
		return true, nil
	}
	start := time.Now()
	res := c.Heartbeat(testNode("h1"))
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("heartbeat blocked by a slow mirror: %v", d)
	}
	if !res.Control.OK() || !res.Tenants["a"].OK() {
		t.Fatalf("control and fast mirror must be OK: %+v", res)
	}
	if !errors.Is(res.Tenants["b"].Err, errMirrorTimeout) {
		t.Fatalf("slow mirror should time out: %+v", res.Tenants["b"])
	}
	// Still running: the next heartbeat does not pile up another write.
	res = c.Heartbeat(testNode("h1"))
	if !errors.Is(res.Tenants["b"].Err, errMirrorBusy) {
		t.Fatalf("expected busy, got %+v", res.Tenants["b"])
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if res = c.Heartbeat(testNode("h1")); res.Tenants["b"].OK() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slow mirror never recovered: %+v", res.Tenants["b"])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestExpireOnControlAndMirrors(t *testing.T) {
	c, control, a, b := newControlWithMirrors(t)
	for _, be := range []*db.Backend{control, a, b} {
		_, err := be.Exec(`insert into node_health (hostname, token, first_seen_active, last_seen_active) values ('old', 'old', now() - interval 1 hour, now() - interval 1 hour)`)
		if err != nil {
			t.Fatal(err)
		}
		_, err = be.Exec(`insert into node_health_history (hostname, token, first_seen_active, extra_info) values ('old', 'old', now() - interval 100000 hour, '')`)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := c.ExpireAvailableNodes(); err != nil {
		t.Fatalf("ExpireAvailableNodes: %v", err)
	}
	if err := c.ExpireNodesHistory(); err != nil {
		t.Fatalf("ExpireNodesHistory: %v", err)
	}
	for _, be := range []*db.Backend{control, a, b} {
		if n := count(t, be, "node_health"); n != 0 {
			t.Errorf("%s: node_health = %d after expire", be.ID(), n)
		}
		if n := count(t, be, "node_health_history"); n != 0 {
			t.Errorf("%s: node_health_history = %d after expire", be.ID(), n)
		}
	}
	_ = b.Close()
	err := c.ExpireAvailableNodes()
	terrs := TenantErrors(err)
	if len(terrs) != 1 || terrs[0].TenantID != "b" || !errors.Is(err, db.ErrBackendClosed) {
		t.Fatalf("expected a single tenant error for b, got %v", err)
	}
}

func TestAccessTokensLiveInControl(t *testing.T) {
	c, control, a, _ := newControlWithMirrors(t)
	public, err := c.GenerateAccessToken("owner")
	if err != nil {
		t.Fatal(err)
	}
	if n := count(t, control, "access_token"); n != 1 {
		t.Errorf("control access_token = %d", n)
	}
	if n := count(t, a, "access_token"); n != 0 {
		t.Errorf("tenant backend must not hold tokens, got %d", n)
	}
	secret, err := c.AcquireAccessToken(public)
	if err != nil || secret == "" {
		t.Fatalf("AcquireAccessToken: %q %v", secret, err)
	}
	if ok, err := c.TokenIsValid(public, secret); err != nil || !ok {
		t.Errorf("TokenIsValid = %v %v", ok, err)
	}
	if ok, _ := c.TokenIsValid(public, "bad"); ok {
		t.Errorf("bad secret accepted")
	}
	if _, err := c.AcquireAccessToken("nope"); err == nil {
		t.Errorf("acquiring an unknown token must fail")
	}
	if err := c.ExpireAccessTokens(); err != nil {
		t.Error(err)
	}
}

// expireSeedElection ages the seed row the schema creates in active_node (a
// vacant seat stamped "now"), which otherwise cannot be taken over for
// ActiveNodeExpireSeconds.
func expireSeedElection(t *testing.T, b *db.Backend) {
	t.Helper()
	if _, err := b.Exec(`update active_node set last_seen_active = now() - interval 1 hour where anchor = 1`); err != nil {
		t.Fatal(err)
	}
}

func TestElectionUsesControlBackend(t *testing.T) {
	c, control, a, _ := newControlWithMirrors(t)
	expireSeedElection(t, control)
	ok, err := c.AttemptElection()
	if err != nil || !ok {
		t.Fatalf("AttemptElection = %v %v", ok, err)
	}
	if n := count(t, control, "active_node"); n != 1 {
		t.Errorf("control active_node = %d", n)
	}
	// Tenant backends keep their (inert) seed row: the election never writes there.
	if n := count(t, a, "active_node where hostname != ''"); n != 0 {
		t.Errorf("active_node must not leak to tenants: %d", n)
	}
	node, elected, err := c.ElectedNode()
	if err != nil || !elected || node.Hostname != ThisHostname || node.Token != util.ProcessToken.Hash {
		t.Fatalf("ElectedNode = %+v %v %v", node, elected, err)
	}
	// Another live node holds the election.
	if _, err := control.Exec(`update active_node set hostname='other', token='other', last_seen_active=now() where anchor=1`); err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.AttemptElection(); ok {
		t.Errorf("must not take over a fresh leader")
	}
	if _, elected, _ := c.ElectedNode(); elected {
		t.Errorf("must not be elected")
	}
	if err := c.GrabElection(); err != nil {
		t.Fatal(err)
	}
	if _, elected, _ := c.ElectedNode(); !elected {
		t.Errorf("GrabElection should elect this node")
	}
	if err := c.Reelect(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, control, "active_node"); n != 0 {
		t.Errorf("Reelect left %d rows", n)
	}
}

func TestValidateMultiTenantElection(t *testing.T) {
	cases := []struct {
		raft    bool
		tenants int
		wantErr bool
	}{
		{false, 0, false}, {false, 1, false}, {false, 2, true},
		{true, 1, false}, {true, 2, false}, {true, 10, false},
	}
	for _, tc := range cases {
		err := ValidateMultiTenantElection(tc.raft, tc.tenants)
		if (err != nil) != tc.wantErr {
			t.Errorf("raft=%v tenants=%d: err=%v wantErr=%v", tc.raft, tc.tenants, err, tc.wantErr)
		}
	}
}

func TestAggregateHealth(t *testing.T) {
	hb := &HeartbeatResult{
		Control: BackendHeartbeat{Healthy: true},
		Tenants: map[string]BackendHeartbeat{
			"z": {Healthy: true},
			"a": {Err: errors.New("boom")},
		},
	}
	a := AggregateHealth(hb, nil)
	if !a.NodeHealthy || a.NodeErr != nil {
		t.Errorf("node should be healthy: %+v", a)
	}
	if len(a.Tenants) != 2 || a.Tenants[0].TenantID != "a" || a.Tenants[0].Healthy || !a.Tenants[1].Healthy {
		t.Errorf("tenants = %+v", a.Tenants)
	}
	raftErr := errors.New("no quorum")
	a = AggregateHealth(hb, raftErr)
	if a.NodeHealthy || !errors.Is(a.NodeErr, raftErr) {
		t.Errorf("raft failure must fail the node: %+v", a)
	}
	hb.Control = BackendHeartbeat{Err: errors.New("control down")}
	a = AggregateHealth(hb, nil)
	if a.NodeHealthy || a.ControlErr == nil {
		t.Errorf("control failure must fail the node: %+v", a)
	}
	hb.Control = BackendHeartbeat{} // not registered, no error
	if a = AggregateHealth(hb, nil); a.NodeHealthy {
		t.Errorf("unregistered heartbeat is not healthy")
	}
	if a := NewControl(newTestBackend(t, "x")).Health(); a.NodeHealthy || !errors.Is(a.NodeErr, ErrNoHeartbeatYet) {
		t.Errorf("before the first heartbeat: %+v", a)
	}
}

func TestAddMirrorValidation(t *testing.T) {
	c := NewControl(newTestBackend(t, "control"))
	if err := c.AddMirror("", newTestBackend(t, "x")); err == nil {
		t.Errorf("empty id accepted")
	}
	if err := c.AddMirror("x", nil); err == nil {
		t.Errorf("nil backend accepted")
	}
}

// TestLegacyShimsUseTheDefaultBackend checks that the package-level functions
// behave as before: they operate on db.DefaultBackend(), driven by config.Config.
func TestLegacyShimsUseTheDefaultBackend(t *testing.T) {
	origDB, origFile := config.Config.BackendDB, config.Config.SQLite3DataFile
	defer func() { config.Config.BackendDB, config.Config.SQLite3DataFile = origDB, origFile }()
	config.Config.BackendDB = "sqlite3"
	config.Config.SQLite3DataFile = filepath.Join(t.TempDir(), "default.sqlite3")
	prev := defaultControl.Swap(nil)
	defer defaultControl.Store(prev)

	if DefaultControl().Backend() != db.DefaultBackend() {
		t.Fatalf("default control must sit on the default backend")
	}
	nh := testNode("legacy")
	if healthy, err := RegisterNode(nh); err != nil || !healthy {
		t.Fatalf("RegisterNode = %v %v", healthy, err)
	}
	if n := count(t, db.DefaultBackend(), "node_health"); n != 1 {
		t.Errorf("default backend node_health = %d", n)
	}
	if SinceLastGoodHealthCheck() > time.Minute {
		t.Errorf("good health timestamp not set")
	}
	nodes, err := ReadAvailableNodes(false)
	if err != nil || len(nodes) != 1 || nodes[0].Hostname != ThisHostname {
		t.Errorf("ReadAvailableNodes = %+v %v", nodes, err)
	}
	expireSeedElection(t, db.DefaultBackend())
	if ok, err := AttemptElection(); err != nil || !ok {
		t.Errorf("AttemptElection = %v %v", ok, err)
	}
	if _, elected, err := ElectedNode(); err != nil || !elected {
		t.Errorf("ElectedNode = %v %v", elected, err)
	}
	public, err := GenerateAccessToken("me")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := AcquireAccessToken(public)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := TokenIsValid(public, secret); !ok {
		t.Errorf("legacy token not valid")
	}
	if err := ExpireAccessTokens(); err != nil {
		t.Error(err)
	}
	if err := ExpireNodesHistory(); err != nil {
		t.Error(err)
	}
	ExpireAvailableNodes()
	if err := Reelect(); err != nil {
		t.Error(err)
	}
	if hs, err := HealthTest(); err != nil || !hs.Healthy {
		t.Errorf("HealthTest = %+v %v", hs, err)
	}
}

func TestSetDefaultControl(t *testing.T) {
	prev := defaultControl.Swap(nil)
	defer defaultControl.Store(prev)
	custom := NewControl(newTestBackend(t, "control"))
	SetDefaultControl(custom)
	if DefaultControl() != custom {
		t.Fatalf("SetDefaultControl not honoured")
	}
	if healthy, err := RegisterNode(testNode("x")); err != nil || !healthy {
		t.Fatalf("shim over custom control: %v %v", healthy, err)
	}
	if n := count(t, custom.Backend(), "node_health"); n != 1 {
		t.Errorf("custom control node_health = %d", n)
	}
}

// A mirror write that outlives Heartbeat (timeout) must not share mutable
// state with the next Heartbeat, which rewrites nodeHealth.LastReported.
func TestHeartbeatSlowMirrorDoesNotRaceWithNextHeartbeat(t *testing.T) {
	c, _, _, _ := newControlWithMirrors(t)
	c.SetMirrorTimeout(20 * time.Millisecond)
	c.writeMirror = func(tg *target, nh *NodeHealth) (bool, error) {
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) {
			_ = nh.LastReported // what writeRegisterNode reads
			time.Sleep(time.Millisecond)
		}
		return true, nil
	}
	nh := NewNodeHealth()
	for i := 0; i < 10; i++ {
		c.Heartbeat(nh)
		time.Sleep(25 * time.Millisecond)
	}
}
