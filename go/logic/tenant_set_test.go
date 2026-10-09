package logic

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/config"
	orcraft "github.com/proxysql/orchestrator/go/raft"
	"github.com/rcrowley/go-metrics"
)

// tenantSetFixture is a manager of tenants a and b, identified as node n1, with the
// mismatch outcome and the yield seam under control.
type tenantSetFixture struct {
	m       *TenantManager
	leader  *testLeadership
	applier *CommandApplier
	yields  chan struct{}
	now     time.Time
}

func newTenantSetFixture(t *testing.T, policy string) *tenantSetFixture {
	t.Helper()
	f := &tenantSetFixture{yields: make(chan struct{}, 4), now: time.Unix(1_700_000_000, 0)}
	var leader *testLeadership
	f.m, leader = newTestManager(t, []string{"a", "b"}, nil)
	f.leader = leader
	f.m.base.RaftTenantSetMismatchPolicy = policy
	f.m.thisNode = func() string { return "n1" }
	f.m.now = func() time.Time { return f.now }
	f.applier = NewCommandApplier(f.m)
	prev := tenantSetYield
	tenantSetYield = func() error { f.yields <- struct{}{}; return nil }
	orcraft.SetTenantSetMismatch(false)
	t.Cleanup(func() {
		tenantSetYield = prev
		orcraft.SetTenantSetMismatch(false)
	})
	return f
}

func (f *tenantSetFixture) announce(t *testing.T, node string, tenants map[string]string) interface{} {
	t.Helper()
	b, err := json.Marshal(orcraft.NewTenantSet(node, tenants))
	if err != nil {
		t.Fatal(err)
	}
	return f.applier.ApplyCommand(orcraft.TenantSetCommand, b)
}

func (f *tenantSetFixture) yielded() bool {
	select {
	case <-f.yields:
		return true
	case <-time.After(200 * time.Millisecond):
		return false
	}
}

var (
	tenantsAB  = map[string]string{"a": "mysql", "b": "mysql"}
	tenantsABC = map[string]string{"a": "mysql", "b": "mysql", "c": "mysql"}
	tenantsA   = map[string]string{"a": "mysql"}
)

func TestTenantSetSameTenantsNoMismatch(t *testing.T) {
	f := newTenantSetFixture(t, config.TenantSetMismatchYield)
	if res := f.announce(t, "n2", tenantsAB); res != nil {
		t.Fatalf("tenant-set: %v", res)
	}
	if orcraft.TenantSetMismatch() || !f.m.TenantSetDiff().Empty() {
		t.Errorf("same tenants: mismatch=%v diff=%v", orcraft.TenantSetMismatch(), f.m.TenantSetDiff())
	}
	// ... and the leader announcing the same thing as the previous leader
	f.leader.leader.Store(true)
	f.announce(t, "n1", tenantsAB)
	if orcraft.TenantSetMismatch() || f.yielded() {
		t.Error("a leader with the same tenants must neither report a mismatch nor yield")
	}
}

func TestTenantSetMismatchDetected(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		leaderSet                     map[string]string
		missing, extra, differentWant []string
	}{
		{"this node has one tenant less", tenantsABC, []string{"c"}, nil, nil},
		{"this node has one tenant more", tenantsA, nil, []string{"b"}, nil},
		{"same tenant, another provider", map[string]string{"a": "mysql", "b": "postgresql"}, nil, nil, []string{"b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTenantSetFixture(t, config.TenantSetMismatchWarn)
			if res := f.announce(t, "n2", tc.leaderSet); res != nil {
				t.Fatalf("tenant-set must not fail the command: %v", res)
			}
			if !orcraft.TenantSetMismatch() {
				t.Fatal("tenant_set_mismatch must be 1")
			}
			d := f.m.TenantSetDiff()
			if strings.Join(d.MissingHere, ",") != strings.Join(tc.missing, ",") ||
				strings.Join(d.ExtraHere, ",") != strings.Join(tc.extra, ",") ||
				strings.Join(d.Different, ",") != strings.Join(tc.differentWant, ",") {
				t.Errorf("diff = %v, want missing=%v extra=%v different=%v", d, tc.missing, tc.extra, tc.differentWant)
			}
			// the mismatch clears when the leader announces the same tenants again
			f.announce(t, "n2", tenantsAB)
			if orcraft.TenantSetMismatch() {
				t.Error("tenant_set_mismatch must go back to 0 when the tenants match")
			}
		})
	}
}

// policy=warn: nothing but the report, even for a leader; policy=yield: a LEADER with
// different tenants gives the leadership up, a non-leader only warns.
func TestTenantSetMismatchPolicy(t *testing.T) {
	t.Run("warn never yields", func(t *testing.T) {
		f := newTenantSetFixture(t, config.TenantSetMismatchWarn)
		f.announce(t, "n2", tenantsABC)
		f.leader.leader.Store(true)
		f.announce(t, "n1", tenantsAB)
		if !orcraft.TenantSetMismatch() || f.yielded() {
			t.Error("warn: mismatch reported, no yield")
		}
	})
	t.Run("yield: a follower only warns", func(t *testing.T) {
		f := newTenantSetFixture(t, config.TenantSetMismatchYield)
		f.leader.leader.Store(false)
		f.announce(t, "n2", tenantsABC)
		if !orcraft.TenantSetMismatch() || f.yielded() {
			t.Error("yield policy on a non-leader: mismatch reported, no yield")
		}
	})
	t.Run("yield: the leader with different tenants cedes", func(t *testing.T) {
		f := newTenantSetFixture(t, config.TenantSetMismatchYield)
		f.announce(t, "n2", tenantsABC) // the previous leader announced three tenants
		f.leader.leader.Store(true)     // this node is elected and announces its own two
		f.announce(t, "n1", tenantsAB)
		if !orcraft.TenantSetMismatch() {
			t.Error("the elected node must report the mismatch")
		}
		if !f.yielded() {
			t.Error("yield policy: the leader with different tenants must yield")
		}
	})
	t.Run("yield: a leader that is not leader any more at apply time does not yield", func(t *testing.T) {
		f := newTenantSetFixture(t, config.TenantSetMismatchYield)
		f.announce(t, "n2", tenantsABC)
		f.leader.leader.Store(false)
		f.announce(t, "n1", tenantsAB)
		if f.yielded() {
			t.Error("must not yield without leadership")
		}
	})
	t.Run("yield: a stale baseline proves nothing", func(t *testing.T) {
		f := newTenantSetFixture(t, config.TenantSetMismatchYield)
		f.announce(t, "n2", tenantsABC)
		f.now = f.now.Add(foreignTenantSetTTL + time.Second)
		f.leader.leader.Store(true)
		f.announce(t, "n1", tenantsAB)
		if orcraft.TenantSetMismatch() || f.yielded() {
			t.Error("the set another node announced long ago must not make this leader yield")
		}
	})
	t.Run("yield: no baseline, nothing to compare", func(t *testing.T) {
		f := newTenantSetFixture(t, config.TenantSetMismatchYield)
		f.leader.leader.Store(true)
		f.announce(t, "n1", tenantsAB)
		if orcraft.TenantSetMismatch() || f.yielded() {
			t.Error("a leader that has seen nobody else announce has nothing to compare with")
		}
	})
}

func TestTenantSetIsGlobalAndMalformedIsHarmless(t *testing.T) {
	if !orcraft.IsGlobalOp(orcraft.TenantSetCommand) {
		t.Fatal("tenant-set must be a global op")
	}
	f := newTenantSetFixture(t, config.TenantSetMismatchWarn)
	// the applier must not treat it as a tenant-scoped (unknown) op
	if res := f.applier.ApplyTenantCommand(orcraft.LegacyTenantID, orcraft.TenantSetCommand, mustJSON(t, orcraft.NewTenantSet("n2", tenantsAB))); res != nil {
		t.Errorf("tenant-set through ApplyTenantCommand: %v", res)
	}
	if res, ok := f.applier.ApplyCommand(orcraft.TenantSetCommand, []byte(`{"tenants": 12`)).(error); !ok || res == nil {
		t.Error("a malformed tenant-set must be answered with an error, not panic or apply")
	}
}

// What is announced carries ids, providers and a digest, nothing else of the tenant.
func TestTenantSetCarriesNoCredentials(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, map[string]string{"a": `"MySQLTopologyPassword": "s3cr3t-topology-pw", "MySQLTopologyUser": "topo-user-x"`})
	b, err := json.Marshal(m.TenantSet())
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"s3cr3t-topology-pw", "topo-user-x", "sqlite3"} {
		if strings.Contains(string(b), secret) {
			t.Errorf("tenant-set payload leaks %q: %s", secret, b)
		}
	}
	// the digest only depends on id and provider: the same on every node whatever the credentials
	m2, _ := newTestManager(t, []string{"a"}, map[string]string{"a": `"MySQLTopologyPassword": "another"`})
	if !orcraft.SameTenants(m.TenantSet(), m2.TenantSet()) {
		t.Error("nodes that differ only in credentials run the same tenant set")
	}
}

// The count of commands a node could not apply (unknown tenant) is exported as a global metric.
func TestUnknownTenantCommandsMetricIsRegistered(t *testing.T) {
	registerNodeMetrics()
	g, ok := metrics.Get("raft.unknown_tenant_commands").(metrics.Gauge)
	if !ok {
		t.Fatal("raft.unknown_tenant_commands is not registered as a gauge")
	}
	if g.Value() != orcraft.UnknownTenantCommands() {
		t.Errorf("gauge = %d, want %d", g.Value(), orcraft.UnknownTenantCommands())
	}
}
