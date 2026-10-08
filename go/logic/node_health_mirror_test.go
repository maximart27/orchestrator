package logic

import (
	"context"
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"

	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/process"
)

func countRowsOf(t *testing.T, tn *Tenant, query string) int {
	t.Helper()
	n := -1
	if err := tn.Backend.Query(query, nil, func(m sqlutils.RowMap) error {
		n = m.GetInt("c")
		return nil
	}); err != nil {
		t.Fatalf("tenant %s: %s: %v", tn.ID, query, err)
	}
	return n
}

func beginMaintenance(t *testing.T, tn *Tenant) {
	t.Helper()
	if _, err := tn.Store.BeginMaintenance(&inst.InstanceKey{Hostname: "db1", Port: 3306}, "test", "mirror test"); err != nil {
		t.Fatalf("tenant %s: BeginMaintenance: %v", tn.ID, err)
	}
	if n := countRowsOf(t, tn, `select count(*) as c from database_instance_maintenance where maintenance_active = 1`); n != 1 {
		t.Fatalf("tenant %s: %d active maintenances, want 1", tn.ID, n)
	}
}

const activeMaintenances = `select count(*) as c from database_instance_maintenance where maintenance_active = 1`

// ExpireMaintenance deletes the maintenances of nodes that are not in node_health:
// with a mirror that has not (yet) got this node's heartbeat it would wipe the
// manual maintenances of the tenant. The tenant must not run it then.
func TestExpireMaintenanceWaitsForTheNodeHealthMirror(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, nil)
	a, b := mustTenant(t, m, "a"), mustTenant(t, m, "b")
	beginMaintenance(t, a)
	beginMaintenance(t, b)

	// premise: without the gate, an empty node_health makes the store wipe it
	if err := b.Store.ExpireMaintenance(); err != nil {
		t.Fatal(err)
	}
	if n := countRowsOf(t, b, activeMaintenances); n != 0 {
		t.Fatalf("premise: ExpireMaintenance with an empty node_health should delete the maintenance, %d left", n)
	}

	// the node has not heartbeaten into the mirror of a yet
	if a.nodeHealthMirrored() {
		t.Fatal("no heartbeat yet: the mirror must not count as fresh")
	}
	if err := a.expireMaintenance(); err != nil {
		t.Fatal(err)
	}
	if n := countRowsOf(t, a, activeMaintenances); n != 1 {
		t.Fatalf("the maintenance was deleted while the mirror had no heartbeat (%d left)", n)
	}

	// once the heartbeat has reached the mirror the node is alive in it
	if hb := m.Control().Heartbeat(process.ThisNodeHealth); !hb.Tenants["a"].OK() {
		t.Fatalf("heartbeat: %+v", hb.Tenants["a"])
	}
	if !a.nodeHealthMirrored() {
		t.Fatal("a fresh heartbeat: the mirror must count as fresh")
	}
	if err := a.expireMaintenance(); err != nil {
		t.Fatal(err)
	}
	if n := countRowsOf(t, a, activeMaintenances); n != 1 {
		t.Fatalf("the maintenance of a live node was expired (%d left)", n)
	}

	// the mirror falls behind (its node_health row purged, no new heartbeat)
	if _, err := a.Backend.Exec(`delete from node_health`); err != nil {
		t.Fatal(err)
	}
	prev := mirrorFreshness
	mirrorFreshness = 50 * time.Millisecond
	defer func() { mirrorFreshness = prev }()
	time.Sleep(100 * time.Millisecond)
	if err := a.expireMaintenance(); err != nil {
		t.Fatal(err)
	}
	if n := countRowsOf(t, a, activeMaintenances); n != 1 {
		t.Fatalf("the maintenance was deleted while the mirror was stale (%d left)", n)
	}
}

func TestAcknowledgeCrashedRecoveriesWaitsForTheNodeHealthMirror(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	a := mustTenant(t, m, "a")
	const active = `select count(*) as c from topology_recovery where in_active_period = 1`
	if _, err := a.Backend.Exec(`insert into topology_recovery
		(hostname, port, in_active_period, processing_node_hostname, processcing_node_token)
		values ('db1', 3306, 1, ?, ?)`, process.ThisNodeHealth.Hostname, process.ThisNodeHealth.Token); err != nil {
		t.Fatal(err)
	}
	if n, err := a.acknowledgeCrashedRecoveries(); err != nil || n != 0 {
		t.Fatalf("no heartbeat in the mirror: nothing may be acknowledged, got %d, %v", n, err)
	}
	if n := countRowsOf(t, a, active); n != 1 {
		t.Fatalf("the recovery of a live node was acknowledged while the mirror had no heartbeat")
	}
	m.Control().Heartbeat(process.ThisNodeHealth)
	if n, err := a.acknowledgeCrashedRecoveries(); err != nil || n != 0 {
		t.Fatalf("the node is in node_health: its recovery is not crashed, got %d, %v", n, err)
	}
	if n := countRowsOf(t, a, active); n != 1 {
		t.Fatalf("the recovery of a live node was acknowledged")
	}
}

// Start writes the first heartbeat into every tenant backend before any tenant
// starts its maintenance.
func TestStartWritesTheFirstHeartbeatBeforeTheTenantsRun(t *testing.T) {
	m, leader := newTestManager(t, []string{"a", "b"}, nil)
	leader.leader.Store(true)
	for _, tn := range m.All() {
		if n := countRowsOf(t, tn, `select count(*) as c from node_health`); n != 0 {
			t.Fatalf("tenant %s: node_health has %d rows before Start", tn.ID, n)
		}
		beginMaintenance(t, tn)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	for _, tn := range m.All() {
		if n := countRowsOf(t, tn, `select count(*) as c from node_health`); n != 1 {
			t.Errorf("tenant %s: node_health has %d rows right after Start, want 1", tn.ID, n)
		}
		if !tn.nodeHealthMirrored() {
			t.Errorf("tenant %s: the mirror must be fresh right after Start", tn.ID)
		}
	}
	// the just-became-leader expiry that the first health tick launches finds the node alive
	time.Sleep(1500 * time.Millisecond)
	for _, tn := range m.All() {
		if n := countRowsOf(t, tn, activeMaintenances); n != 1 {
			t.Errorf("tenant %s: %d maintenances after the first leader tick, want 1", tn.ID, n)
		}
	}
}
