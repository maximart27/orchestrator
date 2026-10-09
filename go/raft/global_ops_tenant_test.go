package orcraft_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// Scenario 6: a global op that carries a tenant is rejected the same way by every node,
// whatever the op, and applies nothing. The entries are crafted by hand: PublishTenant
// Command refuses them before they reach the log (checked too).
func TestGlobalOpsWithTenantAreRejectedDeterministically(t *testing.T) {
	apps, fakes := newFakeApps(3, false, "a", "b")
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	c.WaitConverged(10 * time.Second)
	base := len(fakes[0].globalOps())
	globalOps := []string{
		orcraft.HeartbeatCommand, orcraft.AsyncSnapshotCommand, orcraft.RegisterNodeCommand, orcraft.LeaderURICommand,
		orcraft.RequestHealthReportCommand, orcraft.TenantSetCommand, orcraft.YieldCommand, orcraft.YieldHintCommand,
	}
	for _, op := range globalOps {
		// the publishing path refuses it
		if _, err := c.Publish(ld, "a", op, []byte(`"v"`)); !errors.Is(err, orcraft.ErrTenantOnGlobalCommand) {
			t.Errorf("%s with a tenant must be refused at publish, got %v", op, err)
		}
		// a crafted entry reaches the log of every node; every node must reject it
		raw, _ := json.Marshal(map[string]interface{}{"op": op, "value": []byte(`"v"`), "tenant": "a"})
		f := c.raftOf(ld).Apply(raw, 5*time.Second)
		if err := f.Error(); err != nil {
			t.Fatalf("%s: raft apply: %v", op, err)
		}
		resp, _ := f.Response().(error)
		if !errors.Is(resp, orcraft.ErrTenantOnGlobalCommand) {
			t.Errorf("crafted %s entry with tenant a: response %v, want ErrTenantOnGlobalCommand", op, f.Response())
		}
	}
	c.WaitConverged(10 * time.Second)
	for i, f := range fakes {
		if n := len(f.globalOps()); n != base {
			t.Errorf("node%d applied %d global ops that carried a tenant: %v", i, n-base, f.globalOps())
		}
	}
	// the same ops WITHOUT tenant are fine
	if _, err := c.GenericCommand(ld, orcraft.HeartbeatCommand, nil); err != nil {
		t.Errorf("tenant-less heartbeat: %v", err)
	}
}
