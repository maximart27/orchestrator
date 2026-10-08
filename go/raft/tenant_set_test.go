package orcraft_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	orcraft "github.com/proxysql/orchestrator/go/raft"
)

func TestDiffTenantSets(t *testing.T) {
	mk := func(m map[string]string) orcraft.TenantSet { return orcraft.NewTenantSet("n", m) }
	same := mk(map[string]string{"b": "mysql", "a": "postgresql"})
	if !orcraft.SameTenants(same, mk(map[string]string{"a": "postgresql", "b": "mysql"})) {
		t.Error("the order of declaration must not matter")
	}
	if got := same.Tenants[0].ID; got != "a" {
		t.Errorf("tenants are sorted by id, first is %s", got)
	}
	d := orcraft.DiffTenantSets(mk(map[string]string{"a": "mysql", "c": "mysql", "d": "mysql"}), mk(map[string]string{"a": "mysql", "b": "mysql", "d": "postgresql"}))
	if fmt.Sprint(d.MissingHere, d.ExtraHere, d.Different) != "[c] [b] [d]" {
		t.Errorf("diff: %v", d)
	}
	if orcraft.TenantHash("a", "mysql") == orcraft.TenantHash("a", "postgresql") || orcraft.TenantHash("ab", "") == orcraft.TenantHash("a", "b") {
		t.Error("the digest must tell tenants and providers apart")
	}
}

// oldNodeApp behaves like a node of a version that does not know the tenant-set op: the
// op reaches it as an unknown command and it answers with an error ("Unknown command op").
type oldNodeApp struct{ *fakeApp }

func (a oldNodeApp) ApplyCommand(op string, value []byte) interface{} {
	if op == orcraft.TenantSetCommand {
		return fmt.Errorf("Unknown command op: %s", op)
	}
	return a.fakeApp.ApplyCommand(op, value)
}

// A node that does not know tenant-set answers with an error and nothing else: it keeps
// applying the log, the cluster does not diverge and the publisher gets the error as
// the response, not a failure of the replication.
func TestTenantSetUnknownToAnOlderNodeIsHarmless(t *testing.T) {
	apps, fakes := newFakeApps(3, false, "a")
	apps[2] = oldNodeApp{fakes[2]}
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	set, _ := json.Marshal(orcraft.NewTenantSet("node0", map[string]string{"a": "mysql"}))
	for i := 0; i < 3; i++ {
		// when the older node is the leader, the error of its applier is the response to the publisher
		if _, err := c.GenericCommand(ld, orcraft.TenantSetCommand, set); err != nil && !(ld == 2 && strings.Contains(err.Error(), "Unknown command op")) {
			t.Fatalf("tenant-set: %v", err)
		}
		if _, err := c.Publish(ld, "a", "cat", fakeVal("log", fmt.Sprintf("c%d", i))); err != nil {
			t.Fatalf("cat: %v", err)
		}
	}
	c.WaitConverged(10 * time.Second)
	c.AssertIdentical("a")
	for i, f := range fakes {
		if got := f.get("a", "log"); got != "c0,c1,c2," {
			t.Errorf("node%d log = %q, commands after tenant-set must apply on every node", i, got)
		}
	}
	for i := 0; i < 2; i++ {
		if n := len(fakes[i].globalOps()); n < 3 {
			t.Errorf("node%d applied %d tenant-set announcements, want 3", i, n)
		}
	}
	if n := len(fakes[2].globalOps()); n != 0 {
		t.Errorf("the older node must not have applied tenant-set as a global op, applied %v", fakes[2].globalOps())
	}
	// the node keeps being a healthy member
	c.AssertSingleLeaderPerTerm()
}

// The leader publishes its tenant set through the group; a follower publishes nothing.
func TestPublishTenantSetOnlyByTheLeader(t *testing.T) {
	apps, fakes := newFakeApps(3, false, "a", "b")
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	c.WaitConverged(10 * time.Second)
	orcraft.SetTenantSetSource(func() map[string]string { return map[string]string{"a": "mysql", "b": "postgresql"} })
	t.Cleanup(func() { orcraft.SetTenantSetSource(nil) })

	restore := orcraft.SetGlobalStore(c.storeOf((ld + 1) % 3)) // a follower
	if err := orcraft.PublishTenantSet(); err != nil {
		t.Errorf("a follower must silently do nothing: %v", err)
	}
	restore()
	c.WaitConverged(10 * time.Second)
	for i, f := range fakes {
		if len(f.globalOps()) != 0 {
			t.Errorf("node%d: a follower published something: %v", i, f.globalOps())
		}
	}

	restore = orcraft.SetGlobalStore(c.storeOf(ld))
	defer restore()
	if got := orcraft.ThisNode(); got != fmt.Sprintf("node%d", ld) {
		t.Errorf("ThisNode = %q", got)
	}
	if err := orcraft.PublishTenantSet(); err != nil {
		t.Fatalf("leader PublishTenantSet: %v", err)
	}
	c.WaitConverged(10 * time.Second)
	for i, f := range fakes {
		ops := f.globalOps()
		if len(ops) != 1 || ops[0] != orcraft.TenantSetCommand {
			t.Errorf("node%d: global ops %v, want [tenant-set]", i, ops)
		}
	}
}
