package orcraft_test

import (
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/inst"
)

// An untag (of one instance, and untag-all) published on the leader removes the tag on every
// node, in its tenant only, and the leader's answer lists the untagged instances.
func TestRealUntagIsReplicated(t *testing.T) {
	tenants := []string{"a", "b"}
	apps, reals := newRealApps(t, 3, tenants, tenants, realHosts...)
	c := newChaosCluster(t, apps, nil)
	ld := c.WaitLeader()
	tag := func(tenant, host, name, value string) {
		mustPublish(t, c, ld, tenant, "put-instance-tag", jsonOf(t, inst.InstanceTag{Key: inst.InstanceKey{Hostname: host, Port: 3306}, T: inst.Tag{TagName: name, TagValue: value}}))
	}
	for _, tenant := range tenants {
		for _, host := range []string{"db1", "db2", "db3"} {
			tag(tenant, host, "env", "prod")
		}
	}
	c.WaitConverged(30 * time.Second)
	count := func(node int, tenant string) int {
		m, err := reals[node].tenant(tenant).Store.GetInstanceKeysByTag(&inst.Tag{TagName: "env"})
		if err != nil {
			t.Fatal(err)
		}
		return len(m.GetInstanceKeys())
	}
	assertEverywhere := func(tenant string, want int) {
		t.Helper()
		c.WaitConverged(30 * time.Second)
		for i := range reals {
			if got := count(i, tenant); got != want {
				t.Errorf("node%d: tenant %s has %d instances tagged env, want %d", i, tenant, got, want)
			}
		}
	}

	resp, err := c.Publish(ld, "a", "delete-instance-tag", jsonOf(t, inst.InstanceUntag{Key: &inst.InstanceKey{Hostname: "db1", Port: 3306}, T: inst.Tag{TagName: "env"}}))
	if err != nil {
		t.Fatal(err)
	}
	if untagged, ok := resp.(*inst.InstanceKeyMap); !ok || len(untagged.GetInstanceKeys()) != 1 {
		t.Errorf("the untag answered %#v, want the one instance untagged", resp)
	}
	assertEverywhere("a", 2)
	assertEverywhere("b", 3)

	resp, err = c.Publish(ld, "a", "delete-instance-tag", jsonOf(t, inst.InstanceUntag{T: inst.Tag{TagName: "env", TagValue: "prod", HasValue: true}}))
	if err != nil {
		t.Fatal(err)
	}
	if untagged, ok := resp.(*inst.InstanceKeyMap); !ok || len(untagged.GetInstanceKeys()) != 2 {
		t.Errorf("the untag-all answered %#v, want 2 instances", resp)
	}
	assertEverywhere("a", 0)
	assertEverywhere("b", 3)
	c.AssertIdentical(tenants...)
}
