package logic

import (
	"testing"

	"github.com/proxysql/orchestrator/go/inst"
)

func tagKeys(t *testing.T, tn *Tenant, name string) int {
	t.Helper()
	m, err := tn.Store.GetInstanceKeysByTag(&inst.Tag{TagName: name})
	if err != nil {
		t.Fatal(err)
	}
	return len(m.GetInstanceKeys())
}

// delete-instance-tag answers the instances it untagged and, without an instance (untag-all),
// removes the tag from every one of them -- in its own tenant only.
func TestApplierDeleteInstanceTag(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, nil)
	applier := NewCommandApplier(m)
	for _, tenant := range []string{"a", "b"} {
		for _, host := range []string{"db1", "db2"} {
			tagCommand(t, applier, tenant, host, "env", "prod")
		}
		tagCommand(t, applier, tenant, "db1", "keep", "")
	}
	a, b := mustTenant(t, m, "a"), mustTenant(t, m, "b")

	res := applier.ApplyTenantCommand("a", "delete-instance-tag", mustJSON(t, inst.InstanceUntag{
		Key: &inst.InstanceKey{Hostname: "db1", Port: 3306}, T: inst.Tag{TagName: "env"}}))
	untagged, ok := res.(*inst.InstanceKeyMap)
	if !ok || len(untagged.GetInstanceKeys()) != 1 {
		t.Fatalf("untag of one instance answered %#v", res)
	}
	if got := tagKeys(t, a, "env"); got != 1 {
		t.Errorf("tenant a keeps %d instances tagged env, want 1", got)
	}

	// untag-all: no instance in the payload
	res = applier.ApplyTenantCommand("a", "delete-instance-tag", mustJSON(t, inst.InstanceUntag{T: inst.Tag{TagName: "env", TagValue: "prod", HasValue: true}}))
	untagged, ok = res.(*inst.InstanceKeyMap)
	if !ok || len(untagged.GetInstanceKeys()) != 1 {
		t.Fatalf("untag-all answered %#v", res)
	}
	if got := tagKeys(t, a, "env"); got != 0 {
		t.Errorf("tenant a keeps %d instances tagged env after untag-all", got)
	}
	if got := tagKeys(t, a, "keep"); got != 1 {
		t.Errorf("untag-all of env removed another tag: keep on %d instances", got)
	}
	if got := tagKeys(t, b, "env"); got != 2 {
		t.Errorf("tenant b lost its tags: %d instances tagged env, want 2", got)
	}

	// an untag-all of a tag without a value is refused, never applied
	if res := applier.ApplyTenantCommand("b", "delete-instance-tag", mustJSON(t, inst.InstanceUntag{T: inst.Tag{TagName: "env"}})); res == nil {
		t.Error("untag-all of a valueless tag must be an error")
	} else if _, isErr := res.(error); !isErr {
		t.Errorf("untag-all of a valueless tag answered %#v", res)
	}
	if got := tagKeys(t, b, "env"); got != 2 {
		t.Errorf("tenant b has %d instances tagged env, want 2", got)
	}
}
