package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/proxysql/orchestrator/go/inst"
)

// With Raft enabled the untag is published (a direct delete would only change the leader's
// backend); the API answers the instances the applier reported.
func TestUntagIsPublishedThroughRaft(t *testing.T) {
	m := newTestManager(t, "a", "b")
	a := mustTenant(t, m, "a")
	writeTenantInstance(t, a, "db1", 3306, "c1")
	if err := a.Store.PutInstanceTag(&inst.InstanceKey{Hostname: "db1", Port: 3306}, &inst.Tag{TagName: "env", TagValue: "prod"}); err != nil {
		t.Fatal(err)
	}
	router := newTestRouter(m)

	type published struct {
		tenant, op string
		value      inst.InstanceUntag
	}
	var got []published
	oldEnabled, oldPublish := raftEnabled, publishTenantCommand
	t.Cleanup(func() { raftEnabled, publishTenantCommand = oldEnabled, oldPublish })
	raftEnabled = func() bool { return true }
	publishTenantCommand = func(tenantID, op string, value interface{}) (interface{}, error) {
		p := published{tenant: tenantID, op: op, value: value.(inst.InstanceUntag)}
		got = append(got, p)
		keys := inst.NewInstanceKeyMap()
		keys.AddKey(inst.InstanceKey{Hostname: "db1", Port: 3306})
		return keys, nil
	}

	for _, c := range []struct {
		url    string
		hasKey bool
	}{
		{"/api/tenants/a/untag/db1/3306/env", true},
		{"/api/tenants/a/untag-all/env/prod", false},
	} {
		got = nil
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, c.url, nil))
		var e envelope
		if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Code != "OK" {
			t.Fatalf("%s: %v %s", c.url, err, rec.Body.String())
		}
		if len(got) != 1 || got[0].tenant != "a" || got[0].op != "delete-instance-tag" || (got[0].value.Key != nil) != c.hasKey || got[0].value.T.TagName != "env" {
			t.Errorf("%s published %+v", c.url, got)
		}
		var details []inst.InstanceKey
		if err := json.Unmarshal(e.Details, &details); err != nil || len(details) != 1 {
			t.Errorf("%s: details %s", c.url, e.Details)
		}
	}
	// published, not applied locally: the follower-side apply is the applier's job
	if tagged, _ := a.Store.GetInstanceKeysByTag(&inst.Tag{TagName: "env"}); len(tagged.GetInstanceKeys()) != 1 {
		t.Errorf("the handler removed the tag locally instead of publishing the removal")
	}
}
