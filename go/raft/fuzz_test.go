package orcraft

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

// Scenario 8: whatever bytes end up in a log entry, the FSM does not panic, never
// reaches the applier of a tenant it does not know, never applies a global op that
// carries a tenant, and routes tenant-less entries to the legacy tenant.
func checkFSMApplyRobust(t testing.TB, data []byte) {
	app := &tenantApplier{known: map[string]bool{LegacyTenantID: true, "a": true, "b-2": true}}
	f := &fsm{applier: app}
	resp := f.Apply(&raft.Log{Index: 1, Data: data})

	var c storeCommand
	decodes := json.Unmarshal(data, &c) == nil
	if !decodes {
		if err, ok := resp.(error); !ok || !errors.Is(err, ErrUndecodableCommand) {
			t.Fatalf("an entry that does not decode must be ErrUndecodableCommand, got %v (input %.80q)", resp, data)
		}
	}
	applied := app.get()
	for _, ap := range applied {
		if !decodes {
			t.Fatalf("applied something from an entry that does not decode: op=%s tenant=%q (input %.80q)", ap.Op, ap.Tenant, data)
		}
		if IsGlobalOp(ap.Op) { // global ops are applied by the node, without tenant
			if ap.Tenant != "" || ResolveTenant(c.Tenant) != LegacyTenantID {
				t.Fatalf("global op %s applied with tenant %q (entry tenant %q)", ap.Op, ap.Tenant, c.Tenant)
			}
			continue
		}
		if !app.known[ap.Tenant] {
			t.Fatalf("applied to a tenant that does not exist here: %q (input %.80q)", ap.Tenant, data)
		}
		if ap.Tenant != ResolveTenant(c.Tenant) {
			t.Fatalf("applied to tenant %q, the entry says %q (input %.80q)", ap.Tenant, ResolveTenant(c.Tenant), data)
		}
	}
	if decodes && IsGlobalOp(c.Op) && ResolveTenant(c.Tenant) != LegacyTenantID {
		if err, ok := resp.(error); !ok || !errors.Is(err, ErrTenantOnGlobalCommand) {
			t.Fatalf("global op with a tenant must be ErrTenantOnGlobalCommand, got %v (input %.80q)", resp, data)
		}
		if len(applied) != 0 {
			t.Fatalf("global op with a tenant was applied: %d entries", len(applied))
		}
	}
	if decodes && !IsGlobalOp(c.Op) && !app.known[ResolveTenant(c.Tenant)] {
		if err, ok := resp.(error); !ok || !errors.Is(err, ErrUnknownTenant) {
			t.Fatalf("unknown tenant must be ErrUnknownTenant, got %v (input %.80q)", resp, data)
		}
	}
}

func FuzzFSMApply(f *testing.F) {
	for _, seed := range []string{
		``, `{}`, `null`, `[]`, `"x"`, `{"op":`, `{"op":"discover","value":"IjEwLjAuMC4xIg=="}`,
		`{"op":"forget","value":"eA==","tenant":"a"}`,
		`{"op":"forget","tenant":"ghost"}`,
		`{"op":"heartbeat","tenant":"a"}`,
		`{"op":"yield","tenant":"a","value":"aG9zdDoxMjM="}`,
		`{"op":"yield-hint","value":"aG9zdA=="}`,
		`{"op":"","tenant":""}`,
		`{"op":"forget","tenant":"../../etc/passwd"}`,
		`{"op":"forget","tenant":"a\u0000b"}`,
		"{\"op\":\"forget\",\"tenant\":\"\xff\xfe\"}",
		`{"op":"forget","tenant":"b-2","value":"not base64!!"}`,
		`{"op":"forget","tenant":"a","unknown":{"deep":[1,2,3]}}`,
		`{"op":"put-key-value","tenant":"a","value":null}`,
		`{"Op":"forget","TENANT":"a"}`,
		`{"op":"forget","op":"heartbeat","tenant":"a","tenant":""}`,
		`{"op":"` + strings.Repeat("x", 1<<16) + `","tenant":"a"}`,
		`{"op":"forget","tenant":"` + strings.Repeat("t", 1<<16) + `"}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		checkFSMApplyRobust(t, data)
	})
}

func TestFSMApplyHugeAndWeirdValues(t *testing.T) {
	big := strings.Repeat("A", 1<<20) // 1 MiB value
	for _, c := range []storeCommand{
		{Op: "forget", Value: []byte(big), Tenant: "a"},
		{Op: "forget", Value: []byte(big)},
		{Op: "forget", Tenant: strings.Repeat("é", 100000)},
		{Op: "forget", Tenant: "a\x00b\n\t\"\\"},
		{Op: HeartbeatCommand, Value: []byte(big)},
		{Op: "forget", Tenant: "A"},             // case sensitive: not "a"
		{Op: "forget", Tenant: " a"},            // no trimming
		{Op: "forget", Tenant: "default"},       // explicit legacy
		{Op: "forget", Tenant: "DEFAULT"},       // not the legacy tenant
		{Op: "forget", Tenant: "_control"},      // reserved
		{Op: "forget", Tenant: "a", Value: nil}, // empty value
		{Op: "no-such-op", Tenant: "a"},
	} {
		data, err := json.Marshal(&c)
		if err != nil {
			t.Fatal(err)
		}
		checkFSMApplyRobust(t, data)
	}
}
