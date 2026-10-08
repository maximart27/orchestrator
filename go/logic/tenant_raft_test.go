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

package logic

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/proxysql/orchestrator/go/inst"
	orcraft "github.com/proxysql/orchestrator/go/raft"
)

func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestApplierRoutesCommandsToTheirTenant(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, nil)
	a, b := mustTenant(t, m, "a"), mustTenant(t, m, "b")
	applier := NewCommandApplier(m)
	var _ orcraft.TenantCommandApplier = applier
	var _ orcraft.CommandApplier = applier

	for _, tn := range []*Tenant{a, b} {
		tenantWriteInstance(t, tn, "db1", 3306)
		tenantWriteInstance(t, tn, "db2", 3306)
	}

	key := mustJSON(t, inst.InstanceKey{Hostname: "db1", Port: 3306})
	if res := applier.ApplyTenantCommand("a", "forget", key); res != nil {
		t.Fatalf("forget in a: %v", res)
	}
	if tenantHasInstance(t, a, "db1", 3306) || !tenantHasInstance(t, a, "db2", 3306) {
		t.Error("forget must remove the instance from tenant a")
	}
	if !tenantHasInstance(t, b, "db1", 3306) {
		t.Error("a command of tenant a was applied to tenant b")
	}

	// recoveries are switched off in the tenant of the command only
	if res := applier.ApplyTenantCommand("b", "disable-global-recoveries", nil); res != nil {
		t.Fatalf("disable in b: %v", res)
	}
	if disabled, _ := b.IsRecoveryDisabled(); !disabled {
		t.Error("disable-global-recoveries did not reach tenant b")
	}
	if disabled, _ := a.IsRecoveryDisabled(); disabled {
		t.Error("disable-global-recoveries of b disabled tenant a")
	}
	if res := applier.ApplyTenantCommand("b", "enable-global-recoveries", nil); res != nil {
		t.Fatalf("enable in b: %v", res)
	}
	if disabled, _ := b.IsRecoveryDisabled(); disabled {
		t.Error("enable-global-recoveries did not reach tenant b")
	}

	// key/value pairs go to the KV stores of the tenant
	if res := applier.ApplyTenantCommand("a", "put-key-value", mustJSON(t, map[string]string{"Key": "k", "Value": "v"})); res != nil {
		t.Fatalf("put-key-value: %v", res)
	}
	if _, found, _ := a.KV.GetValue("k"); !found {
		t.Error("put-key-value did not reach the KV store of a")
	}
	if _, found, _ := b.KV.GetValue("k"); found {
		t.Error("put-key-value reached the KV store of b")
	}

	// an unknown op is an error, not a panic
	if res := applier.ApplyTenantCommand("a", "no-such-op", nil); res == nil {
		t.Error("an unknown op must be an error")
	}
}

func TestApplierUnknownTenantIsAnErrorNotAPanic(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	applier := NewCommandApplier(m)
	res := applier.ApplyTenantCommand("ghost", "forget", mustJSON(t, inst.InstanceKey{Hostname: "db1", Port: 1}))
	err, ok := res.(error)
	if !ok || !errors.Is(err, orcraft.ErrUnknownTenant) {
		t.Fatalf("expected an error wrapping ErrUnknownTenant, got %#v", res)
	}
	// a legacy entry (no tenant) belongs to the tenant "default", which this node does not have
	res = applier.ApplyCommand("forget", mustJSON(t, inst.InstanceKey{Hostname: "db1", Port: 1}))
	if err, ok := res.(error); !ok || !errors.Is(err, orcraft.ErrUnknownTenant) {
		t.Fatalf("a legacy command without a default tenant must be ErrUnknownTenant, got %#v", res)
	}
}

func TestApplierLegacyCommandsGoToTheDefaultTenant(t *testing.T) {
	m, _ := newTestManager(t, []string{"default", "b"}, nil)
	def, b := mustTenant(t, m, "default"), mustTenant(t, m, "b")
	tenantWriteInstance(t, def, "db1", 3306)
	tenantWriteInstance(t, b, "db1", 3306)
	applier := NewCommandApplier(m)
	if res := applier.ApplyCommand("forget", mustJSON(t, inst.InstanceKey{Hostname: "db1", Port: 3306})); res != nil {
		t.Fatalf("forget: %v", res)
	}
	if tenantHasInstance(t, def, "db1", 3306) || !tenantHasInstance(t, b, "db1", 3306) {
		t.Error("an entry without tenant must be applied to the default tenant only")
	}
}

func TestApplierGlobalOpsAreHandledByTheNode(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	applier := NewCommandApplier(m)
	for _, op := range []string{orcraft.HeartbeatCommand, orcraft.RegisterNodeCommand} {
		if res := applier.ApplyCommand(op, nil); res != nil {
			t.Errorf("global op %s: %v", op, res)
		}
		// even if it reaches the tenant entry point
		if res := applier.ApplyTenantCommand("ghost", op, nil); res != nil {
			t.Errorf("global op %s through ApplyTenantCommand: %v", op, res)
		}
	}
	prev := orcraft.LeaderURI.Get()
	defer orcraft.LeaderURI.Set(prev)
	if res := applier.ApplyCommand(orcraft.LeaderURICommand, mustJSON(t, "http://leader:3000")); res != nil {
		t.Fatalf("leader-uri: %v", res)
	}
	if got := orcraft.LeaderURI.Get(); got != "http://leader:3000" {
		t.Errorf("leader-uri not applied: %q", got)
	}
}

func gzipJSON(t *testing.T, v interface{}) io.ReadCloser {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(mustJSON(t, v)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return io.NopCloser(&buf)
}

func TestSnapshotV2RoundTripsEveryTenant(t *testing.T) {
	src, _ := newTestManager(t, []string{"a", "b"}, nil)
	a, b := mustTenant(t, src, "a"), mustTenant(t, src, "b")
	tenantWriteInstance(t, a, "only-a", 3306)
	tenantWriteInstance(t, a, "shared", 3306)
	tenantWriteInstance(t, b, "only-b", 3306)
	tenantWriteInstance(t, b, "shared", 3306)
	if err := a.DisableRecovery(); err != nil {
		t.Fatal(err)
	}

	data, err := NewSnapshotDataCreatorApplier(src).GetData()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeSnapshot(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.FormatVersion != 2 || len(snapshot.Tenants) != 2 {
		t.Fatalf("snapshot: version=%d tenants=%d", snapshot.FormatVersion, len(snapshot.Tenants))
	}
	if n := len(snapshot.Tenants["a"].MinimalInstances); n != 2 {
		t.Errorf("tenant a: %d instances in the snapshot", n)
	}
	if !snapshot.Tenants["a"].RecoveryDisabled || snapshot.Tenants["b"].RecoveryDisabled {
		t.Error("the recovery flag is not per tenant in the snapshot")
	}

	dst, _ := newTestManager(t, []string{"a", "b"}, nil)
	da, db := mustTenant(t, dst, "a"), mustTenant(t, dst, "b")
	if err := NewSnapshotDataCreatorApplier(dst).Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		tn   *Tenant
		host string
		has  bool
	}{
		{da, "only-a", true}, {da, "shared", true}, {da, "only-b", false},
		{db, "only-b", true}, {db, "shared", true}, {db, "only-a", false},
	} {
		if got := tenantHasInstance(t, want.tn, want.host, 3306); got != want.has {
			t.Errorf("tenant %s: instance %s present=%v, want %v", want.tn.ID, want.host, got, want.has)
		}
	}
	if disabled, _ := da.IsRecoveryDisabled(); !disabled {
		t.Error("the recovery flag of a was not restored")
	}
	if disabled, _ := db.IsRecoveryDisabled(); disabled {
		t.Error("the recovery flag of a leaked to b")
	}
}

func TestSnapshotV1IsTheDefaultTenant(t *testing.T) {
	m, _ := newTestManager(t, []string{"default", "b"}, nil)
	def, b := mustTenant(t, m, "default"), mustTenant(t, m, "b")
	tenantWriteInstance(t, b, "keep-in-b", 3306)
	tenantSetLastSeen(t, b, "keep-in-b", 3306, 500)

	// what the former, single tenant code wrote: the state at the top level
	v1 := map[string]interface{}{
		"Keys":             []inst.InstanceKey{{Hostname: "from-v1", Port: 3306}},
		"MinimalInstances": []inst.MinimalInstance{{Key: inst.InstanceKey{Hostname: "from-v1", Port: 3306}}},
		"RecoveryDisabled": true,
		"LeaderURI":        "http://old-leader:3000",
	}
	prev := orcraft.LeaderURI.Get()
	defer orcraft.LeaderURI.Set(prev)
	if err := NewSnapshotDataCreatorApplier(m).Restore(gzipJSON(t, v1)); err != nil {
		t.Fatal(err)
	}
	if !tenantHasInstance(t, def, "from-v1", 3306) {
		t.Error("a v1 snapshot must be restored into the default tenant")
	}
	if tenantHasInstance(t, b, "from-v1", 3306) {
		t.Error("a v1 snapshot reached a tenant other than default")
	}
	if !tenantHasInstance(t, b, "keep-in-b", 3306) {
		t.Error("restoring the state of the default tenant must not forget instances of b")
	}
	if disabled, _ := def.IsRecoveryDisabled(); !disabled {
		t.Error("the v1 recovery flag was not restored")
	}
	if disabled, _ := b.IsRecoveryDisabled(); disabled {
		t.Error("the v1 recovery flag leaked to b")
	}
	if orcraft.LeaderURI.Get() != "http://old-leader:3000" {
		t.Error("the leader URI of a v1 snapshot was not restored")
	}
}

func TestSnapshotOmitsAnUnreadableTenant(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, nil)
	a, b := mustTenant(t, m, "a"), mustTenant(t, m, "b")
	tenantWriteInstance(t, a, "db1", 3306)
	tenantWriteInstance(t, b, "db1", 3306)
	_ = b.Backend.Close() // the backend of b is gone

	snapshot := m.CreateSnapshotData()
	if _, ok := snapshot.Tenants["b"]; ok {
		t.Error("a tenant whose backend cannot be read must be left out of the snapshot, never emitted empty")
	}
	if ts, ok := snapshot.Tenants["a"]; !ok || len(ts.MinimalInstances) != 1 {
		t.Errorf("the readable tenant must still be in the snapshot: %+v", ts)
	}
}

func TestSnapshotRestoreLeavesAbsentAndUnknownTenantsAlone(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, nil)
	a, b := mustTenant(t, m, "a"), mustTenant(t, m, "b")
	for _, tn := range []*Tenant{a, b} {
		tenantWriteInstance(t, tn, "stale", 3306)
		tenantSetLastSeen(t, tn, "stale", 3306, 1000)
	}
	snapshot := NewSnapshotData()
	snapshot.Tenants["a"] = &TenantSnapshot{}
	snapshot.Tenants["ghost"] = &TenantSnapshot{MinimalInstances: []inst.MinimalInstance{{Key: inst.InstanceKey{Hostname: "g", Port: 1}}}}
	if err := NewSnapshotDataCreatorApplier(m).Restore(gzipJSON(t, snapshot)); err != nil {
		t.Fatalf("a tenant without runtime must be skipped, not an error: %v", err)
	}
	if tenantHasInstance(t, a, "stale", 3306) {
		t.Error("tenant a is in the snapshot: its stale instance must be forgotten")
	}
	if !tenantHasInstance(t, b, "stale", 3306) {
		t.Error("tenant b is not in the snapshot: nothing may be forgotten from it")
	}
}

func TestSnapshotRestoreUsesTheForgetWindowOfEachTenant(t *testing.T) {
	m, _ := newTestManager(t, []string{"a", "b"}, map[string]string{
		"a": `"UnseenInstanceForgetHours": 240`,
		"b": `"UnseenInstanceForgetHours": 1`,
	})
	a, b := mustTenant(t, m, "a"), mustTenant(t, m, "b")
	for _, tn := range []*Tenant{a, b} {
		tenantWriteInstance(t, tn, "two-hours-old", 3306)
		tenantSetLastSeen(t, tn, "two-hours-old", 3306, 2)
	}
	snapshot := NewSnapshotData()
	snapshot.Tenants["a"] = &TenantSnapshot{}
	snapshot.Tenants["b"] = &TenantSnapshot{}
	if err := NewSnapshotDataCreatorApplier(m).Restore(gzipJSON(t, snapshot)); err != nil {
		t.Fatal(err)
	}
	if !tenantHasInstance(t, a, "two-hours-old", 3306) {
		t.Error("a keeps what it saw within its 240 hours")
	}
	if tenantHasInstance(t, b, "two-hours-old", 3306) {
		t.Error("b forgets what it did not see within its 1 hour")
	}
}

func TestSnapshotRejectsAFutureFormat(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	err := NewSnapshotDataCreatorApplier(m).Restore(gzipJSON(t, map[string]interface{}{"FormatVersion": 99}))
	if err == nil {
		t.Error("a snapshot of an unknown format version must be refused")
	}
}
