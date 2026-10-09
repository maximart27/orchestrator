package logic

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"testing"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/inst"
	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// preTenantSnapshotData is the snapshot of a version without multi-tenancy (upstream orchestrator,
// go/logic/snapshot_data.go), copied: what such a node writes and, above all, how it reads.
type preTenantSnapshotData struct {
	Keys             []inst.InstanceKey
	MinimalInstances []inst.MinimalInstance
	RecoveryDisabled bool

	ClusterAlias,
	ClusterAliasOverride,
	ClusterDomainName,
	HostAttributes,
	InstanceTags,
	AccessToken,
	PoolInstances,
	InjectedPseudoGTIDClusters,
	HostnameResolves,
	HostnameUnresolves,
	DowntimedInstances,
	Candidates,
	Detections,
	KVStore,
	Recovery,
	RecoverySteps sqlutils.NamedResultData

	LeaderURI string
}

func decodePreTenant(t *testing.T, data []byte) *preTenantSnapshotData {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &preTenantSnapshotData{}
	if err := json.NewDecoder(zr).Decode(snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// A legacy single-tenant deployment keeps writing the snapshot format that a node of a version
// without multi-tenancy reads: during a rolling upgrade that node can be sent this snapshot (or
// restore it from its own dir after a downgrade) and must find the state of the cluster in it.
func TestLegacySnapshotIsReadableByAPreMultiTenantNode(t *testing.T) {
	m, _ := newTestManager(t, []string{orcraft.LegacyTenantID}, nil)
	tn := mustTenant(t, m, orcraft.LegacyTenantID)
	tenantWriteInstance(t, tn, "db1", 3306)
	tenantWriteInstance(t, tn, "db2", 3306)
	tagCommand(t, NewCommandApplier(m), orcraft.LegacyTenantID, "db2", "rack", "r1")
	if err := tn.DisableRecovery(); err != nil {
		t.Fatal(err)
	}
	orcraft.LeaderURI.Set("http://leader:3000")

	data := snapshotOf(t, m)
	old := decodePreTenant(t, data)
	if len(old.MinimalInstances) != 2 {
		t.Errorf("a pre-multi-tenant reader sees %d instances, want 2", len(old.MinimalInstances))
	}
	if !old.RecoveryDisabled {
		t.Error("a pre-multi-tenant reader would re-enable the recoveries the cluster disabled")
	}
	if len(old.InstanceTags.Data) != 1 {
		t.Errorf("a pre-multi-tenant reader sees %d tags, want 1", len(old.InstanceTags.Data))
	}
	if old.LeaderURI != "http://leader:3000" {
		t.Errorf("leader URI = %q", old.LeaderURI)
	}

	// and it is still one snapshot for this version too
	snapshot, err := decodeSnapshot(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	legacy := snapshot.Tenants[orcraft.LegacyTenantID]
	if legacy == nil || len(legacy.MinimalInstances) != 2 || !legacy.RecoveryDisabled || snapshot.LeaderURI != "http://leader:3000" {
		t.Errorf("this version reads the legacy snapshot back wrongly: %+v", snapshot)
	}

	dst, _ := newTestManager(t, []string{orcraft.LegacyTenantID}, nil)
	restoreInto(t, dst, data)
	d := mustTenant(t, dst, orcraft.LegacyTenantID)
	if !tenantHasInstance(t, d, "db1", 3306) || !tenantHasInstance(t, d, "db2", 3306) {
		t.Error("restoring the legacy snapshot lost instances")
	}
	if n := countRows(t, d, "database_instance_tags"); n != 1 {
		t.Errorf("restoring the legacy snapshot: %d tags, want 1", n)
	}
}

// What a node of a version without multi-tenancy writes is restored as the state of the legacy tenant.
func TestSnapshotOfAPreMultiTenantNodeRestoresAsTheLegacyTenant(t *testing.T) {
	src, _ := newTestManager(t, []string{orcraft.LegacyTenantID}, nil)
	s := mustTenant(t, src, orcraft.LegacyTenantID)
	tenantWriteInstance(t, s, "db1", 3306)
	tagCommand(t, NewCommandApplier(src), orcraft.LegacyTenantID, "db1", "rack", "r1")
	if err := s.DisableRecovery(); err != nil {
		t.Fatal(err)
	}
	ts, err := s.createSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	old := preTenantSnapshotData{Keys: ts.Keys, MinimalInstances: ts.MinimalInstances, RecoveryDisabled: ts.RecoveryDisabled, InstanceTags: ts.InstanceTags, LeaderURI: "http://old-leader:3000"}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(old); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	dst, _ := newTestManager(t, []string{orcraft.LegacyTenantID}, nil)
	restoreInto(t, dst, buf.Bytes())
	d := mustTenant(t, dst, orcraft.LegacyTenantID)
	if !tenantHasInstance(t, d, "db1", 3306) {
		t.Error("the instance of the pre-multi-tenant snapshot was lost")
	}
	if n := countRows(t, d, "database_instance_tags"); n != 1 {
		t.Errorf("%d tags, want 1", n)
	}
	if disabled, err := d.IsRecoveryDisabled(); err != nil || !disabled {
		t.Errorf("recoveries disabled = %v (%v), want true", disabled, err)
	}
	if got := orcraft.LeaderURI.Get(); got != "http://old-leader:3000" {
		t.Errorf("leader URI = %q", got)
	}
}

// Version 2 is written as soon as there is any tenant other than the legacy one -- including a
// deployment with one tenant that is not called "default" -- and a pre-multi-tenant reader finds
// no state in it, which is why every node must be upgraded before a second tenant is enabled.
func TestSnapshotIsVersion2UnlessItIsTheLegacyTenantAlone(t *testing.T) {
	for _, ids := range [][]string{{"a"}, {orcraft.LegacyTenantID, "b"}} {
		m, _ := newTestManager(t, ids, nil)
		tenantWriteInstance(t, mustTenant(t, m, ids[0]), "db1", 3306)
		var raw struct {
			FormatVersion int
			Tenants       map[string]json.RawMessage
		}
		zr, err := gzip.NewReader(bytes.NewReader(snapshotOf(t, m)))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(zr)
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}
		if raw.FormatVersion != SnapshotFormatVersion || len(raw.Tenants) != len(ids) {
			t.Errorf("tenants %v: format version %d with %d tenants", ids, raw.FormatVersion, len(raw.Tenants))
		}
		if old := decodePreTenant(t, snapshotOf(t, m)); len(old.MinimalInstances) != 0 {
			t.Errorf("tenants %v: a pre-multi-tenant reader must not find the state of tenant %s at the top level", ids, ids[0])
		}
	}
}
