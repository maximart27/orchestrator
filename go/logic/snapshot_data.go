/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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
	"fmt"
	"io"
	"sort"

	"github.com/proxysql/orchestrator/go/inst"

	"github.com/proxysql/golib/log"
	"github.com/proxysql/golib/sqlutils"
	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// SnapshotFormatVersion is the version of the snapshot written by this code.
//
// Version 1 (no FormatVersion field) was the state of a single, implicit tenant:
// its fields were the top level fields of the snapshot. Version 2 holds the state
// of every tenant, by tenant ID. A version 1 snapshot is read as the state of the
// legacy tenant (orcraft.LegacyTenantID).
const SnapshotFormatVersion = 2

// TenantSnapshot is the part of a snapshot that belongs to one tenant: what its
// backend holds, as read from (and restored into) that backend only.
type TenantSnapshot struct {
	Keys             []inst.InstanceKey `json:",omitempty"` // Kept for backwards comapatibility
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
}

// SnapshotData is the content of a raft snapshot (format version 2): the state
// of every tenant, by ID, plus the node-level leader URI.
type SnapshotData struct {
	FormatVersion int
	LeaderURI     string
	Tenants       map[string]*TenantSnapshot
}

// snapshotDataV1 is the format that predates multi-tenancy.
type snapshotDataV1 struct {
	TenantSnapshot
	LeaderURI string
}

// snapshotTables are the tables of a tenant's backend that a snapshot carries,
// by the TenantSnapshot field that holds them.
func (ts *TenantSnapshot) tables() []struct {
	name string
	data *sqlutils.NamedResultData
} {
	return []struct {
		name string
		data *sqlutils.NamedResultData
	}{
		{"cluster_alias", &ts.ClusterAlias},
		{"cluster_alias_override", &ts.ClusterAliasOverride},
		{"cluster_domain_name", &ts.ClusterDomainName},
		{"access_token", &ts.AccessToken},
		{"host_attributes", &ts.HostAttributes},
		{"database_instance_tags", &ts.InstanceTags},
		{"database_instance_pool", &ts.PoolInstances},
		{"hostname_resolve", &ts.HostnameResolves},
		{"hostname_unresolve", &ts.HostnameUnresolves},
		{"database_instance_downtime", &ts.DowntimedInstances},
		{"candidate_database_instance", &ts.Candidates},
		{"topology_failure_detection", &ts.Detections},
		{"kv_store", &ts.KVStore},
		{"topology_recovery", &ts.Recovery},
		{"topology_recovery_steps", &ts.RecoverySteps},
		{"cluster_injected_pseudo_gtid", &ts.InjectedPseudoGTIDClusters},
	}
}

func NewSnapshotData() *SnapshotData {
	return &SnapshotData{FormatVersion: SnapshotFormatVersion, Tenants: make(map[string]*TenantSnapshot)}
}

// readTableData reads a whole table of the tenant's backend.
func (t *Tenant) readTableData(tableName string, data *sqlutils.NamedResultData) error {
	orcdb, err := t.Backend.Open()
	if err != nil {
		return log.Errore(err)
	}
	*data, err = sqlutils.ScanTable(orcdb, tableName)
	return log.Errore(err)
}

// writeTableData writes a table into the tenant's backend.
func (t *Tenant) writeTableData(tableName string, data *sqlutils.NamedResultData) error {
	orcdb, err := t.Backend.Open()
	if err != nil {
		return log.Errore(err)
	}
	err = sqlutils.WriteTable(orcdb, tableName, *data)
	return log.Errore(err)
}

// createSnapshot reads the state of the tenant from ITS backend. It fails as a
// whole if anything cannot be read: a partial snapshot would, once restored,
// make the nodes forget instances (or recoveries) that were merely unreadable.
func (t *Tenant) createSnapshot() (*TenantSnapshot, error) {
	ts := &TenantSnapshot{}
	var err error
	if ts.Keys, err = t.Store.ReadAllInstanceKeys(); err != nil {
		return nil, fmt.Errorf("reading the instance keys: %w", err)
	}
	if ts.MinimalInstances, err = t.Store.ReadAllMinimalInstances(); err != nil {
		return nil, fmt.Errorf("reading the instances: %w", err)
	}
	if ts.RecoveryDisabled, err = t.IsRecoveryDisabled(); err != nil {
		return nil, fmt.Errorf("reading the recovery disable flag: %w", err)
	}
	for _, table := range ts.tables() {
		if err := t.readTableData(table.name, table.data); err != nil {
			return nil, fmt.Errorf("reading table %s: %w", table.name, err)
		}
	}
	return ts, nil
}

// CreateSnapshotData reads the state of every tenant. A tenant whose backend
// cannot be read is left out of the snapshot (and the error logged): an empty
// entry would make the nodes that restore it forget the instances of the tenant.
func (m *TenantManager) CreateSnapshotData() *SnapshotData {
	snapshotData := NewSnapshotData()
	snapshotData.LeaderURI = orcraft.LeaderURI.Get()
	for _, t := range m.tenants {
		ts, err := t.createSnapshot()
		if err != nil {
			_ = log.Errorf("raft snapshot: tenant %s is omitted from the snapshot, its backend could not be read: %+v", t.ID, err)
			continue
		}
		snapshotData.Tenants[t.ID] = ts
	}
	log.Debugf("raft snapshot data created")
	return snapshotData
}

// SnapshotDataCreatorApplier creates and restores the raft snapshots of the tenants of a manager.
type SnapshotDataCreatorApplier struct {
	manager *TenantManager
}

// NewSnapshotDataCreatorApplier returns the snapshotter of manager's tenants.
// A nil manager is the default manager (see DefaultManager).
func NewSnapshotDataCreatorApplier(manager *TenantManager) *SnapshotDataCreatorApplier {
	return &SnapshotDataCreatorApplier{manager: manager}
}

func (s *SnapshotDataCreatorApplier) tenants() *TenantManager {
	if s.manager == nil {
		return DefaultManager()
	}
	return s.manager
}

func (s *SnapshotDataCreatorApplier) GetData() (data []byte, err error) {
	snapshotData := s.tenants().CreateSnapshotData()
	b, err := json.Marshal(snapshotData)
	if err != nil {
		return b, err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return b, err
	}
	if err := zw.Close(); err != nil {
		return b, err
	}
	return buf.Bytes(), nil
}

// decodeSnapshot reads a snapshot of any known format version as SnapshotData.
func decodeSnapshot(rc io.Reader) (*SnapshotData, error) {
	zr, err := gzip.NewReader(rc)
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	var version struct{ FormatVersion int }
	if err := json.Unmarshal(raw, &version); err != nil {
		return nil, err
	}
	switch {
	case version.FormatVersion == 0 || version.FormatVersion == 1:
		var v1 snapshotDataV1
		if err := json.Unmarshal(raw, &v1); err != nil {
			return nil, err
		}
		snapshotData := NewSnapshotData()
		snapshotData.LeaderURI = v1.LeaderURI
		snapshotData.Tenants[orcraft.LegacyTenantID] = &v1.TenantSnapshot
		return snapshotData, nil
	case version.FormatVersion == SnapshotFormatVersion:
		snapshotData := NewSnapshotData()
		if err := json.Unmarshal(raw, snapshotData); err != nil {
			return nil, err
		}
		if snapshotData.Tenants == nil {
			snapshotData.Tenants = make(map[string]*TenantSnapshot)
		}
		return snapshotData, nil
	}
	return nil, fmt.Errorf("unsupported raft snapshot format version %d (this node reads up to %d)", version.FormatVersion, SnapshotFormatVersion)
}

// Restore applies a snapshot, tenant by tenant, each into its own backend. A
// tenant of the snapshot this node has no runtime for is skipped with a warning;
// a local tenant that is not in the snapshot is left untouched -- in particular
// nothing is forgotten from it.
func (s *SnapshotDataCreatorApplier) Restore(rc io.ReadCloser) error {
	snapshotData, err := decodeSnapshot(rc)
	if err != nil {
		return err
	}
	manager := s.tenants()

	orcraft.LeaderURI.Set(snapshotData.LeaderURI)

	ids := make([]string, 0, len(snapshotData.Tenants))
	for id := range snapshotData.Tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		t, found := manager.Get(id)
		if !found {
			_ = log.Warningf("raft snapshot restore: tenant %s is in the snapshot but has no runtime on this node; skipping it", id)
			continue
		}
		t.restoreSnapshot(snapshotData.Tenants[id])
	}
	log.Debugf("raft snapshot restore applied")
	return nil
}

// restoreSnapshot restores the state of the tenant from its part of a snapshot.
func (t *Tenant) restoreSnapshot(snapshotData *TenantSnapshot) {
	// keys
	{
		snapshotInstanceKeyMap := inst.NewInstanceKeyMap()
		snapshotInstanceKeyMap.AddKeys(snapshotData.Keys)
		for _, minimalInstance := range snapshotData.MinimalInstances {
			snapshotInstanceKeyMap.AddKey(minimalInstance.Key)
		}

		discardedKeys := 0
		// Forget instances that were not in snapshot.
		// Guard: only forget an instance absent from the snapshot if it is ALSO stale
		// locally (not seen within UnseenInstanceForgetHours). A freshly discovered
		// instance may legitimately exist in our local backend but not yet be captured
		// in the (older) snapshot we are restoring; deleting it here races with discovery
		// and can wipe recent discoveries cluster-wide on restart/leader-change. Genuine
		// decommissions age out and are removed both here and by ForgetLongUnseenInstances(),
		// and explicit forgets arrive via the replicated "forget" command.
		existingKeys, _ := t.Store.ReadAllInstanceKeys()
		recentlySeenKeys, _ := t.Store.ReadRecentlySeenInstanceKeyMap(t.Cfg.UnseenInstanceForgetHours)
		for _, existingKey := range existingKeys {
			if snapshotInstanceKeyMap.HasKey(existingKey) {
				continue
			}
			if recentlySeenKeys.HasKey(existingKey) {
				log.Debugf("raft snapshot restore: tenant %s: retaining recently-seen instance %+v absent from snapshot", t.ID, existingKey)
				continue
			}
			_ = t.Store.ForgetInstance(&existingKey)
			discardedKeys++
		}
		log.Debugf("raft snapshot restore: tenant %s: discarded %+v keys", t.ID, discardedKeys)
		existingKeysMap := inst.NewInstanceKeyMap()
		existingKeysMap.AddKeys(existingKeys)

		// Discover instances that are in snapshot and not in our own database.
		// Instances that _are_ in our own database will self-discover. No need
		// to explicitly discover them.
		discoveredKeys := 0
		// v2: read keys + master keys
		for _, minimalInstance := range snapshotData.MinimalInstances {
			if !existingKeysMap.HasKey(minimalInstance.Key) {
				if err := t.Store.WriteInstance(minimalInstance.ToInstance(), false, nil); err == nil {
					discoveredKeys++
				} else {
					_ = log.Errore(err)
				}
			}
		}
		if len(snapshotData.MinimalInstances) == 0 {
			// v1: read keys (backwards support)
			for _, snapshotKey := range snapshotData.Keys {
				if !existingKeysMap.HasKey(snapshotKey) {
					snapshotKey := snapshotKey
					go func() {
						select {
						case t.snapshotDiscoveryKeys <- snapshotKey:
						case <-t.quit:
						}
					}()
					discoveredKeys++
				}
			}
		}
		log.Debugf("raft snapshot restore: tenant %s: discovered %+v keys", t.ID, discoveredKeys)
	}
	for _, table := range snapshotData.tables() {
		_ = t.writeTableData(table.name, table.data)
	}

	// recovery disable
	{
		_ = t.SetRecoveryDisabled(snapshotData.RecoveryDisabled)
	}
}

// CreateSnapshotData reads the state of every tenant of the default manager.
//
// Deprecated: use (*TenantManager).CreateSnapshotData.
func CreateSnapshotData() *SnapshotData {
	return DefaultManager().CreateSnapshotData()
}
