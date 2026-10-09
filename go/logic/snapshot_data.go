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
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

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

// snapshotTable is a table of a tenant's backend that a snapshot carries, with the
// TenantSnapshot field that holds it and how it is restored.
type snapshotTable struct {
	name string
	data *sqlutils.NamedResultData
	// authoritative tables are written by Raft commands only: the snapshot is their
	// whole content, so restoring one deletes what it does not hold (a row the leader
	// deleted while this node was down would otherwise stay here forever: nothing
	// replays a delete that is behind the snapshot). The others are also written by
	// the node itself and are only added to (REPLACE), never emptied.
	authoritative bool
}

// tables lists the tables of a tenant's backend that a snapshot carries.
//
// Classification (who writes the table; "applier" is CommandApplier, i.e. Raft):
//
//	table                         writers                                                       restore
//	cluster_alias                 local: UpdateClusterAliases on every tick, from the           additive
//	                              discovered instances; ReplaceAliasClusterName
//	cluster_alias_override        applier: set-cluster-alias-manual-override                    authoritative
//	cluster_domain_name           local: discovery (ReadTopologyInstance), local expiry         additive
//	access_token                  node control data (process.Control), not tenant data; in a    additive (NEVER
//	                              legacy setup it shares the backend of the default tenant      emptied: it would
//	                                                                                            wipe this node's
//	                                                                                            API tokens)
//	host_attributes               local: HTTP API on the node that receives the call, no        additive
//	                              Raft command
//	database_instance_tags        applier: put-/delete-instance-tag                             authoritative
//	database_instance_pool        applier: submit-pool-instances (+ time based expiry)          authoritative
//	hostname_resolve              local: hostname resolution of each node                       additive
//	hostname_unresolve            applier: register-hostname-unresolve (+ time based expiry)    authoritative
//	database_instance_downtime    applier: begin-/end-downtime (+ time based expiry)            authoritative
//	candidate_database_instance   applier: register-candidate (+ time based expiry)             authoritative
//	topology_failure_detection    applier: register-failure-detection, ack-recovery             authoritative
//	kv_store                      applier: put-key-value                                        authoritative
//	topology_recovery             applier: write-/resolve-/ack-recovery                         authoritative
//	topology_recovery_steps       applier: write-recovery-step                                  authoritative
//	cluster_injected_pseudo_gtid  applier: injected-pseudo-gtid (+ time based expiry)           authoritative
//
// database_instance (the instances, MinimalInstances) is also written by the local
// discovery and is reconciled apart, by the "keys" block of restoreSnapshot (guard of PR #124). The
// time based expiry runs on every node over rows the leader's snapshot already
// reflects, so it does not make a table "mixed".
func (ts *TenantSnapshot) tables() []snapshotTable {
	return []snapshotTable{
		{"cluster_alias", &ts.ClusterAlias, false},
		{"cluster_alias_override", &ts.ClusterAliasOverride, true},
		{"cluster_domain_name", &ts.ClusterDomainName, false},
		{"access_token", &ts.AccessToken, false},
		{"host_attributes", &ts.HostAttributes, false},
		{"database_instance_tags", &ts.InstanceTags, true},
		{"database_instance_pool", &ts.PoolInstances, true},
		{"hostname_resolve", &ts.HostnameResolves, false},
		{"hostname_unresolve", &ts.HostnameUnresolves, true},
		{"database_instance_downtime", &ts.DowntimedInstances, true},
		{"candidate_database_instance", &ts.Candidates, true},
		{"topology_failure_detection", &ts.Detections, true},
		{"kv_store", &ts.KVStore, true},
		{"topology_recovery", &ts.Recovery, true},
		{"topology_recovery_steps", &ts.RecoverySteps, true},
		{"cluster_injected_pseudo_gtid", &ts.InjectedPseudoGTIDClusters, true},
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
	if err != nil {
		return log.Errore(err)
	}
	if t.Backend.IsSQLite() {
		// The SQLite driver hands DATETIME/TIMESTAMP columns over as time.Time, which a snapshot
		// would carry as RFC3339 text: write them the way SQLite stores them.
		timeColumns, err := backendTimeColumns(orcdb, true, tableName)
		if err != nil {
			return log.Errore(err)
		}
		normalizeTimestamps(data, timeColumns)
	}
	return nil
}

// backendTimeColumns lists the DATETIME/TIMESTAMP columns of a table of a tenant's backend.
func backendTimeColumns(orcdb *sql.DB, sqlite bool, tableName string) (map[string]bool, error) {
	columns := make(map[string]bool)
	var rows *sql.Rows
	var err error
	if sqlite {
		rows, err = orcdb.Query(fmt.Sprintf("pragma table_info(%s)", tableName))
	} else {
		rows, err = orcdb.Query(`select column_name, data_type from information_schema.columns
			where table_schema = database() and table_name = ?`, tableName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	isTime := func(declaredType string) bool {
		switch strings.ToLower(declaredType) {
		case "timestamp", "datetime":
			return true
		}
		return false
	}
	for rows.Next() {
		var name, declaredType string
		if sqlite {
			var cid, notNull, pk int
			var dflt sql.NullString
			if err := rows.Scan(&cid, &name, &declaredType, &notNull, &dflt, &pk); err != nil {
				return nil, err
			}
		} else if err := rows.Scan(&name, &declaredType); err != nil {
			return nil, err
		}
		if isTime(declaredType) {
			columns[strings.ToLower(name)] = true
		}
	}
	return columns, rows.Err()
}

// normalizeTimestamps rewrites the RFC3339 timestamps of the given columns (which is how the SQLite
// driver renders a time column, and so how a snapshot carries it) to the text the backends store and
// compare: "2006-01-02 15:04:05[.ffffff]", in UTC (what SQLite's datetime('now') writes and what its
// driver reads a stored time as). Left as RFC3339, a value would sort after every stored one ('T' >
// ' ') and the SQL comparisons against NOW() would never be true. Other values are left alone.
func normalizeTimestamps(data *sqlutils.NamedResultData, timeColumns map[string]bool) {
	for i, column := range data.Columns {
		if !timeColumns[strings.ToLower(column)] {
			continue
		}
		for _, row := range data.Data {
			if i >= len(row) || !row[i].Valid {
				continue
			}
			if parsed, err := time.Parse(time.RFC3339Nano, row[i].String); err == nil {
				row[i].String = parsed.UTC().Format(sqlutils.DateTimeFormat)
			}
		}
	}
}

// writeTables writes the tables of a snapshot into the tenant's backend in ONE
// transaction: if anything fails nothing of it is applied. A table the snapshot
// does not carry (no columns: e.g. a snapshot of an older version) is left alone.
func (t *Tenant) writeTables(tables []snapshotTable) error {
	return t.Store.ExecDBWriteFunc(func() error {
		orcdb, err := t.Backend.Open()
		if err != nil {
			return err
		}
		for _, table := range tables {
			// a snapshot written by a node of this code or of an older one may carry RFC3339 timestamps
			timeColumns, err := backendTimeColumns(orcdb, t.Backend.IsSQLite(), table.name)
			if err != nil {
				return fmt.Errorf("table %s: %w", table.name, err)
			}
			normalizeTimestamps(table.data, timeColumns)
		}
		tx, err := orcdb.Begin()
		if err != nil {
			return err
		}
		for _, table := range tables {
			if err := writeTableTx(tx, table); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("table %s: %w", table.name, err)
			}
		}
		return tx.Commit()
	})
}

func writeTableTx(tx *sql.Tx, table snapshotTable) error {
	data := table.data
	if len(data.Columns) == 0 {
		return nil
	}
	if table.authoritative {
		if _, err := tx.Exec(fmt.Sprintf("delete from %s", table.name)); err != nil {
			return err
		}
	}
	if len(data.Data) == 0 {
		return nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(data.Columns)), ",")
	query := fmt.Sprintf("replace into %s (%s) values (%s)", table.name, strings.Join(data.Columns, ","), placeholders)
	for _, row := range data.Data {
		if _, err := tx.Exec(query, row.Args()...); err != nil {
			return err
		}
	}
	return nil
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

// forEachTenantConcurrently runs f for every tenant, each in its own goroutine (their backends are
// independent), and waits for all of them.
func (m *TenantManager) forEachTenantConcurrently(f func(i int, t *Tenant)) {
	var wg sync.WaitGroup
	for i, t := range m.tenants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f(i, t)
		}()
	}
	wg.Wait()
}

// readSnapshots reads the state of every tenant, concurrently. A tenant whose backend cannot be read
// has a nil entry (and the error is logged): an empty entry would make the nodes that restore the
// snapshot forget the instances of the tenant.
func (m *TenantManager) readSnapshots() []*TenantSnapshot {
	snapshots := make([]*TenantSnapshot, len(m.tenants))
	m.forEachTenantConcurrently(func(i int, t *Tenant) {
		ts, err := t.createSnapshot()
		if err != nil {
			_ = log.Errorf("raft snapshot: tenant %s is omitted from the snapshot, its backend could not be read: %+v", t.ID, err)
			return
		}
		snapshots[i] = ts
	})
	return snapshots
}

// CreateSnapshotData reads the state of every tenant. A tenant whose backend
// cannot be read is left out of the snapshot (and the error logged): an empty
// entry would make the nodes that restore it forget the instances of the tenant.
func (m *TenantManager) CreateSnapshotData() *SnapshotData {
	snapshotData := NewSnapshotData()
	snapshotData.LeaderURI = orcraft.LeaderURI.Get()
	for i, ts := range m.readSnapshots() {
		if ts != nil {
			snapshotData.Tenants[m.tenants[i].ID] = ts
		}
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

func gzipped(b ...[]byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	for _, chunk := range b {
		if _, err := zw.Write(chunk); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// isLegacySingleTenant tells whether the manager holds exactly the legacy tenant: a single-tenant
// deployment with no tenants file, where every node of the Raft group may still be running a version
// without multi-tenancy.
func (m *TenantManager) isLegacySingleTenant() bool {
	return len(m.tenants) == 1 && m.tenants[0].ID == orcraft.LegacyTenantID
}

// GetData reads the state of every tenant and encodes it (gzip of a SnapshotData in JSON).
//
// The FSM is stopped while this runs (the snapshot is the state at a point of the log), so it is
// concurrent per tenant: the tenants are read and marshalled each in its own goroutine and the
// pieces are compressed concurrently too, as the members of a multi-member gzip stream, which is
// still one gzip stream that every reader of a snapshot (decodeSnapshot, older nodes) decodes.
//
// A legacy single-tenant deployment is written in format version 1 (no FormatVersion, the tables
// of the tenant at the top level, exactly what a version without multi-tenancy writes and reads):
// a node of an older version that restores it -- InstallSnapshot from this leader, or its own
// snapshot after a downgrade -- would otherwise find no table in a version 2 snapshot and, for
// one, re-enable recoveries that the cluster has disabled. Version 2 is written as soon as
// there is any other tenant, which an older node cannot serve anyway.
func (s *SnapshotDataCreatorApplier) GetData() (data []byte, err error) {
	manager := s.tenants()
	leaderURI, err := json.Marshal(orcraft.LeaderURI.Get())
	if err != nil {
		return nil, err
	}
	snapshots := manager.readSnapshots()
	marshalled := make([][]byte, len(snapshots))
	errs := make([]error, len(snapshots))
	manager.forEachTenantConcurrently(func(i int, t *Tenant) {
		if snapshots[i] == nil {
			return
		}
		body, err := json.Marshal(snapshots[i])
		if err != nil {
			errs[i] = err
			return
		}
		if manager.isLegacySingleTenant() {
			marshalled[i] = body
			return
		}
		id, err := json.Marshal(t.ID)
		if err != nil {
			errs[i] = err
			return
		}
		marshalled[i] = append(append(id, ':'), body...)
	})
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	var pieces [][]byte
	if manager.isLegacySingleTenant() && marshalled[0] != nil {
		// {"LeaderURI":...,<the fields of the tenant>}: the body is an object whose first byte is its '{'
		pieces = [][]byte{[]byte(fmt.Sprintf(`{"LeaderURI":%s,`, leaderURI)), marshalled[0][1:]}
	} else {
		// {"FormatVersion":2,"LeaderURI":...,"Tenants":{<id>:<state>,<id>:<state>}}
		pieces = [][]byte{[]byte(fmt.Sprintf(`{"FormatVersion":%d,"LeaderURI":%s,"Tenants":{`, SnapshotFormatVersion, leaderURI))}
		for _, piece := range marshalled {
			if piece == nil {
				continue
			}
			if len(pieces) > 1 {
				piece = append([]byte{','}, piece...)
			}
			pieces = append(pieces, piece)
		}
		pieces = append(pieces, []byte(`}}`))
	}

	members := make([][]byte, len(pieces))
	memberErrs := make([]error, len(pieces))
	var wg sync.WaitGroup
	for i := range pieces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			members[i], memberErrs[i] = gzipped(pieces[i])
		}()
	}
	wg.Wait()
	var out bytes.Buffer
	for i, member := range members {
		if memberErrs[i] != nil {
			return nil, memberErrs[i]
		}
		out.Write(member)
	}
	return out.Bytes(), nil
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
func (t *Tenant) restoreSnapshot(snapshotData *TenantSnapshot) error {
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
	tablesErr := t.writeTables(snapshotData.tables())
	if tablesErr != nil {
		_ = log.Errorf("raft snapshot restore: tenant %s: the tables were left as they were, restoring them failed: %+v", t.ID, tablesErr)
	}

	// recovery disable
	{
		_ = t.SetRecoveryDisabled(snapshotData.RecoveryDisabled)
	}
	return tablesErr
}

// CreateSnapshotData reads the state of every tenant of the default manager.
//
// Deprecated: use (*TenantManager).CreateSnapshotData.
func CreateSnapshotData() *SnapshotData {
	return DefaultManager().CreateSnapshotData()
}
