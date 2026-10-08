/*
   Copyright 2024 Orchestrator Authors

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

package inst

import (
	"errors"

	"github.com/proxysql/golib/log"
	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/config"
)

// MySQLProvider implements DatabaseProvider for MySQL and MySQL-compatible
// databases (MariaDB, Percona Server, etc.). It delegates to the existing
// orchestrator DAO functions.
type MySQLProvider struct {
	cfg   *config.Configuration
	store *Store
}

// NewMySQLProvider creates a MySQL database provider for the tenant configuration
// cfg (without one, the pre-multi-tenant signature, the legacy global
// configuration). A Store binds itself to the providers it creates (see
// NewStore); an unbound provider can only answer ProviderName.
func NewMySQLProvider(cfg ...*config.Configuration) *MySQLProvider {
	if len(cfg) == 0 || cfg[0] == nil {
		return &MySQLProvider{cfg: legacyConfig()}
	}
	return &MySQLProvider{cfg: cfg[0]}
}

func (p *MySQLProvider) boundStore() (*Store, error) {
	if p.store == nil {
		return nil, errors.New("MySQLProvider is not bound to a Store")
	}
	return p.store, nil
}

// ProviderName returns "mysql".
func (p *MySQLProvider) ProviderName() string {
	return "mysql"
}

// GetReplicationStatus retrieves the replication state for a MySQL instance
// by reading the topology and mapping it to a database-agnostic ReplicationStatus.
func (p *MySQLProvider) GetReplicationStatus(key InstanceKey) (*ReplicationStatus, error) {
	store, err := p.boundStore()
	if err != nil {
		return nil, log.Errore(err)
	}
	instance, err := store.ReadTopologyInstance(&key)
	if err != nil {
		return nil, log.Errore(err)
	}

	lag := int64(-1)
	if instance.SecondsBehindMaster.Valid {
		lag = instance.SecondsBehindMaster.Int64
	}

	position := instance.ExecutedGtidSet
	if position == "" {
		position = instance.SelfBinlogCoordinates.DisplayString()
	}

	return &ReplicationStatus{
		ReplicaRunning:   instance.ReplicaRunning(),
		SQLThreadRunning: instance.ReplicationSQLThreadRuning,
		IOThreadRunning:  instance.ReplicationIOThreadRuning,
		Position:         position,
		Lag:              lag,
	}, nil
}

// IsReplicaRunning checks whether replication is actively running on the
// given MySQL instance.
func (p *MySQLProvider) IsReplicaRunning(key InstanceKey) (bool, error) {
	store, err := p.boundStore()
	if err != nil {
		return false, log.Errore(err)
	}
	instance, err := store.ReadTopologyInstance(&key)
	if err != nil {
		return false, log.Errore(err)
	}
	return instance.ReplicaRunning(), nil
}

// SetReadOnly sets or clears the global read_only variable on a MySQL instance.
// This delegates to the existing SetReadOnly function.
func (p *MySQLProvider) SetReadOnly(key InstanceKey, readOnly bool) error {
	store, err := p.boundStore()
	if err != nil {
		return err
	}
	_, err = store.SetReadOnly(&key, readOnly)
	return err
}

// IsReadOnly checks whether a MySQL instance has global read_only enabled.
func (p *MySQLProvider) IsReadOnly(key InstanceKey) (bool, error) {
	store, err := p.boundStore()
	if err != nil {
		return false, log.Errore(err)
	}
	sqlDB, err := store.backend.OpenTopology(key.Hostname, key.Port)
	if err != nil {
		return false, log.Errore(err)
	}
	var readOnly bool
	err = sqlutils.QueryRowsMap(sqlDB, "SELECT @@global.read_only AS read_only", func(m sqlutils.RowMap) error {
		readOnly = m.GetBool("read_only")
		return nil
	})
	if err != nil {
		return false, log.Errore(err)
	}
	return readOnly, nil
}

// StartReplication starts the replication threads on the given MySQL instance.
// This delegates to the existing StartReplication function.
func (p *MySQLProvider) StartReplication(key InstanceKey) error {
	store, err := p.boundStore()
	if err != nil {
		return err
	}
	_, err = store.StartReplication(&key)
	return err
}

// StopReplication stops the replication threads on the given MySQL instance.
// This delegates to the existing StopReplication function.
func (p *MySQLProvider) StopReplication(key InstanceKey) error {
	store, err := p.boundStore()
	if err != nil {
		return err
	}
	_, err = store.StopReplication(&key)
	return err
}

// Compile-time check that MySQLProvider implements DatabaseProvider.
var _ DatabaseProvider = (*MySQLProvider)(nil)
