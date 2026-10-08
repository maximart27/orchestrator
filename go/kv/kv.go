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

package kv

import (
	"fmt"
	"sync"

	"github.com/proxysql/golib/log"
	"github.com/proxysql/orchestrator/go/config"
)

type KVPair struct {
	Key   string
	Value string
}

func NewKVPair(key string, value string) *KVPair {
	return &KVPair{Key: key, Value: value}
}

func (this *KVPair) String() string {
	return fmt.Sprintf("%s:%s", this.Key, this.Value)
}

type KVStore interface {
	PutKeyValue(key string, value string) (err error)
	PutKVPairs(kvPairs []*KVPair) (err error)
	GetKeyValue(key string) (value string, found bool, err error)
	DistributePairs(kvPairs [](*KVPair)) (err error)
}

// DefaultTenantID is the tenant ID of the legacy, single tenant stores.
const DefaultTenantID = "default"

// StoresOption customizes NewStores.
type StoresOption func(*storesOptions)

type storesOptions struct {
	backend Backend
}

// WithBackend sets the relational backend of the internal KV store (typically the
// tenant's own database backend). It is required for every tenant but the legacy
// DefaultTenantID, whose internal KV lives in the default backend; NewStores fails
// otherwise rather than silently writing in another tenant's database.
func WithBackend(backend Backend) StoresOption {
	return func(o *storesOptions) { o.backend = backend }
}

// Stores is the set of KV stores (internal, ZooKeeper, Consul) of one tenant.
// Each instance owns its own clients, ACL token and distribution re-entrancy guard,
// and reads the tenant's configuration (Consul*, Zk*, KVClusterMasterPrefix).
type Stores struct {
	tenantID string
	cfg      *config.Configuration
	stores   []KVStore
}

// NewStores creates the KV stores for a tenant. Unlike InitKVStores it can be called
// any number of times and does not touch package-level state.
func NewStores(cfg *config.Configuration, tenantID string, opts ...StoresOption) (*Stores, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kv: tenant %q: nil configuration", tenantID)
	}
	var o storesOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.backend == nil {
		if tenantID != DefaultTenantID {
			return nil, fmt.Errorf("kv: tenant %q: a backend is required (WithBackend)", tenantID)
		}
		o.backend = defaultBackend{}
	}

	var consul KVStore
	switch cfg.ConsulKVStoreProvider {
	case "consul-txn", "consul_txn":
		store, err := newConsulTxnStore(cfg)
		if err != nil {
			return nil, fmt.Errorf("kv: tenant %q: consul: %v", tenantID, err)
		}
		consul = store
	default:
		store, err := newConsulStore(cfg)
		if err != nil {
			return nil, fmt.Errorf("kv: tenant %q: consul: %v", tenantID, err)
		}
		consul = store
	}

	return &Stores{
		tenantID: tenantID,
		cfg:      cfg,
		stores: []KVStore{
			newInternalKVStore(o.backend),
			newZkStore(cfg),
			consul,
		},
	}, nil
}

// TenantID returns the tenant these stores belong to.
func (s *Stores) TenantID() string {
	return s.tenantID
}

// ClusterMasterKey returns the KV key of a cluster's master, using the tenant's KVClusterMasterPrefix.
func (s *Stores) ClusterMasterKey(clusterAlias string) string {
	return ClusterMasterKey(s.cfg.KVClusterMasterPrefix, clusterAlias)
}

// ClusterMasterKey returns the KV key of a cluster's master given the (normalized,
// see config.Configuration.postReadAdjustments) KVClusterMasterPrefix.
func ClusterMasterKey(prefix, clusterAlias string) string {
	return fmt.Sprintf("%s%s", prefix, clusterAlias)
}

// GetValue reads a key; only the first (internal) store matters here.
func (s *Stores) GetValue(key string) (value string, found bool, err error) {
	for _, store := range s.stores {
		return store.GetKeyValue(key)
	}
	return value, found, err
}

func (s *Stores) PutValue(key string, value string) (err error) {
	for _, store := range s.stores {
		if err := store.PutKeyValue(key, value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stores) PutKVPairs(kvPairs []*KVPair) (err error) {
	if len(kvPairs) < 1 {
		return nil
	}
	for _, store := range s.stores {
		if err := store.PutKVPairs(kvPairs); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stores) DistributePairs(kvPairs [](*KVPair)) (err error) {
	for _, store := range s.stores {
		if err := store.DistributePairs(kvPairs); err != nil {
			return err
		}
	}
	return nil
}

// Legacy (single tenant) package-level API: a shim over a default Stores built from config.Config.
var kvMutex sync.Mutex
var kvInitOnce sync.Once
var defaultStores *Stores

// InitKVStores initializes the default KV stores (duh), once in the lifetime of this app.
// Configuration reload does not affect a running instance.
func InitKVStores() {
	kvMutex.Lock()
	defer kvMutex.Unlock()

	kvInitOnce.Do(func() {
		stores, err := NewStores(config.Config, DefaultTenantID)
		if err != nil {
			// legacy behavior: a failing Consul client is logged and the store becomes a no-op
			_ = log.Errore(err)
			var consul KVStore
			switch config.Config.ConsulKVStoreProvider {
			case "consul-txn", "consul_txn":
				consul = NewConsulTxnStore()
			default:
				consul = NewConsulStore()
			}
			stores = &Stores{
				tenantID: DefaultTenantID,
				cfg:      config.Config,
				stores:   []KVStore{NewInternalKVStore(), NewZkStore(), consul},
			}
		}
		defaultStores = stores
	})
}

// DefaultStores returns the stores created by InitKVStores, or nil if it has not run yet.
func DefaultStores() *Stores {
	kvMutex.Lock()
	defer kvMutex.Unlock()
	return defaultStores
}

func GetValue(key string) (value string, found bool, err error) {
	if s := DefaultStores(); s != nil {
		return s.GetValue(key)
	}
	return value, found, err
}

func PutValue(key string, value string) (err error) {
	if s := DefaultStores(); s != nil {
		return s.PutValue(key, value)
	}
	return nil
}

func PutKVPairs(kvPairs []*KVPair) (err error) {
	if s := DefaultStores(); s != nil {
		return s.PutKVPairs(kvPairs)
	}
	return nil
}

func DistributePairs(kvPairs [](*KVPair)) (err error) {
	if s := DefaultStores(); s != nil {
		return s.DistributePairs(kvPairs)
	}
	return nil
}
