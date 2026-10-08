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
	"context"
	"sync"
	"sync/atomic"

	"github.com/proxysql/golib/log"

	"github.com/proxysql/orchestrator/go/config"
)

// This file bridges the pre-multi-tenant API of the package (package-level
// functions, *_compat.go) onto the tenant of the legacy single-tenant setup.

var (
	defaultManager  atomic.Pointer[TenantManager]
	legacyManagerMu sync.Mutex
)

// SetDefaultManager installs the TenantManager the package-level legacy
// functions resolve their tenant in. It is meant to be called once at startup.
//
// Deprecated: tenant-aware code resolves its tenant in its own TenantManager.
func SetDefaultManager(m *TenantManager) {
	defaultManager.Store(m)
}

// DefaultManager returns the manager installed by SetDefaultManager or, until
// then, a legacy manager built lazily from config.Config: a single tenant
// "default" over the process-wide defaults. The lazy manager is never started
// (it only serves the functions that operate on the backend) and must not be
// built before the configuration is loaded.
//
// Deprecated: tenant-aware code resolves its tenant in its own TenantManager.
func DefaultManager() *TenantManager {
	if m := defaultManager.Load(); m != nil {
		return m
	}
	legacyManagerMu.Lock()
	defer legacyManagerMu.Unlock()
	if m := defaultManager.Load(); m != nil {
		return m
	}
	provider, err := config.NormalizeProvider(config.Config.ProviderType)
	if err != nil {
		// as the legacy store always did: an unknown provider type is MySQL
		provider = config.ProviderMySQL
	}
	m, err := NewTenantManagerFor(config.Config, []*config.TenantConfig{
		{ID: config.DefaultTenantID, Provider: provider, Cfg: config.Config},
	}, WithoutProcessBootstrap())
	if err != nil {
		log.Fatalf("logic: cannot build the default tenant: %v", err)
	}
	defaultManager.Store(m)
	return m
}

// DefaultTenant returns the tenant that answers the legacy API: the tenant
// "default" of the default manager or its only tenant. A multi-tenant process
// without a "default" tenant has none: the legacy API cannot be used there.
//
// Deprecated: tenant-aware code resolves its tenant in its TenantManager.
func DefaultTenant() *Tenant {
	t := DefaultManager().Default()
	if t == nil {
		log.Fatalf("logic: the legacy API was used in a multi-tenant process without a %q tenant; resolve the tenant in the TenantManager", config.DefaultTenantID)
	}
	return t
}

// ContinuousDiscovery starts the continuous discovery of every tenant of the
// process -- instances are periodically investigated and their status captured,
// long since unseen instances are purged and forgotten, failures are recovered --
// and blocks. Without a tenants file there is a single tenant, "default", driven
// by config.Config: the legacy behavior.
//
// Deprecated: build a TenantManager, SetDefaultManager if needed, and Start it.
func ContinuousDiscovery() {
	log.Infof("continuous discovery: setting up")
	m, err := NewTenantManager(config.Config)
	if err != nil {
		_ = log.Fatale(err)
	}
	SetDefaultManager(m)
	if err := m.Start(context.Background()); err != nil {
		_ = log.Fatale(err)
	}
	<-m.Done()
}
