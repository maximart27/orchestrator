package inst

import (
	"sync"
	"sync/atomic"

	"github.com/proxysql/golib/log"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
)

// This file is the ONLY place of the package that reads the legacy global
// configuration (config.Config) and the legacy default backend: it bridges the
// old single-tenant API (package-level functions, see the *_compat.go files)
// onto the Store of the implicit "default" tenant.

var (
	defaultStore     *Store
	defaultStoreOnce sync.Once

	defaultStoreDisabled atomic.Bool
	disabledStoreOnce    sync.Once
	disabledStore        *Store
)

// disabledStoreID names the inert Store DefaultStore() returns once disabled. It
// starts with an underscore, which no tenant ID may, so it cannot clash with a
// tenant's metrics or collections (the legacy store uses the bare names).
const disabledStoreID = "_default-disabled"

// DisableDefaultStore makes DefaultStore() return an inert Store -- bound to the
// disabled db.DefaultBackend(), so every database operation fails with
// db.ErrDefaultBackendDisabled -- instead of the legacy store driven by the
// global config.Config. The TenantManager calls it (with db.DisableDefaultBackend)
// when the process runs from a tenants file: there the base configuration
// describes no tenant, and a call that was not migrated to its tenant's Store
// must fail with an error, never act on the legacy backend or exit the process.
// In the legacy single-tenant setup nothing changes.
func DisableDefaultStore() { defaultStoreDisabled.Store(true) }

// EnableDefaultStore undoes DisableDefaultStore (the TenantManager that disabled
// it was stopped).
func EnableDefaultStore() { defaultStoreDisabled.Store(false) }

// DefaultStore returns the Store of the legacy single-tenant setup: it is driven
// by the global config.Config and by db.DefaultBackend(), and keeps the
// historical per-instance MySQL/PostgreSQL dispatch. It is built on first use
// (never at package init), so it is safe to import the package before the
// configuration is loaded, but it must not be called before then. Its metric and
// collection names are the historical, un-prefixed ones.
//
// After DisableDefaultStore it returns an inert Store whose database operations
// fail with db.ErrDefaultBackendDisabled; the first use is logged as an error.
//
// Deprecated: tenant-aware code receives its own Store (see NewStore).
func DefaultStore() *Store {
	if defaultStoreDisabled.Load() {
		return inertDefaultStore()
	}
	defaultStoreOnce.Do(func() {
		s, err := NewStore(db.DefaultBackendID, config.Config, db.DefaultBackend(), withLegacyMixedProviders())
		if err != nil {
			// unreachable: the arguments are never nil and a legacy store accepts any provider type
			log.Fatalf("inst: cannot build the default store: %v", err)
		}
		defaultStore = s
	})
	return defaultStore
}

// legacyConfig is the global configuration, for the constructors that predate
// per-tenant configuration.
func legacyConfig() *config.Configuration {
	return config.Config
}

func inertDefaultStore() *Store {
	disabledStoreOnce.Do(func() {
		_ = log.Errorf("inst: DefaultStore() was used in a multi-tenant process: that code path is not tenant-aware and its database operations will fail (%v)", db.ErrDefaultBackendDisabled)
		s, err := NewStore(disabledStoreID, config.Config, db.DisabledBackend(), withLegacyMixedProviders())
		if err != nil {
			// unreachable, as for the legacy store
			log.Fatalf("inst: cannot build the inert default store: %v", err)
		}
		disabledStore = s
	})
	return disabledStore
}
