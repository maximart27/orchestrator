package inst

import (
	"sync"

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
)

// DefaultStore returns the Store of the legacy single-tenant setup: it is driven
// by the global config.Config and by db.DefaultBackend(), and keeps the
// historical per-instance MySQL/PostgreSQL dispatch. It is built on first use
// (never at package init), so it is safe to import the package before the
// configuration is loaded, but it must not be called before then. Its metric and
// collection names are the historical, un-prefixed ones.
//
// Deprecated: tenant-aware code receives its own Store (see NewStore).
func DefaultStore() *Store {
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
