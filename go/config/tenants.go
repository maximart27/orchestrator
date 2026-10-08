package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

const (
	// DefaultTenantID is the ID of the single tenant that exists when no tenants file is configured
	// (legacy single-tenant deployment).
	DefaultTenantID = "default"

	ProviderMySQL      = "mysql"
	ProviderPostgreSQL = "postgresql"

	defaultMySQLPort      = 3306
	defaultPostgreSQLPort = 5432
	defaultProxySQLPort   = 6032
)

var tenantIDRegexp = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// TenantConfig is the fully resolved configuration of one tenant.
type TenantConfig struct {
	ID       string // unique tenant ID, [a-z0-9][a-z0-9_-]{0,62}
	Provider string // ProviderMySQL or ProviderPostgreSQL
	// Cfg is the effective configuration of the tenant. In multi-tenant mode it is a private deep copy
	// of the global configuration with the tenant overrides applied. In legacy mode (no tenants file)
	// it is the global config.Config pointer itself.
	Cfg *Configuration
}

// NormalizeProvider maps the accepted provider names to ProviderMySQL / ProviderPostgreSQL.
// An empty name is the legacy default (mysql).
func NormalizeProvider(name string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "mysql", "mariadb":
		return ProviderMySQL, nil
	case "postgresql", "postgres":
		return ProviderPostgreSQL, nil
	}
	return "", fmt.Errorf("unknown provider %q (valid: mysql, mariadb, postgresql)", name)
}

// tenantManifest is the JSON document pointed to by Configuration.TenantsConfigFile.
type tenantManifest struct {
	Tenants []tenantEntry
}

type tenantEntry struct {
	ID       string
	Provider string
	Backend  json.RawMessage // only ScopeBackendNodeLocal fields
	Config   json.RawMessage // only ScopeTenant fields
}

// LoadTenantConfigs returns the tenants of this process.
//
// When base.TenantsConfigFile is empty it returns the single legacy tenant "default", whose Cfg is the
// base pointer itself (not a copy) and whose provider is derived from base.ProviderType.
// Otherwise it reads the tenants file (see ParseTenantConfigs). Errors are returned, never fatal.
func LoadTenantConfigs(base *Configuration) ([]*TenantConfig, error) {
	if base == nil {
		return nil, errors.New("tenants: nil base configuration")
	}
	if base.TenantsConfigFile == "" {
		provider, err := NormalizeProvider(base.ProviderType)
		if err != nil {
			return nil, fmt.Errorf("ProviderType: %w", err)
		}
		return []*TenantConfig{{ID: DefaultTenantID, Provider: provider, Cfg: base}}, nil
	}
	data, err := os.ReadFile(base.TenantsConfigFile)
	if err != nil {
		return nil, fmt.Errorf("tenants file %s: %w", base.TenantsConfigFile, err)
	}
	tenants, err := ParseTenantConfigs(base, data)
	if err != nil {
		return nil, fmt.Errorf("tenants file %s: %w", base.TenantsConfigFile, err)
	}
	return tenants, nil
}

// ParseTenantConfigs parses a tenants manifest and resolves each tenant on top of a deep copy of base.
// base is never modified.
func ParseTenantConfigs(base *Configuration, data []byte) ([]*TenantConfig, error) {
	if base == nil {
		return nil, errors.New("tenants: nil base configuration")
	}
	var manifest tenantManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("invalid tenants manifest: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("invalid tenants manifest: unexpected data after the JSON document")
	}
	if len(manifest.Tenants) == 0 {
		return nil, errors.New("invalid tenants manifest: \"Tenants\" must define at least one tenant")
	}

	tenants := make([]*TenantConfig, 0, len(manifest.Tenants))
	seen := map[string]bool{}
	for i, entry := range manifest.Tenants {
		if !tenantIDRegexp.MatchString(entry.ID) {
			return nil, fmt.Errorf("tenant #%d: invalid ID %q (must match %s)", i+1, entry.ID, tenantIDRegexp)
		}
		if seen[entry.ID] {
			return nil, fmt.Errorf("tenant %q: duplicate ID", entry.ID)
		}
		seen[entry.ID] = true

		tenant, err := buildTenantConfig(base, entry)
		if err != nil {
			return nil, fmt.Errorf("tenant %q: %w", entry.ID, err)
		}
		tenants = append(tenants, tenant)
	}
	if err := validateTenantSet(tenants); err != nil {
		return nil, err
	}
	return tenants, nil
}

func buildTenantConfig(base *Configuration, entry tenantEntry) (*TenantConfig, error) {
	if strings.TrimSpace(entry.Provider) == "" {
		return nil, errors.New("Provider is required (mysql, mariadb or postgresql)")
	}
	provider, err := NormalizeProvider(entry.Provider)
	if err != nil {
		return nil, fmt.Errorf("Provider: %w", err)
	}
	baseProvider, err := NormalizeProvider(base.ProviderType)
	if err != nil {
		return nil, fmt.Errorf("base ProviderType: %w", err)
	}

	cfg := base.Clone()
	set := map[string]bool{}
	if err := applyTenantSection(cfg, set, entry.Backend, "Backend", ScopeBackendNodeLocal); err != nil {
		return nil, err
	}
	if err := applyTenantSection(cfg, set, entry.Config, "Config", ScopeTenant); err != nil {
		return nil, err
	}
	if set["ProviderType"] {
		return nil, errors.New(`Config: field "ProviderType" is not accepted in a tenant: use the tenant "Provider" attribute`)
	}

	cfg.TenantsConfigFile = ""
	cfg.ProviderType = provider
	if provider != baseProvider && !set["DefaultInstancePort"] {
		// the inherited default port belongs to the other engine
		cfg.DefaultInstancePort = defaultMySQLPort
		if provider == ProviderPostgreSQL {
			cfg.DefaultInstancePort = defaultPostgreSQLPort
		}
	}

	resolveTenantSynonyms(cfg, set)
	if err := cfg.resolveOrchestratorCredentials(set["MySQLOrchestratorCredentialsConfigFile"], set["MySQLOrchestratorPassword"]); err != nil {
		return nil, err
	}
	if err := cfg.resolveTopologyCredentials(set["MySQLTopologyCredentialsConfigFile"], set["MySQLTopologyPassword"]); err != nil {
		return nil, err
	}
	if err := cfg.adjustTenantScoped(); err != nil {
		return nil, err
	}
	if err := cfg.validateBackend(); err != nil {
		return nil, err
	}
	return &TenantConfig{ID: entry.ID, Provider: provider, Cfg: cfg}, nil
}

// applyTenantSection validates that every key of the JSON object is a known Configuration field of
// the wanted scope and decodes it over cfg. The canonical names of the keys are added to set.
func applyTenantSection(cfg *Configuration, set map[string]bool, raw json.RawMessage, section string, want Scope) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err != nil {
		return fmt.Errorf("%s: must be a JSON object: %w", section, err)
	}
	names := make([]string, 0, len(keys))
	for key := range keys {
		names = append(names, key)
	}
	sort.Strings(names)

	sectionFor := map[Scope]string{ScopeTenant: "Config", ScopeBackendNodeLocal: "Backend"}
	canonicals := map[string]string{}
	for _, key := range names {
		name, known := canonicalFieldNames[strings.ToLower(key)]
		if !known {
			return fmt.Errorf("%s: unknown field %q", section, key)
		}
		if previous, dup := canonicals[name]; dup {
			return fmt.Errorf("%s: field %q is defined twice (%q and %q)", section, name, previous, key)
		}
		canonicals[name] = key
		scope := fieldScope[name]
		if scope == want {
			continue
		}
		switch scope {
		case ScopeTenant, ScopeBackendNodeLocal:
			return fmt.Errorf("%s: field %q is %s and must be placed in the tenant %q section", section, name, scope, sectionFor[scope])
		default:
			return fmt.Errorf("%s: field %q is %s and cannot be overridden per tenant (set it in the node configuration)", section, name, scope)
		}
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return fmt.Errorf("%s: %w", section, err)
	}
	for name := range canonicals {
		set[name] = true
	}
	return nil
}

// synonymPairs are alias/canonical field pairs that postReadAdjustments reconciles. A tenant that sets only
// one side must win over what the global config left on the other side, otherwise the inherited
// value would conflict with (or silently shadow) the tenant's.
var synonymPairs = [][2]string{
	{"SlaveLagQuery", "ReplicationLagQuery"},
	{"DetachLostSlavesAfterMasterFailover", "DetachLostReplicasAfterMasterFailover"},
	{"MasterFailoverDetachSlaveMasterHost", "MasterFailoverDetachReplicaMasterHost"},
	{"PostponeSlaveRecoveryOnLagMinutes", "PostponeReplicaRecoveryOnLagMinutes"},
	{"RecoveryPeriodBlockMinutes", "RecoveryPeriodBlockSeconds"},
	// derived: ReasonableLockedSemiSyncMasterSeconds falls back to ReasonableReplicationLagSeconds
	{"ReasonableReplicationLagSeconds", "ReasonableLockedSemiSyncMasterSeconds"},
}

func resolveTenantSynonyms(cfg *Configuration, set map[string]bool) {
	v := reflect.ValueOf(cfg).Elem()
	zero := func(name string) { v.FieldByName(name).Set(reflect.Zero(v.FieldByName(name).Type())) }
	for _, pair := range synonymPairs {
		first, second := pair[0], pair[1]
		switch {
		case set[first] && !set[second]:
			zero(second)
		case set[second] && !set[first] && pair[0] != "ReasonableReplicationLagSeconds":
			zero(first)
		}
	}
}

// validateTenantSet checks the constraints between tenants: resources that must not be shared.
func validateTenantSet(tenants []*TenantConfig) error {
	var errs []error

	sqliteFiles := map[string]string{}
	mysqlBackends := map[string]string{}
	for _, t := range tenants {
		cfg := t.Cfg
		switch {
		case cfg.IsSQLite():
			key := filepath.Clean(cfg.SQLite3DataFile)
			if other, found := sqliteFiles[key]; found {
				errs = append(errs, fmt.Errorf("tenants %q and %q share the same SQLite3DataFile %s", other, t.ID, cfg.SQLite3DataFile))
			} else {
				sqliteFiles[key] = t.ID
			}
		case cfg.IsMySQL() && cfg.MySQLOrchestratorHost != "":
			key := fmt.Sprintf("%s:%d/%s", strings.ToLower(cfg.MySQLOrchestratorHost), cfg.MySQLOrchestratorPort, cfg.MySQLOrchestratorDatabase)
			if other, found := mysqlBackends[key]; found {
				errs = append(errs, fmt.Errorf("tenants %q and %q share the same MySQL backend %s", other, t.ID, key))
			} else {
				mysqlBackends[key] = t.ID
			}
		}
	}

	for i, a := range tenants {
		for _, b := range tenants[i+1:] {
			if err := proxySQLConflict(a, b); err != nil {
				errs = append(errs, err)
			}
			if err := kvPrefixConflict(a, b); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func proxySQLEndpoint(cfg *Configuration) string {
	port := cfg.ProxySQLAdminPort
	if port == 0 {
		port = defaultProxySQLPort
	}
	return fmt.Sprintf("%s:%d", strings.ToLower(strings.TrimSpace(cfg.ProxySQLAdminAddress)), port)
}

// proxySQLConflict: two tenants driving the same ProxySQL admin endpoint must use disjoint hostgroups
// (0 means unconfigured and never conflicts).
func proxySQLConflict(a, b *TenantConfig) error {
	if strings.TrimSpace(a.Cfg.ProxySQLAdminAddress) == "" || strings.TrimSpace(b.Cfg.ProxySQLAdminAddress) == "" {
		return nil
	}
	if proxySQLEndpoint(a.Cfg) != proxySQLEndpoint(b.Cfg) {
		return nil
	}
	for _, ha := range []int{a.Cfg.ProxySQLWriterHostgroup, a.Cfg.ProxySQLReaderHostgroup} {
		if ha == 0 {
			continue
		}
		for _, hb := range []int{b.Cfg.ProxySQLWriterHostgroup, b.Cfg.ProxySQLReaderHostgroup} {
			if ha == hb {
				return fmt.Errorf("tenants %q and %q use the same ProxySQL %s with overlapping hostgroup %d", a.ID, b.ID, proxySQLEndpoint(a.Cfg), ha)
			}
		}
	}
	return nil
}

// kvPrefixConflict: tenants publishing to the same KV store (same Consul or ZooKeeper address) with the
// same KVClusterMasterPrefix would overwrite each other's cluster master entries. It is an error rather
// than a warning because the damage (wrong master published to the application layer) is silent.
// Tenants with their own KV store, or none, do not conflict.
func kvPrefixConflict(a, b *TenantConfig) error {
	if a.Cfg.KVClusterMasterPrefix != b.Cfg.KVClusterMasterPrefix {
		return nil
	}
	sharedStore := func(x, y string) bool {
		return strings.TrimSpace(x) != "" && strings.EqualFold(strings.TrimSpace(x), strings.TrimSpace(y))
	}
	if sharedStore(a.Cfg.ConsulAddress, b.Cfg.ConsulAddress) || sharedStore(a.Cfg.ZkAddress, b.Cfg.ZkAddress) {
		return fmt.Errorf("tenants %q and %q share a KV store and the same KVClusterMasterPrefix %q: give each tenant its own prefix", a.ID, b.ID, a.Cfg.KVClusterMasterPrefix)
	}
	return nil
}
