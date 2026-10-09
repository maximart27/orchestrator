package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const validManifest = `{
  "Tenants": [
    {
      "ID": "mariadb-a",
      "Provider": "mysql",
      "Backend": {"BackendDB": "sqlite3", "SQLite3DataFile": "/data/a.db"},
      "Config": {
        "MySQLTopologyUser": "orc-a",
        "MySQLTopologyPassword": "secret-a",
        "InstancePollSeconds": 5,
        "RecoverMasterClusterFilters": ["*"],
        "KVClusterMasterPrefix": "mariadb-a/master",
        "ConsulAddress": "consul:8500",
        "ProxySQLAdminAddress": "proxysql-a",
        "ProxySQLWriterHostgroup": 10,
        "ProxySQLReaderHostgroup": 20
      }
    },
    {
      "ID": "mariadb-b",
      "Provider": "mariadb",
      "Backend": {"BackendDB": "sqlite3", "SQLite3DataFile": "/data/b.db"},
      "Config": {
        "MySQLTopologyUser": "orc-b",
        "InstancePollSeconds": 10,
        "ReasonableReplicationLagSeconds": 60,
        "PreFailoverProcesses": ["/usr/local/bin/pre-b {failedHost}"],
        "KVClusterMasterPrefix": "mariadb-b/master",
        "ConsulAddress": "consul:8500",
        "ProxySQLAdminAddress": "proxysql-a",
        "ProxySQLWriterHostgroup": 30,
        "ProxySQLReaderHostgroup": 40
      }
    },
    {
      "ID": "postgres-c",
      "Provider": "postgresql",
      "Backend": {"BackendDB": "sqlite3", "SQLite3DataFile": "/data/c.db"},
      "Config": {
        "PostgreSQLTopologyUser": "orc-c",
        "PostgreSQLTopologyPassword": "secret-c",
        "PostgreSQLSSLMode": "verify-full"
      }
    }
  ]
}`

func testBase() *Configuration {
	c := newConfiguration()
	c.HTTPAuthUser = "admin"
	c.RaftEnabled = true
	c.RaftBind = "10.0.0.1:10008"
	c.RaftDataDir = "/raft"
	c.ReasonableReplicationLagSeconds = 17
	c.PowerAuthUsers = []string{"alice", "bob"}
	c.ClusterNameToAlias = map[string]string{"global": "g"}
	return c
}

func parse(t *testing.T, base *Configuration, manifest string) []*TenantConfig {
	t.Helper()
	tenants, err := ParseTenantConfigs(base, []byte(manifest))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return tenants
}

func tenantByID(t *testing.T, tenants []*TenantConfig, id string) *TenantConfig {
	t.Helper()
	for _, tc := range tenants {
		if tc.ID == id {
			return tc
		}
	}
	t.Fatalf("tenant %s not found", id)
	return nil
}

func TestParseValidManifest(t *testing.T) {
	base := testBase()
	tenants := parse(t, base, validManifest)
	if len(tenants) != 3 {
		t.Fatalf("got %d tenants", len(tenants))
	}
	a, b, c := tenants[0], tenants[1], tenants[2]
	if a.ID != "mariadb-a" || b.ID != "mariadb-b" || c.ID != "postgres-c" {
		t.Fatalf("order/ID not preserved: %s %s %s", a.ID, b.ID, c.ID)
	}
	if a.Provider != ProviderMySQL || b.Provider != ProviderMySQL || c.Provider != ProviderPostgreSQL {
		t.Errorf("providers: %s %s %s", a.Provider, b.Provider, c.Provider)
	}
	if a.Cfg.ProviderType != "mysql" || c.Cfg.ProviderType != "postgresql" {
		t.Errorf("ProviderType not set on Cfg: %q %q", a.Cfg.ProviderType, c.Cfg.ProviderType)
	}
	// overrides
	if a.Cfg.MySQLTopologyUser != "orc-a" || b.Cfg.MySQLTopologyUser != "orc-b" {
		t.Errorf("tenant override not applied")
	}
	if a.Cfg.InstancePollSeconds != 5 || b.Cfg.InstancePollSeconds != 10 {
		t.Errorf("InstancePollSeconds: %d %d", a.Cfg.InstancePollSeconds, b.Cfg.InstancePollSeconds)
	}
	if a.Cfg.SQLite3DataFile != "/data/a.db" || b.Cfg.SQLite3DataFile != "/data/b.db" || c.Cfg.SQLite3DataFile != "/data/c.db" {
		t.Errorf("backend overrides not applied")
	}
	if !reflect.DeepEqual(b.Cfg.PreFailoverProcesses, []string{"/usr/local/bin/pre-b {failedHost}"}) {
		t.Errorf("PreFailoverProcesses: %v", b.Cfg.PreFailoverProcesses)
	}
	if len(a.Cfg.PreFailoverProcesses) != 0 {
		t.Errorf("tenant a must not see tenant b's hooks: %v", a.Cfg.PreFailoverProcesses)
	}
	// post-read adjustments applied on each tenant
	if a.Cfg.KVClusterMasterPrefix != "mariadb-a/master/" {
		t.Errorf("KV prefix not normalized: %q", a.Cfg.KVClusterMasterPrefix)
	}
	if a.Cfg.ReasonableLockedSemiSyncMasterSeconds != 17 {
		t.Errorf("a: locked semisync should inherit global lag: %d", a.Cfg.ReasonableLockedSemiSyncMasterSeconds)
	}
	if b.Cfg.ReasonableLockedSemiSyncMasterSeconds != 60 {
		t.Errorf("b: locked semisync must be re-derived from the tenant's lag: %d", b.Cfg.ReasonableLockedSemiSyncMasterSeconds)
	}
	// default instance port follows the engine unless set
	if a.Cfg.DefaultInstancePort != 3306 || c.Cfg.DefaultInstancePort != 5432 {
		t.Errorf("DefaultInstancePort: %d %d", a.Cfg.DefaultInstancePort, c.Cfg.DefaultInstancePort)
	}
	if c.Cfg.PostgreSQLSSLMode != "verify-full" || a.Cfg.PostgreSQLSSLMode != "require" {
		t.Errorf("PostgreSQLSSLMode: %q %q", c.Cfg.PostgreSQLSSLMode, a.Cfg.PostgreSQLSSLMode)
	}
}

func TestParseInheritsGlobalValues(t *testing.T) {
	base := testBase()
	tenants := parse(t, base, validManifest)
	for _, tc := range tenants {
		if tc.Cfg.HTTPAuthUser != "admin" || !tc.Cfg.RaftEnabled || tc.Cfg.RaftBind != "10.0.0.1:10008" || tc.Cfg.RaftDataDir != "/raft" {
			t.Errorf("%s: global/node-local settings not inherited", tc.ID)
		}
		if !reflect.DeepEqual(tc.Cfg.PowerAuthUsers, []string{"alice", "bob"}) {
			t.Errorf("%s: PowerAuthUsers not inherited: %v", tc.ID, tc.Cfg.PowerAuthUsers)
		}
		if tc.Cfg.TenantsConfigFile != "" {
			t.Errorf("%s: tenant config must not point to a tenants file", tc.ID)
		}
	}
	if got := tenantByID(t, tenants, "mariadb-a").Cfg.ReasonableReplicationLagSeconds; got != 17 {
		t.Errorf("inherited ReasonableReplicationLagSeconds = %d, want 17", got)
	}
	if got := tenantByID(t, tenants, "postgres-c").Cfg.InstancePollSeconds; got != base.InstancePollSeconds {
		t.Errorf("inherited InstancePollSeconds = %d", got)
	}
	// maps are merged with the inherited ones
	merged := parse(t, base, `{"Tenants":[{"ID":"t","Provider":"mysql","Config":{"ClusterNameToAlias":{"mine":"m"}}}]}`)
	if !reflect.DeepEqual(merged[0].Cfg.ClusterNameToAlias, map[string]string{"global": "g", "mine": "m"}) {
		t.Errorf("ClusterNameToAlias: %v", merged[0].Cfg.ClusterNameToAlias)
	}
}

func TestParseKeysAreCaseInsensitive(t *testing.T) {
	tenants := parse(t, testBase(), `{"Tenants":[{"ID":"t","Provider":"MySQL","Config":{"instancepollseconds": 9}}]}`)
	if tenants[0].Cfg.InstancePollSeconds != 9 {
		t.Errorf("case-insensitive key not applied")
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
		contains []string
	}{
		{"global field in Config", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"HTTPAuthUser":"x"}}]}`, []string{`tenant "t1"`, `"HTTPAuthUser"`, "global"}},
		{"read-only is global", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"ReadOnly":true}}]}`, []string{`tenant "t1"`, `"ReadOnly"`, "global"}},
		{"node-local field in Config", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"RaftBind":"x"}}]}`, []string{`tenant "t1"`, `"RaftBind"`, "node-local"}},
		{"backend field in Config", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"SQLite3DataFile":"/x"}}]}`, []string{`tenant "t1"`, `"SQLite3DataFile"`, `"Backend" section`}},
		{"tenant field in Backend", `{"Tenants":[{"ID":"t1","Provider":"mysql","Backend":{"InstancePollSeconds":3}}]}`, []string{`tenant "t1"`, `"InstancePollSeconds"`, `"Config" section`}},
		{"global field in Backend", `{"Tenants":[{"ID":"t1","Provider":"mysql","Backend":{"RaftBind":"x"}}]}`, []string{`tenant "t1"`, `"RaftBind"`}},
		{"TenantsConfigFile in tenant", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"TenantsConfigFile":"/x"}}]}`, []string{`tenant "t1"`, `"TenantsConfigFile"`}},
		{"ProviderType in Config", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"ProviderType":"postgresql"}}]}`, []string{`tenant "t1"`, "ProviderType", "Provider"}},
		{"unknown field in Config", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"NoSuchField":1}}]}`, []string{`tenant "t1"`, `unknown field "NoSuchField"`}},
		{"unknown field in Backend", `{"Tenants":[{"ID":"t1","Provider":"mysql","Backend":{"Nope":1}}]}`, []string{`tenant "t1"`, `unknown field "Nope"`}},
		{"field twice", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"InstancePollSeconds":1,"instancepollseconds":2}}]}`, []string{`tenant "t1"`, "twice"}},
		{"wrong type", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"InstancePollSeconds":"fast"}}]}`, []string{`tenant "t1"`, "InstancePollSeconds"}},
		{"unknown tenant attribute", `{"Tenants":[{"ID":"t1","Provider":"mysql","Colour":"red"}]}`, []string{"Colour"}},
		{"unknown top-level key", `{"Tenant":[]}`, []string{"Tenant"}},
		{"not json", `{`, []string{"invalid tenants manifest"}},
		{"trailing data", `{"Tenants":[{"ID":"t1","Provider":"mysql"}]} {}`, []string{"unexpected data"}},
		{"no tenants", `{"Tenants":[]}`, []string{"at least one tenant"}},
		{"duplicate ID", `{"Tenants":[{"ID":"t1","Provider":"mysql"},{"ID":"t1","Provider":"postgresql"}]}`, []string{`tenant "t1"`, "duplicate"}},
		{"empty ID", `{"Tenants":[{"ID":"","Provider":"mysql"}]}`, []string{"invalid ID"}},
		{"uppercase ID", `{"Tenants":[{"ID":"Prod","Provider":"mysql"}]}`, []string{"invalid ID", "Prod"}},
		{"ID starting with dash", `{"Tenants":[{"ID":"-a","Provider":"mysql"}]}`, []string{"invalid ID"}},
		{"ID with slash", `{"Tenants":[{"ID":"a/b","Provider":"mysql"}]}`, []string{"invalid ID"}},
		{"ID too long", `{"Tenants":[{"ID":"` + strings.Repeat("a", 64) + `","Provider":"mysql"}]}`, []string{"invalid ID"}},
		{"missing provider", `{"Tenants":[{"ID":"t1"}]}`, []string{`tenant "t1"`, "Provider is required"}},
		{"unknown provider", `{"Tenants":[{"ID":"t1","Provider":"oracle"}]}`, []string{`tenant "t1"`, "oracle"}},
		{"sqlite without file", `{"Tenants":[{"ID":"t1","Provider":"mysql","Backend":{"BackendDB":"sqlite3"}}]}`, []string{`tenant "t1"`, "SQLite3DataFile"}},
		{"tenant adjustment error names tenant", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"FailMasterPromotionOnLagMinutes":3}}]}`, []string{`tenant "t1"`, "ReplicationLagQuery"}},
		{"dead instance poll check per tenant", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"InstancePollSeconds":9999}}]}`, []string{`tenant "t1"`, "DeadInstancePollSecondsMax"}},
		{"missing credentials file does not kill the process", `{"Tenants":[{"ID":"t1","Provider":"mysql","Config":{"MySQLTopologyCredentialsConfigFile":"/does/not/exist"}}]}`, []string{`tenant "t1"`, "gcfg"}},
		{"second tenant error is attributed to it", `{"Tenants":[{"ID":"ok","Provider":"mysql"},{"ID":"bad","Provider":"mysql","Config":{"RaftBind":"x"}}]}`, []string{`tenant "bad"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseTenantConfigs(testBase(), []byte(tt.manifest))
			if err == nil {
				t.Fatalf("expected an error")
			}
			for _, want := range tt.contains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
}

func TestCrossTenantValidation(t *testing.T) {
	tenant := func(id, backend, cfg string) string {
		return `{"ID":"` + id + `","Provider":"mysql","Backend":{` + backend + `},"Config":{` + cfg + `}}`
	}
	sqlite := func(file string) string { return `"BackendDB":"sqlite3","SQLite3DataFile":"` + file + `"` }
	tests := []struct {
		name     string
		tenants  []string
		contains []string // empty: must succeed
	}{
		{"sqlite duplicate", []string{tenant("a", sqlite("/d/x.db"), ``), tenant("b", sqlite("/d/x.db"), ``)}, []string{`"a"`, `"b"`, "SQLite3DataFile"}},
		{"sqlite duplicate through path cleaning", []string{tenant("a", sqlite("/d/x.db"), ``), tenant("b", sqlite("/d//./x.db"), ``)}, []string{"SQLite3DataFile"}},
		{"sqlite distinct", []string{tenant("a", sqlite("/d/a.db"), ``), tenant("b", sqlite("/d/b.db"), ``)}, nil},
		{"mysql backend duplicate", []string{
			tenant("a", `"MySQLOrchestratorHost":"db","MySQLOrchestratorDatabase":"orc"`, ``),
			tenant("b", `"MySQLOrchestratorHost":"DB","MySQLOrchestratorDatabase":"orc"`, ``)}, []string{"same MySQL backend"}},
		{"mysql backend distinct database", []string{
			tenant("a", `"MySQLOrchestratorHost":"db","MySQLOrchestratorDatabase":"orc_a"`, ``),
			tenant("b", `"MySQLOrchestratorHost":"db","MySQLOrchestratorDatabase":"orc_b"`, ``)}, nil},
		{"proxysql same endpoint overlapping writer", []string{
			tenant("a", sqlite("/a"), `"ProxySQLAdminAddress":"ps","ProxySQLWriterHostgroup":10,"ProxySQLReaderHostgroup":20`),
			tenant("b", sqlite("/b"), `"ProxySQLAdminAddress":"ps","ProxySQLWriterHostgroup":10,"ProxySQLReaderHostgroup":30`)}, []string{`"a"`, `"b"`, "hostgroup 10"}},
		{"proxysql same endpoint writer vs reader", []string{
			tenant("a", sqlite("/a"), `"ProxySQLAdminAddress":"ps","ProxySQLWriterHostgroup":10,"ProxySQLReaderHostgroup":20`),
			tenant("b", sqlite("/b"), `"ProxySQLAdminAddress":"PS","ProxySQLWriterHostgroup":30,"ProxySQLReaderHostgroup":20`)}, []string{"hostgroup 20"}},
		{"proxysql default port equals 6032", []string{
			tenant("a", sqlite("/a"), `"ProxySQLAdminAddress":"ps","ProxySQLAdminPort":6032,"ProxySQLWriterHostgroup":10`),
			tenant("b", sqlite("/b"), `"ProxySQLAdminAddress":"ps","ProxySQLAdminPort":0,"ProxySQLWriterHostgroup":10`)}, []string{"hostgroup 10"}},
		{"proxysql same endpoint disjoint", []string{
			tenant("a", sqlite("/a"), `"ProxySQLAdminAddress":"ps","ProxySQLWriterHostgroup":10,"ProxySQLReaderHostgroup":20`),
			tenant("b", sqlite("/b"), `"ProxySQLAdminAddress":"ps","ProxySQLWriterHostgroup":30,"ProxySQLReaderHostgroup":40`)}, nil},
		{"proxysql different endpoint same hostgroups", []string{
			tenant("a", sqlite("/a"), `"ProxySQLAdminAddress":"ps1","ProxySQLWriterHostgroup":10`),
			tenant("b", sqlite("/b"), `"ProxySQLAdminAddress":"ps2","ProxySQLWriterHostgroup":10`)}, nil},
		{"proxysql different port same hostgroups", []string{
			tenant("a", sqlite("/a"), `"ProxySQLAdminAddress":"ps","ProxySQLWriterHostgroup":10`),
			tenant("b", sqlite("/b"), `"ProxySQLAdminAddress":"ps","ProxySQLAdminPort":6033,"ProxySQLWriterHostgroup":10`)}, nil},
		{"proxysql unconfigured hostgroups never conflict", []string{
			tenant("a", sqlite("/a"), `"ProxySQLAdminAddress":"ps"`),
			tenant("b", sqlite("/b"), `"ProxySQLAdminAddress":"ps"`)}, nil},
		{"proxysql no address never conflicts", []string{
			tenant("a", sqlite("/a"), `"ProxySQLWriterHostgroup":10`),
			tenant("b", sqlite("/b"), `"ProxySQLWriterHostgroup":10`)}, nil},
		{"kv shared consul same prefix", []string{
			tenant("a", sqlite("/a"), `"ConsulAddress":"c:8500","KVClusterMasterPrefix":"x"`),
			tenant("b", sqlite("/b"), `"ConsulAddress":"C:8500","KVClusterMasterPrefix":"x/"`)}, []string{"KVClusterMasterPrefix"}},
		{"kv shared consul default prefix", []string{
			tenant("a", sqlite("/a"), `"ConsulAddress":"c:8500"`),
			tenant("b", sqlite("/b"), `"ConsulAddress":"c:8500"`)}, []string{"KVClusterMasterPrefix"}},
		{"kv shared consul distinct prefix", []string{
			tenant("a", sqlite("/a"), `"ConsulAddress":"c:8500","KVClusterMasterPrefix":"a"`),
			tenant("b", sqlite("/b"), `"ConsulAddress":"c:8500","KVClusterMasterPrefix":"b"`)}, nil},
		{"kv distinct consul same prefix", []string{
			tenant("a", sqlite("/a"), `"ConsulAddress":"c1:8500"`),
			tenant("b", sqlite("/b"), `"ConsulAddress":"c2:8500"`)}, nil},
		{"kv no store same prefix", []string{
			tenant("a", sqlite("/a"), ``),
			tenant("b", sqlite("/b"), ``)}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifest := `{"Tenants":[` + strings.Join(tt.tenants, ",") + `]}`
			_, err := ParseTenantConfigs(newConfiguration(), []byte(manifest))
			if len(tt.contains) == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error")
			}
			for _, want := range tt.contains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
}

func TestCrossTenantInheritedSharedResources(t *testing.T) {
	// tenants that inherit the same sqlite file / proxysql hostgroups from the node config conflict
	base := newConfiguration()
	base.BackendDB = "sqlite3"
	base.SQLite3DataFile = "/data/orchestrator.db"
	_, err := ParseTenantConfigs(base, []byte(`{"Tenants":[{"ID":"a","Provider":"mysql"},{"ID":"b","Provider":"mysql"}]}`))
	if err == nil || !strings.Contains(err.Error(), "SQLite3DataFile") {
		t.Errorf("expected a shared SQLite error, got %v", err)
	}

	base = newConfiguration()
	base.ProxySQLAdminAddress = "ps"
	base.ProxySQLWriterHostgroup = 10
	_, err = ParseTenantConfigs(base, []byte(`{"Tenants":[{"ID":"a","Provider":"mysql"},{"ID":"b","Provider":"mysql","Config":{"ProxySQLWriterHostgroup":11}}]}`))
	if err != nil {
		t.Errorf("tenant b overrides the writer hostgroup, no conflict expected: %v", err)
	}
	_, err = ParseTenantConfigs(base, []byte(`{"Tenants":[{"ID":"a","Provider":"mysql"},{"ID":"b","Provider":"mysql"}]}`))
	if err == nil || !strings.Contains(err.Error(), "hostgroup 10") {
		t.Errorf("expected a ProxySQL overlap error, got %v", err)
	}
}

func TestLegacyDefaultTenant(t *testing.T) {
	if DefaultTenantID != "default" {
		t.Fatalf("DefaultTenantID must be \"default\"")
	}
	for _, tt := range []struct{ providerType, want string }{
		{"mysql", ProviderMySQL}, {"", ProviderMySQL}, {"mariadb", ProviderMySQL}, {"postgresql", ProviderPostgreSQL},
	} {
		base := newConfiguration()
		base.ProviderType = tt.providerType
		tenants, err := LoadTenantConfigs(base)
		if err != nil {
			t.Fatal(err)
		}
		if len(tenants) != 1 || tenants[0].ID != DefaultTenantID {
			t.Fatalf("expected a single default tenant, got %+v", tenants)
		}
		if tenants[0].Cfg != base {
			t.Errorf("legacy tenant must use the global configuration pointer, not a copy")
		}
		if tenants[0].Provider != tt.want {
			t.Errorf("ProviderType %q: provider %q, want %q", tt.providerType, tenants[0].Provider, tt.want)
		}
	}
	base := newConfiguration()
	base.ProviderType = "oracle"
	if _, err := LoadTenantConfigs(base); err == nil {
		t.Errorf("unknown legacy ProviderType must be an error")
	}
	if _, err := LoadTenantConfigs(nil); err == nil {
		t.Errorf("nil base must be an error")
	}
}

func TestLoadTenantConfigsFromFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "tenants.json")
	if err := os.WriteFile(file, []byte(validManifest), 0o600); err != nil {
		t.Fatal(err)
	}
	base := testBase()
	base.TenantsConfigFile = file
	tenants, err := LoadTenantConfigs(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(tenants) != 3 {
		t.Fatalf("got %d tenants", len(tenants))
	}
	for _, tc := range tenants {
		if tc.Cfg == base {
			t.Errorf("%s: multi-tenant Cfg must be a copy", tc.ID)
		}
	}

	base.TenantsConfigFile = filepath.Join(dir, "missing.json")
	if _, err := LoadTenantConfigs(base); err == nil || !strings.Contains(err.Error(), "missing.json") {
		t.Errorf("missing file must be an error naming the file, got %v", err)
	}

	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"Tenants":[{"ID":"x","Provider":"mysql","Config":{"RaftBind":"y"}}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	base.TenantsConfigFile = bad
	if _, err := LoadTenantConfigs(base); err == nil || !strings.Contains(err.Error(), "bad.json") || !strings.Contains(err.Error(), `"RaftBind"`) {
		t.Errorf("expected an error naming file and field, got %v", err)
	}
}

func TestTenantIsolation(t *testing.T) {
	base := testBase()
	baseSnapshot := mustJSON(t, base)
	tenants := parse(t, base, validManifest)
	if mustJSON(t, base) != baseSnapshot {
		t.Fatalf("parsing tenants modified the base configuration")
	}

	a, b, c := tenants[0].Cfg, tenants[1].Cfg, tenants[2].Cfg
	snapshotB, snapshotC := mustJSON(t, b), mustJSON(t, c)

	// mutate every reference-typed and scalar field of tenant a in place
	v := reflect.ValueOf(a).Elem()
	for i := 0; i < v.NumField(); i++ {
		mutateInPlace(v.Field(i))
	}
	if mustJSON(t, base) != baseSnapshot {
		t.Errorf("mutating a tenant's Cfg altered the base configuration")
	}
	if mustJSON(t, b) != snapshotB || mustJSON(t, c) != snapshotC {
		t.Errorf("mutating a tenant's Cfg altered another tenant")
	}

	// and the other direction: mutating base after loading does not leak into tenants
	snapshotB = mustJSON(t, b)
	bv := reflect.ValueOf(base).Elem()
	for i := 0; i < bv.NumField(); i++ {
		mutateInPlace(bv.Field(i))
	}
	if mustJSON(t, b) != snapshotB {
		t.Errorf("mutating the base altered a tenant's Cfg")
	}
}

func TestTenantSynonymOverrides(t *testing.T) {
	base := newConfiguration()
	base.ReplicationLagQuery = "select 1"
	base.PostponeReplicaRecoveryOnLagMinutes = 5
	base.RecoveryPeriodBlockSeconds = 7200

	// a tenant setting the alias must win over the inherited canonical value, not conflict with it
	tenants := parse(t, base, `{"Tenants":[{"ID":"t","Provider":"mysql","Config":{
		"SlaveLagQuery":"select 2",
		"PostponeSlaveRecoveryOnLagMinutes": 9,
		"RecoveryPeriodBlockMinutes": 2
	}}]}`)
	cfg := tenants[0].Cfg
	if cfg.ReplicationLagQuery != "select 2" || cfg.SlaveLagQuery != "" {
		t.Errorf("lag query: %q / %q", cfg.ReplicationLagQuery, cfg.SlaveLagQuery)
	}
	if cfg.PostponeReplicaRecoveryOnLagMinutes != 9 {
		t.Errorf("postpone: %d", cfg.PostponeReplicaRecoveryOnLagMinutes)
	}
	if cfg.RecoveryPeriodBlockSeconds != 120 {
		t.Errorf("recovery period: %d", cfg.RecoveryPeriodBlockSeconds)
	}

	// both synonyms set in the tenant with different values is still an error
	_, err := ParseTenantConfigs(base, []byte(`{"Tenants":[{"ID":"t","Provider":"mysql","Config":{"SlaveLagQuery":"a","ReplicationLagQuery":"b"}}]}`))
	if err == nil || !strings.Contains(err.Error(), "synonyms") {
		t.Errorf("expected synonyms error, got %v", err)
	}

	// explicit false on the canonical detach flag beats the default-true alias
	tenants = parse(t, newConfiguration(), `{"Tenants":[{"ID":"t","Provider":"mysql","Config":{"DetachLostReplicasAfterMasterFailover":false}}]}`)
	if tenants[0].Cfg.DetachLostReplicasAfterMasterFailover {
		t.Errorf("tenant explicit false was overridden by the inherited legacy alias")
	}
}

func TestTenantCredentials(t *testing.T) {
	dir := t.TempDir()
	creds := filepath.Join(dir, "my.cnf")
	if err := os.WriteFile(creds, []byte("[client]\nuser=fileuser\npassword=filepass\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("S1_TEST_TENANT_PASSWORD", "envpass")

	tenants := parse(t, newConfiguration(), `{"Tenants":[
		{"ID":"file","Provider":"mysql","Config":{"MySQLTopologyCredentialsConfigFile":"`+creds+`"}},
		{"ID":"env","Provider":"mysql","Config":{"MySQLTopologyUser":"u","MySQLTopologyPassword":"${S1_TEST_TENANT_PASSWORD}"}},
		{"ID":"literal","Provider":"mysql","Config":{"MySQLTopologyUser":"u2","MySQLTopologyPassword":"plain"}}
	]}`)
	if c := tenants[0].Cfg; c.MySQLTopologyUser != "fileuser" || c.MySQLTopologyPassword != "filepass" {
		t.Errorf("credentials file not applied: %q/%q", c.MySQLTopologyUser, c.MySQLTopologyPassword)
	}
	if c := tenants[1].Cfg; c.MySQLTopologyPassword != "envpass" {
		t.Errorf("env password not expanded: %q", c.MySQLTopologyPassword)
	}
	if c := tenants[2].Cfg; c.MySQLTopologyPassword != "plain" {
		t.Errorf("literal password: %q", c.MySQLTopologyPassword)
	}

	// a tenant setting credentials directly must not be overridden by an inherited credentials file path
	base := newConfiguration()
	base.MySQLTopologyCredentialsConfigFile = "/does/not/exist.cnf"
	base.MySQLTopologyUser = "global"
	tenants = parse(t, base, `{"Tenants":[{"ID":"t","Provider":"mysql","Config":{"MySQLTopologyUser":"mine"}}]}`)
	if tenants[0].Cfg.MySQLTopologyUser != "mine" {
		t.Errorf("tenant user lost: %q", tenants[0].Cfg.MySQLTopologyUser)
	}

	// backend credentials file in the Backend section
	tenants = parse(t, newConfiguration(), `{"Tenants":[{"ID":"t","Provider":"mysql","Backend":{"MySQLOrchestratorHost":"h","MySQLOrchestratorCredentialsConfigFile":"`+creds+`"}}]}`)
	if c := tenants[0].Cfg; c.MySQLOrchestratorUser != "fileuser" || c.MySQLOrchestratorPassword != "filepass" {
		t.Errorf("backend credentials file not applied: %q/%q", c.MySQLOrchestratorUser, c.MySQLOrchestratorPassword)
	}
}

func TestNormalizeProvider(t *testing.T) {
	for in, want := range map[string]string{"mysql": "mysql", "MariaDB": "mysql", " postgres ": "postgresql", "PostgreSQL": "postgresql", "": "mysql"} {
		got, err := NormalizeProvider(in)
		if err != nil || got != want {
			t.Errorf("NormalizeProvider(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := NormalizeProvider("sqlserver"); err == nil {
		t.Errorf("expected error")
	}
}

// TestDocExampleIsValid keeps docs/multi-tenant-config.md honest: the JSON example in the doc must load.
func TestDocExampleIsValid(t *testing.T) {
	data, err := os.ReadFile("../../docs/multi-tenant-config.md")
	if err != nil {
		t.Skipf("doc not available: %v", err)
	}
	text := string(data)
	start := strings.Index(text, "```json\n{\n  \"Tenants\"")
	if start < 0 {
		t.Fatalf("tenants example not found in the doc")
	}
	start += len("```json\n")
	end := strings.Index(text[start:], "\n```")
	tenants, err := ParseTenantConfigs(newConfiguration(), []byte(text[start:start+end]))
	if err != nil {
		t.Fatalf("doc example is invalid: %v", err)
	}
	if len(tenants) != 3 {
		t.Errorf("doc example has %d tenants, want 3", len(tenants))
	}
}

// With a MySQL backend on the node, a tenant must not live in the control database of the node (nor share a
// database with another tenant): the schema, node_health and recoveries of both would be mixed.
func TestMySQLTenantBackendMustNotBeTheControlBackend(t *testing.T) {
	base := newConfiguration()
	base.BackendDB = "mysql"
	base.MySQLOrchestratorHost = "DB"
	base.MySQLOrchestratorPort = 3306
	base.MySQLOrchestratorDatabase = "orc_control"
	own := func(id, database string) string {
		return `{"ID":"` + id + `","Provider":"mysql","Backend":{"MySQLOrchestratorDatabase":"` + database + `"}}`
	}
	_, err := ParseTenantConfigs(base, []byte(`{"Tenants":[`+own("a", "orc_a")+`,`+own("b", "orc_control")+`]}`))
	if err == nil || !strings.Contains(err.Error(), `"b"`) || !strings.Contains(err.Error(), "control") {
		t.Errorf("tenant b in the control database: expected an error naming it, got %v", err)
	}
	// inheriting everything from the node = the control database
	_, err = ParseTenantConfigs(base, []byte(`{"Tenants":[{"ID":"a","Provider":"mysql"}]}`))
	if err == nil || !strings.Contains(err.Error(), "control") {
		t.Errorf("tenant inheriting the control database: expected an error, got %v", err)
	}
	if _, err = ParseTenantConfigs(base, []byte(`{"Tenants":[`+own("a", "orc_a")+`,`+own("b", "orc_b")+`]}`)); err != nil {
		t.Errorf("one database per tenant plus the control one is the supported layout: %v", err)
	}
}
