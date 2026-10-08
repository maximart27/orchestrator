package config

import (
	"reflect"
	"strings"
	"testing"
)

// TestFieldScopeCoversAllFields fails when a field is added to (or removed from) Configuration without
// updating fieldScope: every new setting must be consciously classified as global, node-local or tenant-scoped.
func TestFieldScopeCoversAllFields(t *testing.T) {
	typ := reflect.TypeOf(Configuration{})
	declared := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		declared[name] = true
		scope, ok := fieldScope[name]
		if !ok {
			t.Errorf("Configuration.%s is not classified: add it to fieldScope in field_scope.go (ScopeGlobal, ScopeNodeLocal, ScopeTenant or ScopeBackendNodeLocal)", name)
			continue
		}
		if scope < ScopeGlobal || scope > ScopeBackendNodeLocal {
			t.Errorf("Configuration.%s has an invalid scope %d", name, scope)
		}
	}
	for name := range fieldScope {
		if !declared[name] {
			t.Errorf("fieldScope has an entry for %s, which is not a field of Configuration", name)
		}
	}
	if len(fieldScope) != typ.NumField() {
		t.Errorf("fieldScope has %d entries, Configuration has %d fields", len(fieldScope), typ.NumField())
	}
}

func TestFieldScopeDecisions(t *testing.T) {
	expect := map[string]Scope{
		// tenant-scoped, including the DUDOSO decided as tenant
		"InstancePollSeconds":                 ScopeTenant,
		"MySQLTopologyUser":                   ScopeTenant,
		"PostgreSQLTopologyPassword":          ScopeTenant,
		"ProviderType":                        ScopeTenant,
		"ProxySQLAdminAddress":                ScopeTenant,
		"ProxySQLReaderHostgroup":             ScopeTenant,
		"ConsulAddress":                       ScopeTenant,
		"ZkAddress":                           ScopeTenant,
		"KVClusterMasterPrefix":               ScopeTenant,
		"DiscoveryMaxConcurrency":             ScopeTenant,
		"DeadInstanceDiscoveryMaxConcurrency": ScopeTenant,
		"DiscoveryQueueCapacity":              ScopeTenant,
		"TLSCacheTTLFactor":                   ScopeTenant,
		"ExpiryHostnameResolvesMinutes":       ScopeTenant,
		"AuditPurgeDays":                      ScopeTenant,
		"RemoveTextFromHostnameDisplay":       ScopeTenant,
		"MySQLConnectTimeoutSeconds":          ScopeTenant,
		"MySQLConnectionLifetimeSeconds":      ScopeTenant,
		// global (PowerAuth*/ReadOnly decided as global)
		"PowerAuthUsers":  ScopeGlobal,
		"PowerAuthGroups": ScopeGlobal,
		"ReadOnly":        ScopeGlobal,
		"RaftEnabled":     ScopeGlobal,
		"RaftNodes":       ScopeGlobal,
		"HTTPAuthUser":    ScopeGlobal,
		"URLPrefix":       ScopeGlobal,
		"WebMessage":      ScopeGlobal,
		// node-local
		"RaftBind":                ScopeNodeLocal,
		"ListenAddress":           ScopeNodeLocal,
		"SSLCertFile":             ScopeNodeLocal,
		"GraphiteAddr":            ScopeNodeLocal,
		"PrometheusEnabled":       ScopeNodeLocal,
		"InstanceWriteBufferSize": ScopeNodeLocal,
		"TenantsConfigFile":       ScopeNodeLocal,
		// backend node-local
		"BackendDB":                         ScopeBackendNodeLocal,
		"SQLite3DataFile":                   ScopeBackendNodeLocal,
		"MySQLOrchestratorHost":             ScopeBackendNodeLocal,
		"MySQLOrchestratorMaxAllowedPacket": ScopeBackendNodeLocal,
	}
	for field, want := range expect {
		got, ok := FieldScope(field)
		if !ok || got != want {
			t.Errorf("%s: scope %v, want %v", field, got, want)
		}
	}
	if !IsTenantScoped("InstancePollSeconds") || IsTenantScoped("RaftBind") || IsTenantScoped("NoSuchField") {
		t.Errorf("IsTenantScoped helper is wrong")
	}
	if !IsGlobal("ReadOnly") || !IsNodeLocal("RaftBind") || !IsBackendNodeLocal("SQLite3DataFile") {
		t.Errorf("scope helpers are wrong")
	}
	// every backend field belongs to the "Backend" section; MySQLOrchestrator* are all backend
	for _, f := range FieldsWithScope(ScopeBackendNodeLocal) {
		if f != "BackendDB" && f != "SQLite3DataFile" && !strings.HasPrefix(f, "MySQLOrchestrator") {
			t.Errorf("unexpected backend field %s", f)
		}
	}
	if n := len(FieldsWithScope(ScopeTenant)); n < 100 {
		t.Errorf("suspiciously few tenant-scoped fields: %d", n)
	}
}
