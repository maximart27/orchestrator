package logic

import (
	"fmt"
	"testing"

	"github.com/proxysql/orchestrator/go/config"
)

func parseTenantID(t *testing.T, id string) error {
	t.Helper()
	base := testBase(t)
	_, err := config.ParseTenantConfigs(base, []byte(fmt.Sprintf(`{"Tenants": [{"ID": %q, "Provider": "mysql",
		"Backend": {"BackendDB": "sqlite3", "SQLite3DataFile": %q}}]}`, id, t.TempDir()+"/x.sqlite3")))
	return err
}

// The ID of the dedicated control backend, and the IDs the inert legacy defaults
// use, must never be accepted as tenant IDs: they would clash with a tenant.
func TestReservedIDsCannotBeTenants(t *testing.T) {
	for _, id := range []string{controlBackendID, "_default-disabled", "_anything"} {
		if err := parseTenantID(t, id); err == nil {
			t.Errorf("tenant ID %q must be rejected", id)
		}
	}
	// "default" is the tenant of the legacy setup and a valid ID of a tenants file
	if err := parseTenantID(t, config.DefaultTenantID); err != nil {
		t.Errorf("tenant ID %q must be accepted: %v", config.DefaultTenantID, err)
	}
}
