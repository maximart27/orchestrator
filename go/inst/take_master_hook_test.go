package inst

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/proxysql/orchestrator/go/config"
)

func TestTakeMasterHookKnowsItsTenant(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	s := newTestStore(t, "hook-tenant", func(c *config.Configuration) {
		c.PostTakeMasterProcesses = []string{`echo "$ORC_TENANT_ID {tenantId}" > ` + out}
	})
	s.TakeMasterHook(testInstance(InstanceKey{Hostname: "new", Port: 3306}), testInstance(InstanceKey{Hostname: "old", Port: 3306}))
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != "hook-tenant hook-tenant" {
		t.Fatalf("hook saw %q", got)
	}
}
