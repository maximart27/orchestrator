package inst

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/config"
)

// The deduplicated-log cache is process wide: an error logged for tenant A must
// not silence the same error of tenant B.
func TestLogReadTopologyInstanceErrorIsPerTenant(t *testing.T) {
	a := newTestStore(t, "log-a", nil)
	b := newTestStore(t, "log-b", nil)
	// the cache is process wide and lives a minute: use a key unique to this run
	key := &InstanceKey{Hostname: fmt.Sprintf("overlapping-ip-%d", time.Now().UnixNano()), Port: 3306}
	boom := errors.New("boom")

	if got := a.logReadTopologyInstanceError(key, "hint", boom); got == nil {
		t.Fatal("expected an error")
	}
	// logged for a: a second identical call of a is deduplicated (returns the bare error)
	if got := a.logReadTopologyInstanceError(key, "hint", boom); got != boom {
		t.Errorf("a second time must be suppressed, got %v", got)
	}
	// ...but b must still log its own
	if got := b.logReadTopologyInstanceError(key, "hint", boom); got == boom {
		t.Error("tenant b's error was silenced by tenant a's")
	}
}

func TestStoreLogKeyAndPrefix(t *testing.T) {
	a := newTestStore(t, "log-key", nil)
	if a.logKey("k") != "log-key/k" || a.logPrefix() != "[tenant:log-key] " {
		t.Errorf("unexpected scoping: %q %q", a.logKey("k"), a.logPrefix())
	}
	d := &Store{id: config.DefaultTenantID}
	if d.logKey("k") != "k" || d.logPrefix() != "" {
		t.Errorf("the default tenant must keep the unscoped behaviour: %q %q", d.logKey("k"), d.logPrefix())
	}
}

func TestAuditLogFileIdentifiesNonDefaultTenants(t *testing.T) {
	file := filepath.Join(t.TempDir(), "audit.log")
	s := newTestStore(t, "audit-tenant", func(c *config.Configuration) { c.AuditLogFile = file })
	k := InstanceKey{Hostname: "db1", Port: 3306}
	if err := s.AuditOperation("test", &k, "hello"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, _ := os.ReadFile(file) // written by a goroutine
		if strings.Contains(string(data), "[tenant:audit-tenant] hello") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("audit file does not identify the tenant: %q", data)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
