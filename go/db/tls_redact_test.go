package db

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/proxysql/orchestrator/go/config"
)

func TestTLSSetupErrorsDoNotLeakPassword(t *testing.T) {
	const secret = "SuperSecret123"
	cfg := config.Config.Clone()
	cfg.BackendDB = "sqlite3"
	cfg.SQLite3DataFile = filepath.Join(t.TempDir(), "x.db")
	cfg.MySQLTopologySSLCAFile = "/nonexistent/ca.pem"
	cfg.MySQLOrchestratorSSLCAFile = "/nonexistent/ca.pem"
	b, err := NewBackend("tls-redact", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	for _, uri := range []string{
		"orchestrator:" + secret + "@tcp(db1:3306)/?timeout=1s",
		"not a dsn but has " + secret,
	} {
		if _, err := b.setupTopologyTLS(uri); err == nil || strings.Contains(err.Error(), secret) {
			t.Errorf("topology TLS error for %q: %v", uri, err)
		}
		if _, err := b.setupOrchestratorTLS(uri); err == nil || strings.Contains(err.Error(), secret) {
			t.Errorf("orchestrator TLS error for %q: %v", uri, err)
		}
	}
}
