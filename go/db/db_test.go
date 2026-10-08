package db

import (
	"strings"
	"testing"

	"github.com/proxysql/orchestrator/go/config"
)

// TestMatchDSN tests that the dsns we match don't expose the password
func TestMatchDSN(t *testing.T) {
	var tests = []struct {
		dsn    string
		output string
		err    error
	}{
		{"user@tcp(host:3306)/", "user@tcp(host:3306)/", nil},
		{"user:pass@tcp(host:3306)/", "user:?@tcp(host:3306)/", nil},
		{"user@tcp(host:3306)/db", "user@tcp(host:3306)/db", nil},
		{"user:pass@tcp(host:3306)/db", "user:?@tcp(host:3306)/db", nil},
		{"user:pass@tcp(host:3306)/db?param1=true", "user:?@tcp(host:3306)/db?param1=true", nil},
		{"user:pass@tcp(host:3306)/db?param1=true&param2=10", "user:?@tcp(host:3306)/db?param1=true&param2=10", nil},
		// tricky ones
		{"user:user:pass@tcp(host:3306)/db?param1=true&param2=10", "user:?@tcp(host:3306)/db?param1=true&param2=10", nil},
		{"user:pass@pass@tcp(host:3306)/db?param1=true&param2=10", "user:?@tcp(host:3306)/db?param1=true&param2=10", nil},
	}

	for i := range tests {
		match, err := matchDSN(tests[i].dsn)
		if match != tests[i].output || err != tests[i].err {
			t.Errorf("Failed to match %q: expected(%q,%v), got(%q,%v)",
				tests[i].dsn, tests[i].output, tests[i].err, match, err)
		}
	}
}

// TestTopologyMaxAllowedPacketDoesNotLeakIntoBackendURI guards against
// openTopology writing the topology maxAllowedPacket into the backend DSN.
func TestTopologyMaxAllowedPacketDoesNotLeakIntoBackendURI(t *testing.T) {
	prev := config.Config.MySQLTopologyMaxAllowedPacket
	defer func() { config.Config.MySQLTopologyMaxAllowedPacket = prev }()
	config.Config.MySQLTopologyMaxAllowedPacket = 4242

	if _, err := OpenTopology("127.0.0.1", 3306); err != nil {
		t.Fatalf("OpenTopology: %v", err)
	}
	uri := getMySQLURI()
	if strings.Contains(uri, "maxAllowedPacket=4242") {
		t.Errorf("backend DSN picked up the topology maxAllowedPacket: %q", uri)
	}
	if !strings.Contains(uri, "@tcp(") {
		t.Errorf("backend DSN looks corrupted: %q", uri)
	}
}
