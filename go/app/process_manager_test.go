/*
   Copyright 2026 Orchestrator Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package app

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/logic"
)

// notLeader is a logic.Leadership that never leads: the tenants of a manager
// started in a test must not run leader-only work.
type notLeader struct{}

func (notLeader) IsLeader() bool         { return false }
func (notLeader) IsLeaderOrActive() bool { return false }

// testBase is the global configuration of a test: it describes the control
// backend (a SQLite file of its own) and is what the tenants inherit.
func testBase(t *testing.T) *config.Configuration {
	t.Helper()
	base := config.Config.Clone()
	base.BackendDB = "sqlite3"
	base.SQLite3DataFile = filepath.Join(t.TempDir(), "control.sqlite3")
	base.HostnameResolveMethod = "none"
	base.InstanceFlushIntervalMilliseconds = 20
	base.InstanceWriteBufferSize = 10
	return base
}

// writeTenantsFile writes a tenants file with one SQLite tenant per id, and
// makes base point to it. dataFile[id] overrides the SQLite file of a tenant.
func writeTenantsFile(t *testing.T, base *config.Configuration, ids []string, dataFile map[string]string) {
	t.Helper()
	dir := t.TempDir()
	var entries []string
	for _, id := range ids {
		file := filepath.Join(dir, id+".sqlite3")
		if f := dataFile[id]; f != "" {
			file = f
		}
		entries = append(entries, fmt.Sprintf(`{"ID": %q, "Provider": "mysql",
			"Backend": {"BackendDB": "sqlite3", "SQLite3DataFile": %q},
			"Config": {"KVClusterMasterPrefix": "%s/master/"}}`, id, file, id))
	}
	path := filepath.Join(dir, "tenants.json")
	if err := os.WriteFile(path, []byte(`{"Tenants": [`+strings.Join(entries, ",")+`]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	base.TenantsConfigFile = path
}

func TestBuildProcessManagerRejectsSeveralTenantsWithoutRaft(t *testing.T) {
	base := testBase(t)
	base.RaftEnabled = false
	writeTenantsFile(t, base, []string{"a", "b"}, nil)

	m, err := buildProcessManager(base)
	if err == nil {
		m.Stop()
		t.Fatal("two tenants without Raft must not start")
	}
	for _, want := range []string{"2 tenants", "Raft"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestBuildProcessManagerAcceptsSeveralTenantsWithRaft(t *testing.T) {
	base := testBase(t)
	base.RaftEnabled = true
	writeTenantsFile(t, base, []string{"a", "b"}, nil)

	m, err := buildProcessManager(base)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	if got := len(m.All()); got != 2 {
		t.Errorf("tenants = %d, want 2", got)
	}
}

func TestBuildProcessManagerNamesTheFailingTenant(t *testing.T) {
	base := testBase(t)
	base.RaftEnabled = true
	writeTenantsFile(t, base, []string{"good", "bad"}, map[string]string{
		"bad": filepath.Join(t.TempDir(), "no-such-dir", "deeper", "bad.sqlite3"),
	})

	m, err := buildProcessManager(base)
	if err == nil {
		m.Stop()
		t.Fatal("a tenant with an unusable backend must make the process fail")
	}
	if !strings.Contains(err.Error(), `tenant "bad"`) {
		t.Errorf("error %q does not name the failing tenant", err)
	}
}

func TestBuildProcessManagerReportsAnUnreadableTenantsFile(t *testing.T) {
	base := testBase(t)
	base.TenantsConfigFile = filepath.Join(t.TempDir(), "missing.json")
	if m, err := buildProcessManager(base); err == nil {
		m.Stop()
		t.Fatal("a missing tenants file must make the process fail")
	} else if !strings.Contains(err.Error(), "missing.json") {
		t.Errorf("error %q does not name the tenants file", err)
	}
}

func TestStartManagerRunsAndStopsCleanly(t *testing.T) {
	base := testBase(t)
	base.RaftEnabled = false
	writeTenantsFile(t, base, []string{"a", "b"}, nil)

	// WithoutProcessBootstrap: no Raft, signals nor metrics exporters in a unit test
	m, err := buildProcessManager(base, logic.WithoutProcessBootstrap(), logic.WithLeadership(notLeader{}))
	if err != nil {
		t.Fatal(err)
	}
	stop, err := startManager(m)
	if err != nil {
		m.Stop()
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	stop()
	stop() // idempotent

	select {
	case <-m.Done():
	default:
		t.Error("Done must be closed once the manager is stopped")
	}
	a, _ := m.Get("a")
	if _, err := a.Backend.Exec("select 1"); err == nil {
		t.Error("the backend of a stopped tenant must be closed")
	}
}

func TestCheckAgentsSupport(t *testing.T) {
	cfg := config.Config.Clone()
	cfg.ServeAgentsHttp = true
	if err := checkAgentsSupport(cfg); err != nil {
		t.Errorf("agents in the legacy single-tenant setup are supported: %v", err)
	}
	cfg.TenantsConfigFile = "/etc/orchestrator-tenants.json"
	if err := checkAgentsSupport(cfg); err == nil {
		t.Error("agents together with a tenants file are not supported")
	}
	cfg.ServeAgentsHttp = false
	if err := checkAgentsSupport(cfg); err != nil {
		t.Errorf("a tenants file without agents is fine: %v", err)
	}
}
