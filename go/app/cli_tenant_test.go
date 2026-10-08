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
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/logic"
)

func TestSelectTenantID(t *testing.T) {
	tests := []struct {
		name      string
		available []string
		requested string
		want      string
		wantErr   []string // substrings of the error
	}{
		{name: "the requested tenant", available: []string{"a", "b"}, requested: "b", want: "b"},
		{name: "the requested tenant wins over default", available: []string{"default", "a"}, requested: "a", want: "a"},
		{name: "default when present", available: []string{"a", "default", "b"}, want: "default"},
		{name: "the only tenant", available: []string{"only"}, want: "only"},
		{name: "ambiguous", available: []string{"b", "a"}, wantErr: []string{"2 tenants", "a, b", "-tenant", TenantEnvVar}},
		{name: "unknown tenant", available: []string{"b", "a"}, requested: "c", wantErr: []string{`tenant "c" does not exist`, "available tenants: a, b"}},
		{name: "unknown tenant in legacy", available: []string{"default"}, requested: "x", wantErr: []string{`tenant "x" does not exist`, "available tenants: default"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectTenantID(tt.available, tt.requested)
			if len(tt.wantErr) > 0 {
				if err == nil {
					t.Fatalf("got %q, want an error", got)
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not contain %q", err, want)
					}
				}
				return
			}
			if err != nil || got != tt.want {
				t.Errorf("got (%q, %v), want %q", got, err, tt.want)
			}
		})
	}
}

func TestCliRequestedTenantFlagWinsOverEnv(t *testing.T) {
	t.Setenv(TenantEnvVar, "from-env")
	if got := cliRequestedTenant("from-flag"); got != "from-flag" {
		t.Errorf("the flag must win, got %q", got)
	}
	if got := cliRequestedTenant(""); got != "from-env" {
		t.Errorf("without the flag the environment applies, got %q", got)
	}
	t.Setenv(TenantEnvVar, "")
	if got := cliRequestedTenant(""); got != "" {
		t.Errorf("no flag nor environment means no request, got %q", got)
	}
}

// useGlobalConfig makes cfg the process-wide configuration for the test, and
// gives the CLI flags the values main would have set.
func useGlobalConfig(t *testing.T, cfg *config.Configuration) {
	t.Helper()
	oldConfig := config.Config
	oldSkip, oldIgnore := config.RuntimeCLIFlags.SkipContinuousRegistration, config.RuntimeCLIFlags.IgnoreRaftSetup
	skip, ignore := true, false
	config.Config = cfg
	config.RuntimeCLIFlags.SkipContinuousRegistration = &skip
	config.RuntimeCLIFlags.IgnoreRaftSetup = &ignore
	t.Cleanup(func() {
		config.Config = oldConfig
		config.RuntimeCLIFlags.SkipContinuousRegistration = oldSkip
		config.RuntimeCLIFlags.IgnoreRaftSetup = oldIgnore
	})
}

type seededInstance struct {
	host    string
	port    int
	cluster string
}

// seedTenants writes instances in the backends of the tenants of base.
func seedTenants(t *testing.T, base *config.Configuration, data map[string][]seededInstance) {
	t.Helper()
	m, err := logic.NewTenantManager(base, logic.WithoutProcessBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	for id, instances := range data {
		tn, ok := m.Get(id)
		if !ok {
			t.Fatalf("tenant %s not found", id)
		}
		for _, i := range instances {
			instance := &inst.Instance{Key: inst.InstanceKey{Hostname: i.host, Port: i.port}, ClusterName: i.cluster}
			if err := tn.Store.WriteInstance(instance, false, nil); err != nil {
				t.Fatalf("tenant %s: WriteInstance: %v", id, err)
			}
		}
	}
}

// twoTenants configures, and seeds, two tenants that both know db1:3306 -- in a different cluster.
func twoTenants(t *testing.T) *config.Configuration {
	t.Helper()
	base := testBase(t)
	writeTenantsFile(t, base, []string{"mariadb-a", "mariadb-b"}, nil)
	useGlobalConfig(t, base)
	seedTenants(t, base, map[string][]seededInstance{
		"mariadb-a": {{"db1", 3306, "cluster-of-a"}, {"only-in-a", 3306, "cluster-of-a"}},
		"mariadb-b": {{"db1", 3306, "cluster-of-b"}},
	})
	return base
}

func runCli(tenantID, command, instance string) string {
	return strings.TrimSpace(captureStdout(func() {
		CliWrapperForTenant(tenantID, command, false, instance, "", "tester", "", "", "", "", "", "")
	}))
}

// captureStdout returns what f prints on the standard output.
func captureStdout(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	out := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		out <- string(b)
	}()
	defer func() { os.Stdout = old }()
	f()
	_ = w.Close()
	return <-out
}

func TestCliMultiTenant(t *testing.T) {
	base := twoTenants(t)

	t.Run("commands act on the selected tenant", func(t *testing.T) {
		if got := runCli("mariadb-a", "clusters", ""); got != "cluster-of-a" {
			t.Errorf("clusters of mariadb-a = %q", got)
		}
		if got := runCli("mariadb-b", "clusters", ""); got != "cluster-of-b" {
			t.Errorf("clusters of mariadb-b = %q", got)
		}
		// the same instance key, a different answer per tenant
		if got := runCli("mariadb-a", "which-cluster", "db1:3306"); got != "cluster-of-a" {
			t.Errorf("which-cluster db1:3306 in mariadb-a = %q", got)
		}
		if got := runCli("mariadb-b", "which-cluster", "db1:3306"); got != "cluster-of-b" {
			t.Errorf("which-cluster db1:3306 in mariadb-b = %q", got)
		}
	})

	t.Run("tenant from the environment, the flag wins", func(t *testing.T) {
		t.Setenv(TenantEnvVar, "mariadb-b")
		if got := runCli("", "clusters", ""); got != "cluster-of-b" {
			t.Errorf("the environment must select the tenant, got %q", got)
		}
		if got := runCli("mariadb-a", "clusters", ""); got != "cluster-of-a" {
			t.Errorf("the flag must win over the environment, got %q", got)
		}
	})

	t.Run("an ambiguous or unknown tenant fails clearly", func(t *testing.T) {
		_, err := openCliSession("", "clusters")
		if err == nil {
			t.Fatal("two tenants and no selection must be an error")
		}
		for _, want := range []string{"mariadb-a", "mariadb-b", "-tenant", TenantEnvVar} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q", err, want)
			}
		}
		if _, err = openCliSession("nope", "clusters"); err == nil || !strings.Contains(err.Error(), "available tenants: mariadb-a, mariadb-b") {
			t.Errorf("an unknown tenant must list the available ones, got %v", err)
		}
	})

	t.Run("the commands of the node need no tenant", func(t *testing.T) {
		session, err := openCliSession("", "active-nodes")
		if err != nil {
			t.Fatalf("active-nodes needs no tenant: %v", err)
		}
		defer session.close()
		if session.tenant != nil || session.manager == nil {
			t.Errorf("active-nodes must run on the node: tenant=%v manager=%v", session.tenant, session.manager)
		}
	})

	t.Run("the session carries the configuration of the tenant", func(t *testing.T) {
		session, err := openCliSession("mariadb-b", "clusters")
		if err != nil {
			t.Fatal(err)
		}
		defer session.close()
		if session.tenant == nil || session.tenant.ID != "mariadb-b" {
			t.Fatalf("tenant = %v", session.tenant)
		}
		if session.cfg != session.tenant.Cfg || session.cfg == config.Config {
			t.Error("the session must carry the configuration of the tenant, not the global one")
		}
	})

	t.Run("tenants lists them without opening a backend", func(t *testing.T) {
		var out bytes.Buffer
		if err := printTenants(&out, base); err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		if len(lines) != 2 {
			t.Fatalf("tenants output = %q", out.String())
		}
		for i, id := range []string{"mariadb-a", "mariadb-b"} {
			fields := strings.Split(lines[i], "\t")
			if len(fields) != 3 || fields[0] != id || fields[1] != config.ProviderMySQL || !strings.HasPrefix(fields[2], "sqlite3:") {
				t.Errorf("line %d = %q", i, lines[i])
			}
		}
		if got := runCli("", "tenants", ""); got != strings.TrimSpace(out.String()) {
			t.Errorf("-c tenants = %q, want %q", got, strings.TrimSpace(out.String()))
		}
	})

	t.Run("dump-config is that of the tenant", func(t *testing.T) {
		dump := func(tenantID string) interface{} {
			var out map[string]interface{}
			if err := json.Unmarshal([]byte(runCli(tenantID, "dump-config", "")), &out); err != nil {
				t.Fatalf("dump-config -tenant %q: %v", tenantID, err)
			}
			return out["KVClusterMasterPrefix"]
		}
		if got := dump("mariadb-a"); got != "mariadb-a/master/" {
			t.Errorf("KVClusterMasterPrefix of mariadb-a = %v", got)
		}
		if got := dump("mariadb-b"); got != "mariadb-b/master/" {
			t.Errorf("KVClusterMasterPrefix of mariadb-b = %v", got)
		}
		// ambiguous: the global configuration, as dump-config always printed
		if got := dump(""); got == "mariadb-a/master/" || got == "mariadb-b/master/" {
			t.Errorf("without a selection dump-config prints the global configuration, got %v", got)
		}
	})
}

// The tests below run the CLI in a child process: it is the real thing -- a
// configuration file, log.Fatal exiting the process -- and the legacy
// single-tenant setup uses process-wide defaults that must not leak between tests.

const helperEnv = "ORC_APP_TEST_CLI_HELPER"

// TestCliHelperProcess is not a test: it is the child process of runCliProcess.
func TestCliHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("child process of the CLI tests")
	}
	config.ForceRead(os.Getenv("ORC_APP_TEST_CONFIG"))
	skip, ignore := true, false
	config.RuntimeCLIFlags.SkipContinuousRegistration = &skip
	config.RuntimeCLIFlags.IgnoreRaftSetup = &ignore
	config.MarkConfigurationLoaded()

	if seed := os.Getenv("ORC_APP_TEST_SEED"); seed != "" { // db1:3306=legacy-cluster
		hostPort, cluster, _ := strings.Cut(seed, "=")
		host, portText, _ := strings.Cut(hostPort, ":")
		port, _ := strconv.Atoi(portText)
		tn := logic.DefaultTenant()
		instance := &inst.Instance{Key: inst.InstanceKey{Hostname: host, Port: port}, ClusterName: cluster}
		if err := tn.Store.WriteInstance(instance, false, nil); err != nil {
			t.Fatal(err)
		}
	}
	CliWrapperForTenant(os.Getenv("ORC_APP_TEST_TENANT"), os.Getenv("ORC_APP_TEST_CMD"), false, os.Getenv("ORC_APP_TEST_INSTANCE"), "", "tester", "", "", "", "", "", "")
	os.Exit(0)
}

type cliProcess struct {
	config    string
	seed      string
	tenant    string
	envTenant string
	command   string
	instance  string
}

// runCliProcess runs the command in a child process and returns its stdout,
// stderr and exit code.
func runCliProcess(t *testing.T, p cliProcess) (stdout, stderr string, exitCode int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCliHelperProcess$")
	cmd.Env = append(os.Environ(),
		helperEnv+"=1",
		"ORC_APP_TEST_CONFIG="+p.config,
		"ORC_APP_TEST_SEED="+p.seed,
		"ORC_APP_TEST_TENANT="+p.tenant,
		"ORC_APP_TEST_CMD="+p.command,
		"ORC_APP_TEST_INSTANCE="+p.instance,
		TenantEnvVar+"="+p.envTenant,
	)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		exitCode = exitErr.ExitCode()
	default:
		t.Fatal(err)
	}
	return strings.TrimSpace(out.String()), errOut.String(), exitCode
}

func writeLegacyConfig(t *testing.T) (configFile, dataFile string) {
	t.Helper()
	dir := t.TempDir()
	dataFile = filepath.Join(dir, "orchestrator.sqlite3")
	configFile = filepath.Join(dir, "orchestrator.conf.json")
	body, _ := json.Marshal(map[string]interface{}{
		"BackendDB":             "sqlite3",
		"SQLite3DataFile":       dataFile,
		"HostnameResolveMethod": "none",
	})
	if err := os.WriteFile(configFile, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return configFile, dataFile
}

// The legacy single-tenant CLI -- no tenants file, no -tenant -- keeps working as ever.
func TestCliLegacyWithoutTenants(t *testing.T) {
	configFile, dataFile := writeLegacyConfig(t)

	out, stderr, code := runCliProcess(t, cliProcess{config: configFile, seed: "db1:3306=legacy-cluster", command: "clusters"})
	if code != 0 || out != "legacy-cluster" {
		t.Fatalf("clusters: exit %d, stdout %q, stderr %q", code, out, stderr)
	}
	out, stderr, code = runCliProcess(t, cliProcess{config: configFile, command: "which-cluster", instance: "db1:3306"})
	if code != 0 || out != "legacy-cluster" {
		t.Errorf("which-cluster: exit %d, stdout %q, stderr %q", code, out, stderr)
	}
	// the one tenant is "default": naming it is accepted, in the flag or in the environment
	out, _, code = runCliProcess(t, cliProcess{config: configFile, tenant: "default", command: "clusters"})
	if code != 0 || out != "legacy-cluster" {
		t.Errorf("-tenant default: exit %d, stdout %q", code, out)
	}
	out, _, code = runCliProcess(t, cliProcess{config: configFile, envTenant: "default", command: "clusters"})
	if code != 0 || out != "legacy-cluster" {
		t.Errorf("ORCHESTRATOR_TENANT=default: exit %d, stdout %q", code, out)
	}
	out, _, code = runCliProcess(t, cliProcess{config: configFile, command: "tenants"})
	if want := "default\tmysql\tsqlite3:" + dataFile; code != 0 || out != want {
		t.Errorf("tenants: exit %d, stdout %q, want %q", code, out, want)
	}
	_, stderr, code = runCliProcess(t, cliProcess{config: configFile, tenant: "other", command: "clusters"})
	if code == 0 || !strings.Contains(stderr, `tenant "other" does not exist; available tenants: default`) {
		t.Errorf("an unknown tenant must fail and list the available ones: exit %d, stderr %q", code, stderr)
	}
}

func TestCliMultiTenantProcessErrors(t *testing.T) {
	dir := t.TempDir()
	base := testBase(t)
	writeTenantsFile(t, base, []string{"mariadb-a", "mariadb-b"}, nil)
	configFile := filepath.Join(dir, "orchestrator.conf.json")
	body, _ := json.Marshal(map[string]interface{}{
		"BackendDB":             "sqlite3",
		"SQLite3DataFile":       base.SQLite3DataFile,
		"HostnameResolveMethod": "none",
		"TenantsConfigFile":     base.TenantsConfigFile,
	})
	if err := os.WriteFile(configFile, body, 0o600); err != nil {
		t.Fatal(err)
	}

	_, stderr, code := runCliProcess(t, cliProcess{config: configFile, command: "clusters"})
	if code == 0 {
		t.Fatal("two tenants and no selection must fail")
	}
	for _, want := range []string{"mariadb-a", "mariadb-b", "-tenant", TenantEnvVar} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr %q does not contain %q", stderr, want)
		}
	}
	// the selection removes the ambiguity
	out, stderr, code := runCliProcess(t, cliProcess{config: configFile, tenant: "mariadb-a", command: "clusters"})
	if code != 0 || out != "" {
		t.Errorf("-tenant mariadb-a: exit %d, stdout %q, stderr %q", code, out, stderr)
	}
	// listing the tenants needs no selection
	out, _, code = runCliProcess(t, cliProcess{config: configFile, command: "tenants"})
	if code != 0 || strings.Count(out, "\n") != 1 {
		t.Errorf("tenants: exit %d, stdout %q", code, out)
	}
}
