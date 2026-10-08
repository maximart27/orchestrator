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
	"io"
	"os"
	"sort"
	"strings"

	"github.com/proxysql/golib/log"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/logic"
)

// TenantEnvVar is the environment variable that selects the tenant of a CLI
// invocation. The -tenant flag wins over it.
const TenantEnvVar = "ORCHESTRATOR_TENANT"

// cliSession is what a CLI invocation runs against: ONE tenant, resolved once
// at the start. A single-command process never needs more.
type cliSession struct {
	// cfg is the configuration of the selected tenant; for the commands that need
	// no tenant it is the global configuration.
	cfg *config.Configuration
	// manager owns the tenants built for the invocation. nil for the commands
	// that touch no backend (help, dump-config, tenants).
	manager *logic.TenantManager
	// tenant is the tenant the command acts on. nil if the command needs none:
	// the commands that touch no backend, and those of the node (active-nodes,
	// access-token, continuous) when no single tenant can be assumed.
	tenant *logic.Tenant
}

// close releases the backends the session opened.
func (s *cliSession) close() {
	if s.manager != nil {
		s.manager.Stop()
	}
}

// selectTenantID picks the tenant a CLI invocation acts on among the available
// ones: the requested one (-tenant, ORCHESTRATOR_TENANT) if any; otherwise the
// tenant "default" or the only tenant, exactly as TenantManager.Default does.
// It fails -- naming the available tenants -- if the requested tenant does not
// exist or if the choice is ambiguous.
func selectTenantID(available []string, requested string) (string, error) {
	ids := append([]string(nil), available...)
	sort.Strings(ids)
	list := strings.Join(ids, ", ")
	if requested != "" {
		for _, id := range ids {
			if id == requested {
				return id, nil
			}
		}
		return "", fmt.Errorf("tenant %q does not exist; available tenants: %s", requested, list)
	}
	for _, id := range ids {
		if id == config.DefaultTenantID {
			return id, nil
		}
	}
	if len(ids) == 1 {
		return ids[0], nil
	}
	return "", fmt.Errorf("%d tenants are configured (%s) and none is named %q: choose one with -tenant <id> or the %s environment variable", len(ids), list, config.DefaultTenantID, TenantEnvVar)
}

// cliRequestedTenant is the tenant asked for by the flag or, failing that, by the environment.
func cliRequestedTenant(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	return strings.TrimSpace(os.Getenv(TenantEnvVar))
}

// nodeCommands are the commands about the node, not about a tenant: they work
// even when no tenant can be assumed.
var nodeCommands = map[string]bool{"active-nodes": true, "access-token": true, "continuous": true}

// openCliSession resolves the tenant of a CLI invocation and builds what the
// command needs. tenantFlag is the value of -tenant ("" if absent).
//
// It builds a TenantManager WithoutProcessBootstrap -- no Raft, signal
// handlers nor metrics exporters, which belong to a long-running process --
// except for "continuous", which is the process itself and runs every tenant.
func openCliSession(tenantFlag string, command string) (*cliSession, error) {
	requested := cliRequestedTenant(tenantFlag)
	switch command {
	case "help", "tenants":
		return &cliSession{cfg: config.Config}, nil
	case "dump-config":
		cfg, err := configForDump(requested)
		if err != nil {
			return nil, err
		}
		return &cliSession{cfg: cfg}, nil
	case "redeploy-internal-db":
		// forces the schema deploy that building the tenants performs
		config.RuntimeCLIFlags.ConfiguredVersion = ""
	}

	var opts []logic.ManagerOption
	if command != "continuous" {
		opts = append(opts, logic.WithoutProcessBootstrap())
	}
	manager, err := buildProcessManager(config.Config, opts...)
	if err != nil {
		return nil, err
	}
	session := &cliSession{cfg: config.Config, manager: manager}
	if command == "continuous" {
		logic.SetDefaultManager(manager)
		if requested != "" {
			log.Warningf("continuous mode runs every tenant: -tenant / %s is ignored", TenantEnvVar)
		}
	}

	var ids []string
	for _, t := range manager.All() {
		ids = append(ids, t.ID)
	}
	id, err := selectTenantID(ids, requested)
	if err != nil {
		if nodeCommands[command] && requested == "" {
			return session, nil
		}
		manager.Stop()
		return nil, err
	}
	session.tenant, _ = manager.Get(id)
	session.cfg = session.tenant.Cfg
	return session, nil
}

// mustOpenCliSession is openCliSession, fatal on error.
func mustOpenCliSession(tenantFlag string, command string) *cliSession {
	session, err := openCliSession(tenantFlag, command)
	if err != nil {
		_ = log.Fatale(err)
	}
	return session
}

// configForDump returns the configuration dump-config prints: that of the
// selected tenant, or the global one when there is no tenants file or the
// choice is ambiguous. It opens no backend.
func configForDump(requested string) (*config.Configuration, error) {
	if requested == "" && config.Config.TenantsConfigFile == "" {
		return config.Config, nil
	}
	tenants, err := config.LoadTenantConfigs(config.Config)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, tc := range tenants {
		ids = append(ids, tc.ID)
	}
	id, err := selectTenantID(ids, requested)
	if err != nil {
		if requested == "" {
			return config.Config, nil
		}
		return nil, err
	}
	for _, tc := range tenants {
		if tc.ID == id {
			return tc.Cfg, nil
		}
	}
	return config.Config, nil
}

// runContinuous is the CLI command "continuous": the discovery, analysis and
// recovery of every tenant, until the process is interrupted.
func runContinuous(session *cliSession) {
	stop, err := startManager(session.manager)
	if err != nil {
		_ = log.Fatale(err)
	}
	shutdownOnInterrupt(session.manager, stop)
	<-session.manager.Done()
}

// backendDescription is a one-line, password-free description of the backend of cfg.
func backendDescription(cfg *config.Configuration) string {
	if cfg.IsSQLite() {
		return "sqlite3:" + cfg.SQLite3DataFile
	}
	return fmt.Sprintf("mysql:%s:%d/%s", cfg.MySQLOrchestratorHost, cfg.MySQLOrchestratorPort, cfg.MySQLOrchestratorDatabase)
}

// printTenants writes one line per tenant of base -- ID, provider and backend,
// tab separated, sorted by ID. It opens no backend.
func printTenants(w io.Writer, base *config.Configuration) error {
	tenants, err := config.LoadTenantConfigs(base)
	if err != nil {
		return err
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i].ID < tenants[j].ID })
	for _, tc := range tenants {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\n", tc.ID, tc.Provider, backendDescription(tc.Cfg)); err != nil {
			return err
		}
	}
	return nil
}
