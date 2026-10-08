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
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/proxysql/golib/log"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/logic"
)

// shutdownTimeout is how long an interrupted process waits for the tenants to
// stop before it gives up and exits.
const shutdownTimeout = 30 * time.Second

// buildProcessManager builds the TenantManager of an orchestrator process
// (http, continuous): the tenants of base.TenantsConfigFile or, without it, the
// single legacy tenant "default" driven by base. It is fail-fast: a tenant that
// cannot be initialised, or more than one tenant without Raft, is an error that
// names the culprit, and nothing is left open.
//
// The manager must be built BEFORE process.ContinuousRegistration: the control
// of the node (node health, election) is the one of the manager, and the
// manager installs it as the process default.
func buildProcessManager(base *config.Configuration, opts ...logic.ManagerOption) (*logic.TenantManager, error) {
	m, err := logic.NewTenantManager(base, opts...)
	if err != nil {
		return nil, fmt.Errorf("cannot initialise the tenants: %w", err)
	}
	return m, nil
}

// checkAgentsSupport tells whether the agents subsystem can run with this
// configuration. It is not multi-tenant: it works on the process-wide backend
// and store, so it is only available in the legacy single-tenant setup.
func checkAgentsSupport(cfg *config.Configuration) error {
	if cfg.ServeAgentsHttp && cfg.TenantsConfigFile != "" {
		return errors.New("ServeAgentsHttp is not supported together with TenantsConfigFile: the agents (seeds) subsystem works on the legacy single-tenant backend only")
	}
	return nil
}

// startManager launches the node machinery and the tenants of m (Raft included).
// The returned function stops them in order, and is what the interrupt signal
// runs.
func startManager(m *logic.TenantManager) (stop func(), err error) {
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		cancel()
		m.Stop()
		return nil, err
	}
	return func() {
		cancel()
		m.Stop()
	}, nil
}

// shutdownOnInterrupt stops the manager, in order, when the process receives
// SIGINT, and then exits. SIGTERM is NOT handled here: the process-wide signal
// handler of the logic package (logic.acceptSignals) already owns SIGHUP and
// SIGTERM.
func shutdownOnInterrupt(m *logic.TenantManager, stop func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		log.Infof("Received SIGINT. Shutting down orchestrator")
		for _, t := range m.All() {
			_ = t.Store.AuditOperation("shutdown", nil, "Triggered via SIGINT")
		}
		stopped := make(chan struct{})
		go func() {
			stop()
			close(stopped)
		}()
		select {
		case <-stopped:
			os.Exit(0)
		case <-time.After(shutdownTimeout):
			_ = log.Errorf("orchestrator did not stop within %v; exiting", shutdownTimeout)
			os.Exit(1)
		}
	}()
}
