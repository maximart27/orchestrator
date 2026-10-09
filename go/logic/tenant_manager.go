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

package logic

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/proxysql/golib/log"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
	"github.com/proxysql/orchestrator/go/inst"
	ometrics "github.com/proxysql/orchestrator/go/metrics"
	"github.com/proxysql/orchestrator/go/process"
)

// The legacy defaults -- db.DefaultBackend() and inst.DefaultStore(), driven by
// the global config.Config -- belong to the legacy single-tenant setup. A manager
// built from a tenants file never uses them (even its tenant "default" has its
// own backend and Store), so while one is alive they are disabled: a call that
// was not migrated to its tenant fails with an error instead of creating a
// schema on, or exiting the process because of, the base backend. The disabling
// is reference counted, since several managers can coexist in a process (tests,
// CLI helpers).
var legacyDefaults struct {
	mu   sync.Mutex
	held int
}

func disableLegacyDefaults() {
	legacyDefaults.mu.Lock()
	defer legacyDefaults.mu.Unlock()
	if legacyDefaults.held == 0 {
		db.DisableDefaultBackend()
		inst.DisableDefaultStore()
	}
	legacyDefaults.held++
}

func enableLegacyDefaults() {
	legacyDefaults.mu.Lock()
	defer legacyDefaults.mu.Unlock()
	legacyDefaults.held--
	if legacyDefaults.held == 0 {
		db.EnableDefaultBackend()
		inst.EnableDefaultStore()
	}
}

// controlBackendID names the dedicated control backend of a multi-tenant node
// that has no "default" tenant. It starts with an underscore, which no tenant
// ID may (see config.LoadTenantConfigs), so it never clashes with a tenant.
const controlBackendID = "_control"

// Leadership tells whether this node leads (or, with Raft, takes part in the
// quorum). It is global to the process: there is one Raft group, one leader,
// for every tenant. The production implementation is the package-level
// IsLeader/IsLeaderOrActive; tests inject their own.
type Leadership interface {
	IsLeader() bool
	IsLeaderOrActive() bool
}

type processLeadership struct{}

func (processLeadership) IsLeader() bool         { return IsLeader() }
func (processLeadership) IsLeaderOrActive() bool { return IsLeaderOrActive() }

// ManagerOption tunes NewTenantManager.
type ManagerOption func(*TenantManager)

// WithLeadership replaces the leadership source the tenants consult before
// running leader-only operations. For tests.
func WithLeadership(l Leadership) ManagerOption {
	return func(m *TenantManager) { m.leader = l }
}

// WithoutProcessBootstrap makes the manager skip everything that is global to
// the process and must happen once: metrics exporters, signal handlers, Raft,
// the multi-tenant election check and process.SetDefaultControl. For tests that
// build several managers in one process.
func WithoutProcessBootstrap() ManagerOption {
	return func(m *TenantManager) { m.processBootstrap = false }
}

// TenantManager owns the tenants of the process and the node-level machinery
// they share: the control (node health, election, access tokens), Raft, the
// signal handlers and the node health tick. 1 process -> N Tenants -> 1 Raft.
type TenantManager struct {
	base *config.Configuration

	byID    map[string]*Tenant
	tenants []*Tenant // sorted by ID

	// control is the node control: node_health, the election without Raft, access
	// tokens. Its backend is, in this order: the process default backend in the
	// legacy single-tenant setup; the backend of the tenant "default" if there is
	// one; otherwise a dedicated backend built from the global (node-local)
	// backend settings of the base configuration (BackendDB, SQLite3DataFile,
	// MySQLOrchestrator*), which then MUST describe a usable backend.
	control *process.Control
	// ownedControlBackend is that dedicated backend, closed by Stop.
	ownedControlBackend *db.Backend
	// installedControl is true when this manager made its control the process
	// default (process.SetDefaultControl); Stop then clears it.
	installedControl bool

	leader           Leadership
	processBootstrap bool
	legacy           bool
	// defaultsDisabled is true while this manager holds the legacy defaults disabled.
	defaultsDisabled bool

	// tenantSetState is the memory of the tenant-set comparison (see tenant_set.go);
	// thisNode and now are seams for tests (nil: the real ones).
	tenantSetState tenantSetState
	thisNode       func() string
	now            func() time.Time

	mu      sync.Mutex
	started bool
	stopped bool
	cancel  context.CancelFunc
	// wg tracks the goroutines of the node loop and the tenant runs.
	wg   sync.WaitGroup
	done chan struct{}
}

// NewTenantManager builds the runtime of every tenant configured for this
// process (see config.LoadTenantConfigs): without a tenants file, the single
// legacy tenant "default" driven by base itself; otherwise the tenants of the
// file. For each tenant it creates and deploys its backend, its inst.Store and
// the rest of its runtime. Nothing runs until Start.
//
// It is fail-fast: if any tenant cannot be initialised the error names it,
// everything already built is released and no manager is returned -- the process
// must not start with an inconsistent set of tenants.
func NewTenantManager(base *config.Configuration, opts ...ManagerOption) (*TenantManager, error) {
	tenantConfigs, err := config.LoadTenantConfigs(base)
	if err != nil {
		return nil, err
	}
	return NewTenantManagerFor(base, tenantConfigs, opts...)
}

// NewTenantManagerFor is NewTenantManager over already resolved tenant configurations.
func NewTenantManagerFor(base *config.Configuration, tenantConfigs []*config.TenantConfig, opts ...ManagerOption) (_ *TenantManager, err error) {
	if base == nil {
		return nil, errors.New("tenant manager: nil base configuration")
	}
	if len(tenantConfigs) == 0 {
		return nil, errors.New("tenant manager: no tenants")
	}
	m := &TenantManager{
		base:             base,
		byID:             make(map[string]*Tenant),
		leader:           processLeadership{},
		processBootstrap: true,
		done:             make(chan struct{}),
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.processBootstrap {
		if err := process.ValidateMultiTenantElection(base.RaftEnabled, len(tenantConfigs)); err != nil {
			return nil, err
		}
	}
	// The legacy tenant runs on the process-wide defaults, so that the code that
	// still uses the pre-multi-tenant API sees the very same state.
	m.legacy = len(tenantConfigs) == 1 && base == config.Config && base.TenantsConfigFile == "" &&
		tenantConfigs[0].ID == config.DefaultTenantID && tenantConfigs[0].Cfg == base
	defer func() {
		if err != nil {
			m.release()
		}
	}()
	if !m.legacy {
		disableLegacyDefaults()
		m.defaultsDisabled = true
		if _, hasDefault := tenantIDs(tenantConfigs)[config.DefaultTenantID]; !hasDefault {
			// The legacy metric variables of package metrics are the series of the
			// tenant "default", created when the package loads. Without such a
			// tenant they would be exported (at 0) for nothing.
			ometrics.ForTenant(ometrics.DefaultTenant).Forget()
		}
	}

	for _, tc := range tenantConfigs {
		if _, dup := m.byID[tc.ID]; dup {
			return nil, fmt.Errorf("tenant %q: duplicate tenant", tc.ID)
		}
		t, err := newTenant(tc, tenantOptions{leader: m.leader, shared: m.legacy})
		if err != nil {
			return nil, fmt.Errorf("tenant %q: %w", tc.ID, err)
		}
		m.byID[tc.ID] = t
		m.tenants = append(m.tenants, t)
	}
	sort.Slice(m.tenants, func(i, j int) bool { return m.tenants[i].ID < m.tenants[j].ID })

	if err := m.setupControl(); err != nil {
		return nil, err
	}
	for _, t := range m.tenants {
		if err := m.control.AddMirror(t.ID, t.Backend); err != nil {
			return nil, fmt.Errorf("tenant %q: %w", t.ID, err)
		}
		t.control = m.control
	}
	return m, nil
}

func tenantIDs(tenantConfigs []*config.TenantConfig) map[string]struct{} {
	ids := make(map[string]struct{}, len(tenantConfigs))
	for _, tc := range tenantConfigs {
		ids[tc.ID] = struct{}{}
	}
	return ids
}

// setupControl decides the control backend (see TenantManager.control) and builds the control.
func (m *TenantManager) setupControl() error {
	if m.legacy {
		m.control = process.DefaultControl()
		return nil
	}
	var backend *db.Backend
	if t := m.byID[config.DefaultTenantID]; t != nil {
		backend = t.Backend
	} else {
		b, err := db.NewBackend(controlBackendID, m.base)
		if err != nil {
			return fmt.Errorf("control backend: no tenant %q to host it and the global backend settings are unusable: %w", config.DefaultTenantID, err)
		}
		if err := b.EnsureSchema(); err != nil {
			_ = b.Close()
			return fmt.Errorf("control backend: deploying the schema: %w", err)
		}
		m.ownedControlBackend = b
		backend = b
	}
	m.control = process.NewControl(backend)
	if m.processBootstrap {
		process.SetDefaultControl(m.control)
		m.installedControl = true
	}
	return nil
}

// Get returns the tenant with the given ID.
func (m *TenantManager) Get(id string) (*Tenant, bool) {
	t, ok := m.byID[id]
	return t, ok
}

// All returns the tenants, sorted by ID.
func (m *TenantManager) All() []*Tenant {
	return append([]*Tenant(nil), m.tenants...)
}

// Default returns the tenant that answers to the API calls that name no tenant:
// the tenant "default" if there is one, the only tenant if there is just one,
// nil otherwise (the caller must then be told which tenant is meant).
func (m *TenantManager) Default() *Tenant {
	if t, ok := m.byID[config.DefaultTenantID]; ok {
		return t
	}
	if len(m.tenants) == 1 {
		return m.tenants[0]
	}
	return nil
}

// Control returns the node control (health, election, access tokens).
func (m *TenantManager) Control() *process.Control { return m.control }

// Done is closed when the manager has been stopped.
func (m *TenantManager) Done() <-chan struct{} { return m.done }

// Start starts the node-level machinery and the tenants: it returns once they
// are launched, and they run until ctx is done or Stop is called. Unless the
// manager was built WithoutProcessBootstrap it also performs the process-wide
// bootstrap: metrics exporters, signal handlers and Raft.
func (m *TenantManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return errors.New("tenant manager: already stopped")
	}
	if m.started {
		return errors.New("tenant manager: already started")
	}
	m.started = true
	ctx, m.cancel = context.WithCancel(ctx)
	if m.processBootstrap {
		if err := m.bootstrapProcess(); err != nil {
			m.cancel()
			return err
		}
	}
	// The first heartbeat reaches every tenant backend (synchronously) before any
	// tenant starts its maintenance: what that maintenance expires depends on this
	// node being in the node_health of the backend (see Tenant.nodeHealthMirrored).
	if hb := m.control.Heartbeat(process.ThisNodeHealth); !hb.Control.OK() {
		_ = log.Errorf("first heartbeat: the control backend did not register this node: %v", hb.Control.Err)
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runNode(ctx)
	}()
	for _, t := range m.tenants {
		t := t
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			t.Run(ctx)
		}()
	}
	return nil
}

// Stop stops the node loop and every tenant, waits for them, and releases what
// the manager owns: the Store and Backend of each tenant, the mirrors of the
// control and its dedicated backend. It is idempotent.
func (m *TenantManager) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	cancel := m.cancel
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	m.release()
	close(m.done)
}

// release stops the tenants and frees the manager's resources. It also serves
// the failed construction path.
func (m *TenantManager) release() {
	for _, t := range m.tenants {
		t.Stop()
		if m.control != nil {
			m.control.RemoveMirror(t.ID)
		}
	}
	if m.installedControl {
		if process.DefaultControl() == m.control {
			process.SetDefaultControl(nil)
		}
		m.installedControl = false
	}
	if m.ownedControlBackend != nil {
		if err := m.ownedControlBackend.Close(); err != nil {
			_ = log.Errorf("closing the control backend: %v", err)
		}
		m.ownedControlBackend = nil
	}
	if m.defaultsDisabled {
		enableLegacyDefaults()
		m.defaultsDisabled = false
	}
}
