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
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
)

// testLeadership is a Leadership the tests switch at will.
type testLeadership struct{ leader atomic.Bool }

func (l *testLeadership) IsLeader() bool         { return l.leader.Load() }
func (l *testLeadership) IsLeaderOrActive() bool { return l.leader.Load() }

// testBase is the global configuration of a test manager: it describes the
// control backend (a SQLite file of its own) and is what tenants inherit.
func testBase(t *testing.T) *config.Configuration {
	t.Helper()
	base := config.Config.Clone()
	base.BackendDB = "sqlite3"
	base.SQLite3DataFile = filepath.Join(t.TempDir(), "control.sqlite3")
	base.HostnameResolveMethod = "none"
	base.InstanceFlushIntervalMilliseconds = 20
	base.InstanceWriteBufferSize = 10
	base.DiscoveryMaxConcurrency = 3
	base.DeadInstanceDiscoveryMaxConcurrency = 2
	return base
}

// testTenantConfigs resolves one tenant per id, each over a SQLite file of its
// own. extra[id] is JSON inserted in the "Config" section of that tenant.
func testTenantConfigs(t *testing.T, base *config.Configuration, ids []string, extra map[string]string) []*config.TenantConfig {
	t.Helper()
	dir := t.TempDir()
	var entries []string
	for _, id := range ids {
		cfg := fmt.Sprintf(`"KVClusterMasterPrefix": "%s/master/"`, id)
		if e := extra[id]; e != "" {
			cfg += ", " + e
		}
		entries = append(entries, fmt.Sprintf(`{"ID": %q, "Provider": "mysql",
			"Backend": {"BackendDB": "sqlite3", "SQLite3DataFile": %q},
			"Config": {%s}}`, id, filepath.Join(dir, id+".sqlite3"), cfg))
	}
	tenants, err := config.ParseTenantConfigs(base, []byte(`{"Tenants": [`+strings.Join(entries, ",")+`]}`))
	if err != nil {
		t.Fatalf("tenant configuration: %v", err)
	}
	return tenants
}

// newTestManager builds (not starts) a manager over SQLite tenants, with an
// injectable leadership and without the process-wide bootstrap. It is stopped at cleanup.
func newTestManager(t *testing.T, ids []string, extra map[string]string, opts ...ManagerOption) (*TenantManager, *testLeadership) {
	t.Helper()
	base := testBase(t)
	leader := &testLeadership{}
	m, err := NewTenantManagerFor(base, testTenantConfigs(t, base, ids, extra),
		append([]ManagerOption{WithoutProcessBootstrap(), WithLeadership(leader)}, opts...)...)
	if err != nil {
		t.Fatalf("NewTenantManagerFor(%v): %v", ids, err)
	}
	t.Cleanup(m.Stop)
	return m, leader
}

func mustTenant(t *testing.T, m *TenantManager, id string) *Tenant {
	t.Helper()
	tn, ok := m.Get(id)
	if !ok {
		t.Fatalf("tenant %s not found", id)
	}
	return tn
}

func tenantWriteInstance(t *testing.T, tn *Tenant, hostname string, port int) {
	t.Helper()
	instance := &inst.Instance{Key: inst.InstanceKey{Hostname: hostname, Port: port}}
	if err := tn.Store.WriteInstance(instance, false, nil); err != nil {
		t.Fatalf("tenant %s: WriteInstance(%s:%d): %v", tn.ID, hostname, port, err)
	}
}

func tenantSetLastSeen(t *testing.T, tn *Tenant, hostname string, port int, hoursAgo int) {
	t.Helper()
	if _, err := tn.Backend.Exec(`
		update database_instance
		set last_seen = NOW() - interval ? hour
		where hostname = ? and port = ?`, hoursAgo, hostname, port); err != nil {
		t.Fatalf("tenant %s: setting last_seen: %v", tn.ID, err)
	}
}

func tenantHasInstance(t *testing.T, tn *Tenant, hostname string, port int) bool {
	t.Helper()
	keys, err := tn.Store.ReadAllInstanceKeys()
	if err != nil {
		t.Fatalf("tenant %s: ReadAllInstanceKeys: %v", tn.ID, err)
	}
	keyMap := inst.NewInstanceKeyMap()
	keyMap.AddKeys(keys)
	return keyMap.HasKey(inst.InstanceKey{Hostname: hostname, Port: port})
}

// waitGoroutines waits for the number of goroutines to drop to at most want,
// collecting garbage meanwhile (the cache janitors stop when their cache is collected).
func waitGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		runtime.GC()
		if n := runtime.NumGoroutine(); n <= want {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			buf = buf[:runtime.Stack(buf, true)]
			t.Fatalf("goroutines did not return to %d (now %d):\n%s", want, runtime.NumGoroutine(), buf)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// opRecorder collects the leader-only operations the tick loops launch, by tenant.
type opRecorder struct {
	mu  sync.Mutex
	ops map[string]map[string]int
}

func newOpRecorder() *opRecorder { return &opRecorder{ops: map[string]map[string]int{}} }

func (r *opRecorder) observe(tenantID string) func(string) {
	return func(op string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.ops[tenantID] == nil {
			r.ops[tenantID] = map[string]int{}
		}
		r.ops[tenantID][op]++
	}
}

func (r *opRecorder) opsOf(tenantID string) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for op, n := range r.ops[tenantID] {
		out[op] = n
	}
	return out
}
