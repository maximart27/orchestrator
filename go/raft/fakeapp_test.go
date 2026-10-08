package orcraft_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// fakeApp is a deterministic, order-sensitive state machine with the same
// contract as logic.CommandApplier: tenant-scoped ops go to a per-tenant state, a
// tenant it does not know is ErrUnknownTenant, global ops touch no tenant.
//
// Ops (value is JSON {"K":..,"V":..}):
//
//	put K V   state[K] = V
//	del K     delete state[K]
//	cat K V   state[K] += V + ","    (order- and duplicate-sensitive)
type fakeApp struct {
	mu      sync.Mutex
	known   map[string]bool
	state   map[string]map[string]string
	seq     map[string][]string // per tenant, value of every applied command (the apply order)
	applied int
	globals []string
	// persistent: keep the state across Restarted (a SQLite backend); otherwise the
	// state is rebuilt by Raft (snapshot + log replay).
	persistent bool
}

type fakeCmd struct{ K, V string }

func newFakeApp(persistent bool, tenants ...string) *fakeApp {
	a := &fakeApp{persistent: persistent}
	a.reset(tenants...)
	return a
}

func (a *fakeApp) reset(tenants ...string) {
	a.known = map[string]bool{}
	a.state = map[string]map[string]string{}
	a.seq = map[string][]string{}
	for _, t := range tenants {
		a.known[t] = true
		a.state[t] = map[string]string{}
	}
}

func (a *fakeApp) knownTenants() []string {
	var ts []string
	for t := range a.known {
		ts = append(ts, t)
	}
	sort.Strings(ts)
	return ts
}

func (a *fakeApp) Restarted() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.persistent {
		a.reset(a.knownTenants()...)
	}
}

func (a *fakeApp) ApplyCommand(op string, value []byte) interface{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.globals = append(a.globals, op)
	return nil
}

func (a *fakeApp) ApplyTenantCommand(tenantID, op string, value []byte) interface{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.known[tenantID] {
		return fmt.Errorf("%w: %s", orcraft.ErrUnknownTenant, tenantID)
	}
	var c fakeCmd
	if err := json.Unmarshal(value, &c); err != nil {
		return err
	}
	st := a.state[tenantID]
	switch op {
	case "put":
		st[c.K] = c.V
	case "del":
		delete(st, c.K)
	case "cat":
		st[c.K] += c.V + ","
	default:
		return fmt.Errorf("unknown op %s", op)
	}
	a.applied++
	a.seq[tenantID] = append(a.seq[tenantID], op+":"+c.K+"="+c.V)
	return "ok"
}

type fakeSnapshot struct {
	State map[string]map[string]string
	Seq   map[string][]string
}

func (a *fakeApp) GetData() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return json.Marshal(fakeSnapshot{State: a.state, Seq: a.seq})
}

func (a *fakeApp) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	var s fakeSnapshot
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&s); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	// like a SQL restore of a tenant of the snapshot: replace what that tenant had
	for t := range s.State {
		if a.known[t] {
			a.state[t] = s.State[t]
			a.seq[t] = s.Seq[t]
			if a.state[t] == nil {
				a.state[t] = map[string]string{}
			}
		}
	}
	return nil
}

func (a *fakeApp) Digest(tenantID string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var b strings.Builder
	st := a.state[tenantID]
	var ks []string
	for k := range st {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		fmt.Fprintf(&b, "%s=%s\n", k, st[k])
	}
	return b.String()
}

// seqOf returns the apply order of a tenant on this node.
func (a *fakeApp) seqOf(tenantID string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.seq[tenantID]...)
}

func (a *fakeApp) get(tenantID, k string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state[tenantID][k]
}

func (a *fakeApp) appliedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.applied
}

func (a *fakeApp) forget(tenant string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.known, tenant)
	delete(a.state, tenant)
}

func (a *fakeApp) learn(tenant string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.known[tenant] = true
	if a.state[tenant] == nil {
		a.state[tenant] = map[string]string{}
	}
}

func fakeVal(k, v string) []byte {
	b, _ := json.Marshal(fakeCmd{K: k, V: v})
	return b
}

func newFakeApps(n int, persistent bool, tenants ...string) ([]nodeApp, []*fakeApp) {
	var apps []nodeApp
	var fakes []*fakeApp
	for i := 0; i < n; i++ {
		f := newFakeApp(persistent, tenants...)
		apps = append(apps, f)
		fakes = append(fakes, f)
	}
	return apps, fakes
}
