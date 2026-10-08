package orcraft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

type appliedCmd struct {
	Tenant string
	Op     string
	Value  string
}

// legacyOnlyApplier implements just CommandApplier (what go/logic is today).
type legacyOnlyApplier struct {
	mu      sync.Mutex
	applied []appliedCmd
}

func (a *legacyOnlyApplier) ApplyCommand(op string, value []byte) interface{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.applied = append(a.applied, appliedCmd{Op: op, Value: string(value)})
	return nil
}

func (a *legacyOnlyApplier) get() []appliedCmd {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]appliedCmd(nil), a.applied...)
}

// tenantApplier is tenant-aware; knownTenants limits the tenants it accepts.
type tenantApplier struct {
	legacyOnlyApplier
	known map[string]bool
}

func (a *tenantApplier) ApplyTenantCommand(tenantID, op string, value []byte) interface{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.known[tenantID] {
		return fmt.Errorf("%w: %s", ErrUnknownTenant, tenantID)
	}
	a.applied = append(a.applied, appliedCmd{Tenant: tenantID, Op: op, Value: string(value)})
	return "ok:" + tenantID
}

func encode(t *testing.T, c storeCommand) []byte {
	t.Helper()
	b, err := json.Marshal(&c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func applyLog(f *fsm, c storeCommand, t *testing.T) interface{} {
	return f.Apply(&raft.Log{Index: 1, Data: encode(t, c)})
}

func TestLegacyEntryDecodesToLegacyTenant(t *testing.T) {
	// JSON written by a pre-multi-tenant node: no "tenant" field.
	old := []byte(`{"op":"discover","value":"IjEwLjAuMC4xIg=="}`)
	var c storeCommand
	if err := json.Unmarshal(old, &c); err != nil {
		t.Fatal(err)
	}
	if c.Tenant != "" || ResolveTenant(c.Tenant) != LegacyTenantID {
		t.Fatalf("legacy entry resolved to %q", ResolveTenant(c.Tenant))
	}
	app := &tenantApplier{known: map[string]bool{LegacyTenantID: true}}
	f := &fsm{applier: app}
	if r := f.Apply(&raft.Log{Index: 1, Data: old}); r != "ok:"+LegacyTenantID {
		t.Fatalf("unexpected response %v", r)
	}
	got := app.get()
	if len(got) != 1 || got[0].Tenant != LegacyTenantID || got[0].Op != "discover" || got[0].Value != `"10.0.0.1"` {
		t.Fatalf("unexpected applied: %+v", got)
	}
}

func TestTenantRoundTrip(t *testing.T) {
	b := encode(t, storeCommand{Op: "forget", Value: []byte(`"x"`), Tenant: "mariadb-a"})
	var c storeCommand
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if c.Tenant != "mariadb-a" || c.Op != "forget" || string(c.Value) != `"x"` {
		t.Fatalf("round trip lost data: %+v", c)
	}
}

func TestLegacySerializationByteForByte(t *testing.T) {
	// This is what the pre-multi-tenant storeCommand produced.
	type oldStoreCommand struct {
		Op    string `json:"op,omitempty"`
		Value []byte `json:"value,omitempty"`
	}
	for _, tenant := range []string{"", LegacyTenantID} {
		for _, op := range []string{"discover", HeartbeatCommand} {
			value := []byte(`"10.0.0.1"`)
			want, _ := json.Marshal(&oldStoreCommand{Op: op, Value: value})
			got, err := marshalForTenant(tenant, op, value)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("tenant=%q op=%s: got %s want %s", tenant, op, got, want)
			}
		}
	}
	got, _ := marshalForTenant("t1", "discover", []byte(`1`))
	if string(got) != `{"op":"discover","value":"MQ==","tenant":"t1"}` {
		t.Fatalf("unexpected tenant encoding %s", got)
	}
}

func TestGlobalOpsTable(t *testing.T) {
	for _, op := range []string{"heartbeat", "async-snapshot", "leader-uri", "request-health-report", "yield", "yield-hint", "register-node"} {
		if !IsGlobalOp(op) {
			t.Errorf("%s should be global", op)
		}
	}
	for _, op := range []string{"discover", "forget", "ack-recovery", "disable-global-recoveries", "put-key-value"} {
		if IsGlobalOp(op) {
			t.Errorf("%s should be tenant-scoped", op)
		}
	}
}

func TestFSMRoutesToTenantApplier(t *testing.T) {
	app := &tenantApplier{known: map[string]bool{LegacyTenantID: true, "a": true, "b": true}}
	f := &fsm{applier: app}
	applyLog(f, storeCommand{Op: "discover", Value: []byte("1"), Tenant: "a"}, t)
	applyLog(f, storeCommand{Op: "discover", Value: []byte("2"), Tenant: "b"}, t)
	applyLog(f, storeCommand{Op: "discover", Value: []byte("3")}, t)
	applyLog(f, storeCommand{Op: "forget", Value: []byte("4"), Tenant: LegacyTenantID}, t)
	want := []appliedCmd{
		{"a", "discover", "1"}, {"b", "discover", "2"},
		{LegacyTenantID, "discover", "3"}, {LegacyTenantID, "forget", "4"},
	}
	got := app.get()
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestFSMLegacyOnlyApplier(t *testing.T) {
	app := &legacyOnlyApplier{}
	f := &fsm{applier: app}
	if r := applyLog(f, storeCommand{Op: "discover", Value: []byte("1")}, t); r != nil {
		t.Fatalf("legacy command failed: %v", r)
	}
	if r := applyLog(f, storeCommand{Op: "discover", Value: []byte("2"), Tenant: LegacyTenantID}, t); r != nil {
		t.Fatalf("legacy command failed: %v", r)
	}
	before := UnknownTenantCommands()
	r := applyLog(f, storeCommand{Op: "discover", Value: []byte("3"), Tenant: "other"}, t)
	if err, ok := r.(error); !ok || !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("non-legacy tenant on tenant-unaware applier must be rejected, got %v", r)
	}
	if UnknownTenantCommands() != before+1 {
		t.Fatalf("unknown tenant counter not incremented")
	}
	if got := app.get(); len(got) != 2 {
		t.Fatalf("only the 2 legacy commands must be applied, got %+v", got)
	}
}

func TestFSMUnknownTenantNoPanicNoApply(t *testing.T) {
	app := &tenantApplier{known: map[string]bool{LegacyTenantID: true}}
	f := &fsm{applier: app}
	before := UnknownTenantCommands()
	r := applyLog(f, storeCommand{Op: "forget", Value: []byte("1"), Tenant: "ghost"}, t)
	if err, ok := r.(error); !ok || !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("expected ErrUnknownTenant, got %v", r)
	}
	if UnknownTenantCommands() != before+1 || len(app.get()) != 0 {
		t.Fatalf("command must be counted and not applied")
	}
	// the FSM keeps working afterwards
	if r := applyLog(f, storeCommand{Op: "forget", Value: []byte("2")}, t); r != "ok:"+LegacyTenantID {
		t.Fatalf("FSM stuck after unknown tenant: %v", r)
	}
}

func TestFSMGlobalCommands(t *testing.T) {
	app := &tenantApplier{known: map[string]bool{LegacyTenantID: true, "a": true}}
	f := &fsm{applier: app}
	// global op goes to ApplyCommand (not ApplyTenantCommand)
	applyLog(f, storeCommand{Op: HeartbeatCommand}, t)
	applyLog(f, storeCommand{Op: LeaderURICommand, Value: []byte(`"http://x"`)}, t)
	got := app.get()
	if len(got) != 2 || got[0].Tenant != "" || got[0].Op != HeartbeatCommand || got[1].Op != LeaderURICommand {
		t.Fatalf("global ops must reach ApplyCommand without tenant, got %+v", got)
	}
	// global op carrying a tenant is rejected and not applied
	r := applyLog(f, storeCommand{Op: HeartbeatCommand, Tenant: "a"}, t)
	if err, ok := r.(error); !ok || !errors.Is(err, ErrTenantOnGlobalCommand) {
		t.Fatalf("expected ErrTenantOnGlobalCommand, got %v", r)
	}
	if len(app.get()) != 2 {
		t.Fatalf("rejected global command was applied")
	}
	// ...while an explicit legacy tenant is tolerated
	if r := applyLog(f, storeCommand{Op: HeartbeatCommand, Tenant: LegacyTenantID}, t); r != nil {
		t.Fatalf("legacy tenant on global op must be tolerated: %v", r)
	}
}

func TestPublishRejectsTenantOnGlobalOp(t *testing.T) {
	s := &Store{} // raft nil: the check must happen before touching it
	if _, err := s.tenantCommand("a", HeartbeatCommand, nil); !errors.Is(err, ErrTenantOnGlobalCommand) {
		t.Fatalf("expected ErrTenantOnGlobalCommand, got %v", err)
	}
}

type nopSnapshotter struct{}

func (nopSnapshotter) GetData() ([]byte, error) { return []byte("{}"), nil }
func (nopSnapshotter) Restore(rc io.ReadCloser) error {
	return rc.Close()
}

func newInmemCluster(t *testing.T, appliers []CommandApplier) []*Store {
	t.Helper()
	n := len(appliers)
	addrs := make([]raft.ServerAddress, n)
	transports := make([]*raft.InmemTransport, n)
	var servers []raft.Server
	for i := range transports {
		addrs[i], transports[i] = raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("node%d", i)))
		servers = append(servers, raft.Server{ID: raft.ServerID(addrs[i]), Address: addrs[i]})
	}
	for i := range transports {
		for j := range transports {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}
	stores := make([]*Store, n)
	for i := range stores {
		cfg := raft.DefaultConfig()
		cfg.LocalID = raft.ServerID(addrs[i])
		cfg.HeartbeatTimeout = 50 * time.Millisecond
		cfg.ElectionTimeout = 50 * time.Millisecond
		cfg.LeaderLeaseTimeout = 50 * time.Millisecond
		cfg.CommitTimeout = 5 * time.Millisecond
		logs, snaps := raft.NewInmemStore(), raft.NewInmemSnapshotStore()
		if err := raft.BootstrapCluster(cfg, logs, logs, snaps, transports[i], raft.Configuration{Servers: servers}); err != nil {
			t.Fatal(err)
		}
		s := NewStore("", string(addrs[i]), string(addrs[i]), appliers[i], nopSnapshotter{})
		r, err := raft.NewRaft(cfg, (*fsm)(s), logs, logs, snaps, transports[i])
		if err != nil {
			t.Fatal(err)
		}
		s.raft = r
		stores[i] = s
		t.Cleanup(func() { r.Shutdown() })
	}
	return stores
}

func waitLeader(t *testing.T, stores []*Store) *Store {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range stores {
			if s.raft.State() == raft.Leader {
				return s
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func TestInmemClusterTenantCommandReachesAllNodes(t *testing.T) {
	apps := []*tenantApplier{}
	cmdAppliers := []CommandApplier{}
	for i := 0; i < 3; i++ {
		a := &tenantApplier{known: map[string]bool{LegacyTenantID: true, "mariadb-a": true, "pg-b": true}}
		apps = append(apps, a)
		cmdAppliers = append(cmdAppliers, a)
	}
	stores := newInmemCluster(t, cmdAppliers)
	leader := waitLeader(t, stores)
	// one follower does not know tenant "pg-b" (divergent config)
	divergent := -1
	for i, s := range stores {
		if s != leader {
			divergent = i
			break
		}
	}
	apps[divergent].mu.Lock()
	apps[divergent].known = map[string]bool{LegacyTenantID: true, "mariadb-a": true}
	apps[divergent].mu.Unlock()

	if _, err := leader.tenantCommand("mariadb-a", "discover", []byte(`"h:3306"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.tenantCommand("", "forget", []byte(`"legacy"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.tenantCommand("pg-b", "discover", []byte(`"p:5432"`)); err != nil {
		t.Fatalf("leader knows pg-b, got %v", err)
	}
	if _, err := leader.genericCommand(HeartbeatCommand, nil); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for i, a := range apps {
			want := 4 // a discover, legacy forget, pg-b discover, heartbeat
			if i == divergent {
				want = 3
			}
			if len(a.get()) != want {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("commands not applied everywhere: %+v %+v %+v", apps[0].get(), apps[1].get(), apps[2].get())
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i, a := range apps {
		got := a.get()
		if got[0] != (appliedCmd{"mariadb-a", "discover", `"h:3306"`}) {
			t.Errorf("node %d: tenant command not applied with its tenant: %+v", i, got[0])
		}
		if got[1] != (appliedCmd{LegacyTenantID, "forget", `"legacy"`}) {
			t.Errorf("node %d: legacy command: %+v", i, got[1])
		}
	}
}
