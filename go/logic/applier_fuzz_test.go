package logic

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/proxysql/orchestrator/go/config"
)

var (
	fuzzManagerOnce sync.Once
	fuzzManager     *TenantManager
)

// sharedFuzzManager builds, once for the process, a manager with tenants a and b. It lives until
// the test binary exits (a fuzz target cannot hand its *testing.T to a manager of its own).
func sharedFuzzManager(t *testing.T) *TenantManager {
	fuzzManagerOnce.Do(func() {
		dir, err := os.MkdirTemp("", "applier-fuzz")
		if err != nil {
			t.Fatal(err)
		}
		base := config.Config.Clone()
		base.BackendDB = "sqlite3"
		base.SQLite3DataFile = filepath.Join(dir, "control.sqlite3")
		base.HostnameResolveMethod = "none"
		var entries []string
		for _, id := range []string{"a", "b"} {
			entries = append(entries, fmt.Sprintf(`{"ID": %q, "Provider": "mysql", "Backend": {"BackendDB": "sqlite3", "SQLite3DataFile": %q}, "Config": {}}`,
				id, filepath.Join(dir, id+".sqlite3")))
		}
		tcs, err := config.ParseTenantConfigs(base, []byte(`{"Tenants": [`+strings.Join(entries, ",")+`]}`))
		if err != nil {
			t.Fatal(err)
		}
		fuzzManager, err = NewTenantManagerFor(base, tcs, WithoutProcessBootstrap(), WithLeadership(&testLeadership{}))
		if err != nil {
			t.Fatal(err)
		}
	})
	if fuzzManager == nil {
		t.Fatal("the shared manager could not be built")
	}
	return fuzzManager
}

// Scenario 8: whatever op, tenant and value a log entry carries, the real applier answers (nil or an
// error) and never panics. discover is left out: it connects to the instance named by the value.
func FuzzApplyTenantCommand(f *testing.F) {
	ops := []string{"injected-pseudo-gtid", "forget", "forget-cluster", "begin-downtime", "end-downtime", "register-candidate",
		"ack-recovery", "register-hostname-unresolve", "submit-pool-instances", "register-failure-detection", "write-recovery",
		"write-recovery-step", "resolve-recovery", "disable-global-recoveries", "enable-global-recoveries", "put-key-value",
		"put-instance-tag", "delete-instance-tag", "set-cluster-alias-manual-override", "no-such-op", ""}
	for i := range ops {
		f.Add(uint8(i), "a", []byte(`{}`))
		f.Add(uint8(i), "a", []byte(`null`))
		f.Add(uint8(i), "ghost", []byte(`"x"`))
		f.Add(uint8(i), "b", []byte(`{"Key":{"Hostname":"db1","Port":3306},"Owner":"o","AllRecoveries":true,"UID":"u","ClusterName":"c","T":{"TagName":"t"}}`))
		f.Add(uint8(i), "a", []byte(`["a","b"]`))
		f.Add(uint8(i), "a", []byte(`{"Hostname":"\u0000","Port":-1,"Key":{"Hostname":"","Port":0}}`))
	}
	f.Add(uint8(0), "A", []byte("\xff\xfe"))
	f.Add(uint8(3), "a\x00b", []byte(`{"Key":null}`))
	f.Add(uint8(16), "a", []byte(`{"Key":"k","Value":"`+strings.Repeat("v", 1<<20)+`"}`))

	f.Fuzz(func(t *testing.T, opIdx uint8, tenant string, value []byte) {
		m := sharedFuzzManager(t)
		op := ops[int(opIdx)%len(ops)]
		res := NewCommandApplier(m).ApplyTenantCommand(tenant, op, value)
		_ = res // nil or an error: both fine; a panic fails the target
	})
}
