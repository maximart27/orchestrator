package logic

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/inst"
)

// Scenario 7: the manager is stopped (SIGTERM path of acceptSignals: Stop, then exit) while
// Raft is still applying commands. Raft's FSM does not know the manager is going away: its
// Apply keeps calling the applier. It must neither panic nor deadlock, nor reach a closed
// Store without an error; and no goroutine must be left behind.
func TestStopWhileRaftIsApplyingCommands(t *testing.T) {
	ids := []string{"a", "b"}
	round := func(t *testing.T) {
		base := testBase(t)
		m, err := NewTenantManagerFor(base, testTenantConfigs(t, base, ids, nil), WithoutProcessBootstrap(), WithLeadership(&testLeadership{}))
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		applier := NewCommandApplier(m)
		for _, id := range ids {
			tenantWriteInstance(t, mustTenant(t, m, id), "db1", 3306)
		}
		var stop atomic.Bool
		var applied, failed, panics int64
		var wg sync.WaitGroup
		ops := []string{"put-instance-tag", "begin-downtime", "forget", "put-key-value", "set-cluster-alias-manual-override",
			"register-candidate", "disable-global-recoveries", "enable-global-recoveries", "injected-pseudo-gtid", "write-recovery", "ack-recovery"}
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				key := inst.InstanceKey{Hostname: "db1", Port: 3306}
				for n := 0; !stop.Load(); n++ {
					op := ops[(w+n)%len(ops)]
					var value []byte
					switch op {
					case "put-instance-tag":
						value = mustJSON(t, inst.InstanceTag{Key: key, T: inst.Tag{TagName: "t", TagValue: "v"}})
					case "begin-downtime":
						value = mustJSON(t, inst.NewDowntime(&key, "o", "r", time.Hour))
					case "forget":
						value = mustJSON(t, key)
					case "put-key-value":
						value = mustJSON(t, map[string]string{"Key": "k", "Value": "v"})
					case "set-cluster-alias-manual-override":
						value = mustJSON(t, [2]string{"c", "a"})
					case "register-candidate":
						value = mustJSON(t, inst.CandidateDatabaseInstance{Hostname: "db1", Port: 3306})
					case "injected-pseudo-gtid":
						value = mustJSON(t, "c")
					case "write-recovery":
						value = mustJSON(t, &TopologyRecovery{UID: fmt.Sprintf("u%d-%d", w, n), AnalysisEntry: inst.ReplicationAnalysis{AnalyzedInstanceKey: key}})
					case "ack-recovery":
						value = mustJSON(t, RecoveryAcknowledgement{Owner: "o", AllRecoveries: true})
					}
					func() {
						defer func() {
							if r := recover(); r != nil {
								atomic.AddInt64(&panics, 1)
								t.Errorf("PANIC applying %s: %v", op, r)
							}
						}()
						res := applier.ApplyTenantCommand(ids[(w+n)%2], op, value)
						if err, isErr := res.(error); isErr && err != nil {
							atomic.AddInt64(&failed, 1)
						} else {
							atomic.AddInt64(&applied, 1)
						}
					}()
				}
			}(w)
		}
		time.Sleep(150 * time.Millisecond)
		stopped := make(chan struct{})
		go func() { m.Stop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(30 * time.Second):
			t.Fatal("Stop() did not return while commands were being applied")
		}
		time.Sleep(100 * time.Millisecond) // keep applying into the stopped manager
		stop.Store(true)
		wg.Wait()
		t.Logf("applied before/while stopping=%d; answered an error after=%d; panics=%d", applied, failed, panics)
		if atomic.LoadInt64(&failed) == 0 {
			t.Errorf("commands applied after Stop must answer an error (closed backend), none did")
		}
		// still a clean answer (error, no panic) for every op after the stop, discover included
		// (it polls the instance -- refused at once here -- and writes through the Store's buffer)
		for _, op := range append(ops, "discover") {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("PANIC applying %s after Stop: %v", op, r)
					}
				}()
				_ = applier.ApplyTenantCommand("a", op, mustJSON(t, inst.InstanceKey{Hostname: "127.0.0.1", Port: 1}))
			}()
		}
	}
	// warm up registries, then check that nothing leaks
	round(t)
	waitGoroutines(t, runtime.NumGoroutine())
	baseline := runtime.NumGoroutine()
	round(t)
	waitGoroutines(t, baseline)
}
