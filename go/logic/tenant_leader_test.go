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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/proxysql/orchestrator/go/inst"
)

// leaderOnlyOps are the operations the tenant loops launch only on the leader.
var leaderOnlyOps = []string{
	"update-cluster-aliases", "expire-downtime", "inject-seeds",
	"record-instance-coordinates-history", "review-unseen-instances", "inject-unseen-masters",
	"forget-long-unseen-instances", "expire-maintenance", "expire-audit",
	"expire-failure-detection-history", "expire-topology-recovery-history", "submit-masters-to-kv-stores",
	"clear-active-failure-detections", "clear-active-recoveries", "expire-blocked-recoveries",
	"acknowledge-crashed-recoveries", "check-and-recover", "snapshot-topologies",
}

func observeAll(m *TenantManager) *opRecorder {
	rec := newOpRecorder()
	for _, tn := range m.All() {
		tn.opObserver = rec.observe(tn.ID)
	}
	return rec
}

// runOneRoundOfTicks runs, for the tenant, the handler of every tick of its loop once.
func runOneRoundOfTicks(tn *Tenant) {
	ctx := context.Background()
	tn.onHealthTick()
	tn.onInstancePollTick(ctx)
	tn.onAutoPseudoGTIDTick(ctx)
	tn.onCaretakingTick(ctx, true)
	tn.onRaftCaretakingTick(ctx)
	tn.onRecoveryTick(ctx, func() bool { return true })
	tn.onSnapshotTopologiesTick(ctx)
	tn.wg.Wait()
}

func TestLeaderOnlyOperationsRunForEveryTenantWhenLeader(t *testing.T) {
	m, leader := newTestManager(t, []string{"a", "b", "c"}, nil)
	rec := observeAll(m)
	leader.leader.Store(true)
	for _, tn := range m.All() {
		runOneRoundOfTicks(tn)
	}
	for _, tn := range m.All() {
		got := rec.opsOf(tn.ID)
		for _, op := range leaderOnlyOps {
			if got[op] == 0 {
				t.Errorf("tenant %s: leader-only operation %s was not run by the leader", tn.ID, op)
			}
		}
	}
}

func TestNoLeaderOnlyOperationRunsWhenNotLeader(t *testing.T) {
	m, leader := newTestManager(t, []string{"a", "b", "c"}, nil)
	rec := observeAll(m)
	leader.leader.Store(false)
	for _, tn := range m.All() {
		runOneRoundOfTicks(tn)
	}
	for _, tn := range m.All() {
		for op, n := range rec.opsOf(tn.ID) {
			// the one thing a follower does is refreshing its own caches
			if op != "load-hostname-resolve-cache" {
				t.Errorf("tenant %s: leader-only operation %s ran %d times on a node that is not the leader", tn.ID, op, n)
			}
		}
	}
}

func TestTenantRunDrivesTheLoopsOnlyWhileLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the one second ticks")
	}
	m, leader := newTestManager(t, []string{"a", "b"}, nil)
	rec := observeAll(m)
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	time.Sleep(1500 * time.Millisecond)
	for _, id := range []string{"a", "b"} {
		if ops := rec.opsOf(id); ops["clear-active-recoveries"] != 0 || ops["check-and-recover"] != 0 {
			t.Errorf("tenant %s ran recovery operations while the node is not the leader: %v", id, ops)
		}
	}

	leader.leader.Store(true)
	for _, id := range []string{"a", "b"} {
		id := id
		waitFor("the recovery tick of tenant "+id, func() bool { return rec.opsOf(id)["clear-active-recoveries"] > 0 })
	}
	// still a single stop for everything
	m.Stop()
}

func TestDiscoveryWorkersDiscoverFromTheQueueOfTheirTenantOnly(t *testing.T) {
	m, leader := newTestManager(t, []string{"a", "b"}, nil)
	leader.leader.Store(true)
	a, b := mustTenant(t, m, "a"), mustTenant(t, m, "b")
	// the instance is known to a only, and up to date there
	tenantWriteInstance(t, a, "db1.invalid", 3306)
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	key := inst.InstanceKey{Hostname: "db1.invalid", Port: 3306}
	a.discoveryQueue.Push(key)
	b.discoveryQueue.Push(key)
	deadline := time.Now().Add(10 * time.Second)
	for a.discoveryQueue.QueueLen() > 0 || b.discoveryQueue.QueueLen() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("the workers did not consume the queues")
		}
		time.Sleep(20 * time.Millisecond)
	}
	m.Stop()
}

func TestHooksReceiveTheTenantID(t *testing.T) {
	m, _ := newTestManager(t, []string{"tenant-a", "tenant-b"}, nil)
	for _, tn := range m.All() {
		tn.auditTopologyRecoveryFn = func(*TopologyRecovery, string) error { return nil }
		recovery := NewTopologyRecovery(inst.ReplicationAnalysis{AnalyzedInstanceKey: inst.InstanceKey{Hostname: "db1", Port: 3306}})

		found := false
		for _, kv := range tn.applyEnvironmentVariables(recovery) {
			found = found || kv == "ORC_TENANT_ID="+tn.ID
		}
		if !found {
			t.Errorf("tenant %s: ORC_TENANT_ID missing from the hook environment", tn.ID)
		}
		command, _ := tn.prepareCommand("notify {tenantId} {failedHost}", recovery)
		if command != "notify "+tn.ID+" db1" {
			t.Errorf("tenant %s: placeholder not replaced: %q", tn.ID, command)
		}

		// a hook that really runs sees it too
		out := filepath.Join(t.TempDir(), "tenant")
		if err := tn.executeProcesses([]string{"printenv ORC_TENANT_ID > " + out}, "TestHooks", recovery, true); err != nil {
			t.Fatalf("tenant %s: %v", tn.ID, err)
		}
		got, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(got)) != tn.ID {
			t.Errorf("tenant %s: the hook saw ORC_TENANT_ID=%q", tn.ID, got)
		}
	}
}
