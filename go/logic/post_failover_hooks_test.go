/*
   Copyright 2026 ProxySQL Authors

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
	"errors"
	"os/exec"
	"strings"
	"testing"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
)

// stubAuditNoBackend replaces the recovery audit of the legacy default tenant,
// whose configuration is the global config.Config these tests change, and returns it.
func stubAuditNoBackend(t *testing.T) *Tenant {
	t.Helper()
	dt := DefaultTenant()
	orig := dt.auditTopologyRecoveryFn
	dt.auditTopologyRecoveryFn = func(tr *TopologyRecovery, message string) error {
		return nil
	}
	t.Cleanup(func() { dt.auditTopologyRecoveryFn = orig })
	return dt
}

func TestFailOnPostFailoverHookErrorFollowsConfig(t *testing.T) {
	orig := config.Config.FailRecoveryOnFailedPostFailoverProcesses
	defer func() { config.Config.FailRecoveryOnFailedPostFailoverProcesses = orig }()

	dt := DefaultTenant()
	config.Config.FailRecoveryOnFailedPostFailoverProcesses = false
	if dt.failOnPostFailoverHookError() {
		t.Fatal("expected false when config disabled")
	}
	config.Config.FailRecoveryOnFailedPostFailoverProcesses = true
	if !dt.failOnPostFailoverHookError() {
		t.Fatal("expected true when config enabled")
	}
}

func TestMarkRecoveryUnsuccessfulDueToHooks(t *testing.T) {
	dt := stubAuditNoBackend(t)
	tr := NewTopologyRecovery(inst.ReplicationAnalysis{})
	tr.IsSuccessful = true

	dt.markRecoveryUnsuccessfulDueToHooks(tr, errors.New("hook boom"))
	if tr.IsSuccessful {
		t.Fatal("expected IsSuccessful=false")
	}
	if len(tr.AllErrors) == 0 {
		t.Fatal("expected error recorded on topology recovery")
	}
	if !strings.Contains(tr.AllErrors[0], "post-failover hooks failed") {
		t.Fatalf("unexpected error text: %q", tr.AllErrors[0])
	}

	// nil-safe
	dt.markRecoveryUnsuccessfulDueToHooks(nil, errors.New("x"))
	dt.markRecoveryUnsuccessfulDueToHooks(tr, nil)
}

func TestExecuteProcessesFailOnError(t *testing.T) {
	dt := stubAuditNoBackend(t)
	if _, err := exec.LookPath("false"); err != nil {
		t.Skip("false binary not available")
	}
	tr := NewTopologyRecovery(inst.ReplicationAnalysis{})

	err := dt.executeProcesses([]string{"false"}, "TestHooks", tr, true)
	if err == nil {
		t.Fatal("expected error when failOnError=true and command fails")
	}

	err = dt.executeProcesses([]string{"false", "true"}, "TestHooks", tr, false)
	// failOnError=false: returns first error but continues (true still runs)
	if err == nil {
		t.Fatal("expected first command error to be returned even when failOnError=false")
	}

	err = dt.executeProcesses([]string{"true"}, "TestHooks", tr, true)
	if err != nil {
		t.Fatalf("true should succeed: %v", err)
	}

	err = dt.executeProcesses(nil, "TestHooks", tr, true)
	if err != nil {
		t.Fatalf("empty hooks should succeed: %v", err)
	}
}

func TestRunPostSuccessFailoverProcessesPolicyOff(t *testing.T) {
	dt := stubAuditNoBackend(t)
	if _, err := exec.LookPath("false"); err != nil {
		t.Skip("false binary not available")
	}
	orig := config.Config.FailRecoveryOnFailedPostFailoverProcesses
	defer func() { config.Config.FailRecoveryOnFailedPostFailoverProcesses = orig }()
	config.Config.FailRecoveryOnFailedPostFailoverProcesses = false

	tr := NewTopologyRecovery(inst.ReplicationAnalysis{})
	tr.IsSuccessful = true
	key := inst.InstanceKey{Hostname: "successor.example", Port: 3306}
	tr.SuccessorKey = &key

	err := dt.runPostSuccessFailoverProcesses([]string{"false"}, "PostFailoverProcesses", tr)
	if err != nil {
		t.Fatalf("policy off should not return error: %v", err)
	}
	if !tr.IsSuccessful {
		t.Fatal("policy off must not flip IsSuccessful")
	}
}

func TestRunPostSuccessFailoverProcessesPolicyOn(t *testing.T) {
	dt := stubAuditNoBackend(t)
	if _, err := exec.LookPath("false"); err != nil {
		t.Skip("false binary not available")
	}
	orig := config.Config.FailRecoveryOnFailedPostFailoverProcesses
	defer func() { config.Config.FailRecoveryOnFailedPostFailoverProcesses = orig }()
	config.Config.FailRecoveryOnFailedPostFailoverProcesses = true

	origPersist := dt.persistResolvedRecoveryFn
	persistCalls := 0
	dt.persistResolvedRecoveryFn = func(tr *TopologyRecovery) error {
		persistCalls++
		if tr.IsSuccessful {
			t.Error("persist should see IsSuccessful=false")
		}
		return nil
	}
	defer func() { dt.persistResolvedRecoveryFn = origPersist }()

	tr := NewTopologyRecovery(inst.ReplicationAnalysis{})
	tr.IsSuccessful = true
	key := inst.InstanceKey{Hostname: "successor.example", Port: 3306}
	tr.SuccessorKey = &key

	err := dt.runPostSuccessFailoverProcesses([]string{"false"}, "PostFailoverProcesses", tr)
	if err == nil {
		t.Fatal("policy on should return hook error")
	}
	if tr.IsSuccessful {
		t.Fatal("policy on must set IsSuccessful=false")
	}
	if persistCalls != 1 {
		t.Fatalf("expected 1 persist call, got %d", persistCalls)
	}
}

func TestRunPostSuccessFailoverProcessesPolicyOnPersistError(t *testing.T) {
	dt := stubAuditNoBackend(t)
	if _, err := exec.LookPath("false"); err != nil {
		t.Skip("false binary not available")
	}
	orig := config.Config.FailRecoveryOnFailedPostFailoverProcesses
	defer func() { config.Config.FailRecoveryOnFailedPostFailoverProcesses = orig }()
	config.Config.FailRecoveryOnFailedPostFailoverProcesses = true

	origPersist := dt.persistResolvedRecoveryFn
	dt.persistResolvedRecoveryFn = func(tr *TopologyRecovery) error {
		return errors.New("db down")
	}
	defer func() { dt.persistResolvedRecoveryFn = origPersist }()

	tr := NewTopologyRecovery(inst.ReplicationAnalysis{})
	tr.IsSuccessful = true

	err := dt.runPostSuccessFailoverProcesses([]string{"false"}, "PostFailoverProcesses", tr)
	if err == nil {
		t.Fatal("expected combined error")
	}
	if !strings.Contains(err.Error(), "db down") {
		t.Fatalf("expected persist error in message: %v", err)
	}
	if !strings.Contains(err.Error(), "post-failover hooks failed") {
		t.Fatalf("expected hook failure in message: %v", err)
	}
}

func TestRunPostSuccessFailoverProcessesSuccess(t *testing.T) {
	dt := stubAuditNoBackend(t)
	if _, err := exec.LookPath("true"); err != nil {
		t.Skip("true binary not available")
	}
	orig := config.Config.FailRecoveryOnFailedPostFailoverProcesses
	defer func() { config.Config.FailRecoveryOnFailedPostFailoverProcesses = orig }()
	config.Config.FailRecoveryOnFailedPostFailoverProcesses = true

	origPersist := dt.persistResolvedRecoveryFn
	dt.persistResolvedRecoveryFn = func(tr *TopologyRecovery) error {
		t.Fatal("persist should not run on success")
		return nil
	}
	defer func() { dt.persistResolvedRecoveryFn = origPersist }()

	tr := NewTopologyRecovery(inst.ReplicationAnalysis{})
	tr.IsSuccessful = true

	if err := dt.runPostSuccessFailoverProcesses([]string{"true"}, "PostFailoverProcesses", tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !tr.IsSuccessful {
		t.Fatal("success should keep IsSuccessful")
	}
}
