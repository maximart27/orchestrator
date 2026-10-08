package logic

import (
	"testing"

	"github.com/proxysql/golib/sqlutils"
)

func recoveryStepMessages(t *testing.T, tn *Tenant) (messages []string) {
	t.Helper()
	if err := tn.Backend.QueryRowsMap("select message from topology_recovery_steps order by recovery_step_id", func(m sqlutils.RowMap) error {
		messages = append(messages, m.GetString("message"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return messages
}

// Raft re-applies log entries after a restart over a backend that kept its data: a recovery step
// (stamped by the leader) applied twice is one row, while the same message at another instant, or
// of another recovery, is another step.
func TestRecoveryStepIsWrittenOnceButDistinctStepsAreKept(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	tn := mustTenant(t, m, "a")
	applier := NewCommandApplier(m)
	apply := func(uid, at, message string) {
		t.Helper()
		step := TopologyRecoveryStep{RecoveryUID: uid, AuditAt: at, Message: message}
		if res := applier.ApplyTenantCommand("a", "write-recovery-step", mustJSON(t, step)); res != nil {
			t.Fatalf("write-recovery-step: %v", res)
		}
	}
	apply("uid-1", "2026-01-02 03:04:05", "relocating replicas")
	apply("uid-1", "2026-01-02 03:04:05", "relocating replicas") // replay
	if n := len(recoveryStepMessages(t, tn)); n != 1 {
		t.Fatalf("a step applied twice left %d rows, want 1", n)
	}
	apply("uid-1", "2026-01-02 03:04:09", "relocating replicas") // same message, later: a real new step
	apply("uid-2", "2026-01-02 03:04:05", "relocating replicas") // same message and instant, other recovery
	apply("uid-1", "2026-01-02 03:04:05", "promoting")           // same instant, other message
	if n := len(recoveryStepMessages(t, tn)); n != 4 {
		t.Fatalf("%d rows after 3 distinct steps were added to 1, want 4", n)
	}
	// replaying the whole log again changes nothing
	apply("uid-1", "2026-01-02 03:04:05", "relocating replicas")
	apply("uid-1", "2026-01-02 03:04:09", "relocating replicas")
	apply("uid-2", "2026-01-02 03:04:05", "relocating replicas")
	apply("uid-1", "2026-01-02 03:04:05", "promoting")
	if n := len(recoveryStepMessages(t, tn)); n != 4 {
		t.Fatalf("%d rows after replaying the log, want 4", n)
	}
}

// The step published with Raft is stamped by the leader (and a node that applies it keeps that instant).
func TestRecoveryStepKeepsTheInstantOfTheLeader(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	tn := mustTenant(t, m, "a")
	step := TopologyRecoveryStep{RecoveryUID: "uid-1", AuditAt: "2026-01-02 03:04:05", Message: "x"}
	if res := NewCommandApplier(m).ApplyTenantCommand("a", "write-recovery-step", mustJSON(t, step)); res != nil {
		t.Fatal(res)
	}
	steps, err := tn.ReadTopologyRecoverySteps("uid-1")
	if err != nil || len(steps) != 1 {
		t.Fatalf("steps %v, err %v", steps, err)
	}
	if got := steps[0].AuditAt; got != "2026-01-02 03:04:05" && got != "2026-01-02T03:04:05Z" {
		t.Errorf("audit_at = %q, want the instant of the leader", got)
	}
}
