package logic

import (
	"testing"
	"time"

	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/inst"
)

// Scenario 7: Raft re-applies log entries after a restart (from the last snapshot, over a
// backend that kept its data) and, with the pre-fix lazy snapshot, after a restore. This
// applies every command of CommandApplier twice and compares the state with the state
// after applying it once. The test FAILS for none of them: it asserts the CURRENT,
// documented behaviour so that a change in it is noticed. (The one that was not idempotent,
// write-recovery-step, is since the leader stamps the step with its instant.)
func TestApplierReapplyingACommandIsIdempotentOrDocumented(t *testing.T) {
	key := inst.InstanceKey{Hostname: "db1", Port: 3306}
	downtime := inst.NewDowntime(&key, "me", "r", time.Hour)
	analysis := inst.ReplicationAnalysis{AnalyzedInstanceKey: key, Analysis: inst.DeadMaster,
		ClusterDetails: inst.ClusterInfo{ClusterName: "cl", ClusterAlias: "al"}, CountReplicas: 1}
	recovery := &TopologyRecovery{UID: "uid-1", AnalysisEntry: analysis}

	// pre-sets state so that the op has something to act on
	type opCase struct {
		op    string
		value interface{}
		setup []opCase
		// idempotent: the state after two applications equals the state after one
		idempotent bool
		// errorOnReapply: the second application answers an error (the effect is already there)
		errorOnReapply bool
		note           string
	}
	writeRecovery := opCase{op: "write-recovery", value: recovery}
	cases := []opCase{
		{op: "injected-pseudo-gtid", value: "cl", idempotent: true},
		{op: "forget", value: key, idempotent: true, errorOnReapply: true,
			note: "the second application answers 'instance not found' (state unchanged)"},
		{op: "forget-cluster", value: "cl", idempotent: true},
		{op: "begin-downtime", value: downtime, idempotent: true},
		{op: "end-downtime", value: key, setup: []opCase{{op: "begin-downtime", value: downtime}}, idempotent: true},
		{op: "register-candidate", value: inst.CandidateDatabaseInstance{Hostname: "db1", Port: 3306, PromotionRule: inst.PreferPromoteRule}, idempotent: true},
		{op: "register-hostname-unresolve", value: inst.HostnameRegistration{Key: key, Hostname: "unresolved"}, idempotent: true},
		{op: "submit-pool-instances", value: inst.PoolInstancesSubmission{Pool: "p", DelimitedInstances: "db1:3306,db2:3306"}, idempotent: true},
		{op: "register-failure-detection", value: analysis, idempotent: true},
		{op: "write-recovery", value: recovery, idempotent: true},
		{op: "write-recovery-step", value: TopologyRecoveryStep{RecoveryUID: "uid-1", Message: "step", AuditAt: "2026-01-02 03:04:05"}, setup: []opCase{writeRecovery},
			idempotent: true},
		{op: "write-recovery-step", value: TopologyRecoveryStep{RecoveryUID: "uid-1", Message: "step"}, setup: []opCase{writeRecovery},
			idempotent: false, note: "an entry without AuditAt (log of a version that did not stamp the step) has no natural key: a row per application"},
		{op: "resolve-recovery", value: recovery, setup: []opCase{writeRecovery}, idempotent: true},
		{op: "ack-recovery", value: RecoveryAcknowledgement{Owner: "me", Comment: "c", AllRecoveries: true}, setup: []opCase{writeRecovery}, idempotent: true},
		{op: "disable-global-recoveries", idempotent: true},
		{op: "enable-global-recoveries", idempotent: true},
		{op: "put-key-value", value: map[string]string{"Key": "k", "Value": "v"}, idempotent: true},
		{op: "put-instance-tag", value: inst.InstanceTag{Key: key, T: inst.Tag{TagName: "t", TagValue: "v"}}, idempotent: true},
		{op: "delete-instance-tag", value: inst.InstanceTag{Key: key, T: inst.Tag{TagName: "t"}},
			setup: []opCase{{op: "put-instance-tag", value: inst.InstanceTag{Key: key, T: inst.Tag{TagName: "t", TagValue: "v"}}}}, idempotent: true},
		{op: "set-cluster-alias-manual-override", value: [2]string{"cl", "alias"}, idempotent: true},
	}

	for _, tc := range cases {
		tc := tc
		name := tc.op
		if tc.op == "write-recovery-step" && !tc.idempotent {
			name += "-without-audit-at"
		}
		t.Run(name, func(t *testing.T) {
			m, _ := newTestManager(t, []string{"a"}, nil)
			tn := mustTenant(t, m, "a")
			applier := NewCommandApplier(m)
			tenantWriteInstance(t, tn, "db1", 3306)
			tenantWriteInstance(t, tn, "db2", 3306)
			apply := func(c opCase, allowError ...bool) {
				var payload []byte
				if c.value != nil {
					payload = mustJSON(t, c.value)
				}
				res := applier.ApplyTenantCommand("a", c.op, payload)
				if _, isMap := res.(*inst.InstanceKeyMap); isMap && c.op == "delete-instance-tag" {
					res = nil // it answers the instances it untagged
				}
				if res != nil && len(allowError) == 0 {
					t.Fatalf("%s: %v", c.op, res)
				}
			}
			for _, s := range tc.setup {
				apply(s)
			}
			apply(opCase{op: tc.op, value: tc.value})
			once := tenantDump(t, tn)
			auditOnce := auditRows(t, tn)
			apply(opCase{op: tc.op, value: tc.value}, tc.errorOnReapply)
			twice := tenantDump(t, tn)
			if (once == twice) != tc.idempotent {
				if tc.idempotent {
					t.Errorf("%s was expected to be idempotent but re-applying it changed the state:\n--- once\n%s\n--- twice\n%s", tc.op, once, twice)
				} else {
					t.Errorf("%s was documented as NOT idempotent but re-applying it changed nothing", tc.op)
				}
			}
			if tc.errorOnReapply {
				t.Logf("%s: %s", tc.op, tc.note)
			}
			if !tc.idempotent {
				t.Logf("NOT IDEMPOTENT: %s: %s", tc.op, tc.note)
			}
			if a := auditRows(t, tn); a != auditOnce {
				t.Logf("%s: re-applying adds %d audit row(s) (node-local audit log, not replicated state)", tc.op, a-auditOnce)
			}
		})
	}
}

// A begin-downtime carries the absolute end of the downtime (computed by the leader), so replaying
// it neither extends the downtime nor resurrects one that already ended.
func TestApplierReapplyingBeginDowntimeDoesNotExtendNorResurrect(t *testing.T) {
	m, _ := newTestManager(t, []string{"a"}, nil)
	tn := mustTenant(t, m, "a")
	applier := NewCommandApplier(m)
	key := inst.InstanceKey{Hostname: "db1", Port: 3306}
	endOf := func() (end string, rows int) {
		if err := tn.Backend.QueryRowsMap("select end_timestamp from database_instance_downtime", func(m sqlutils.RowMap) error {
			end, rows = m.GetString("end_timestamp"), rows+1
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return end, rows
	}
	long := mustJSON(t, inst.NewDowntime(&key, "me", "r", time.Hour))
	if res := applier.ApplyTenantCommand("a", "begin-downtime", long); res != nil {
		t.Fatal(res)
	}
	first, _ := endOf()
	time.Sleep(1200 * time.Millisecond)
	if res := applier.ApplyTenantCommand("a", "begin-downtime", long); res != nil {
		t.Fatal(res)
	}
	second, _ := endOf()
	// same absolute end (SQLite NOW() has one second resolution)
	t1, err1 := parseBackendTime(first)
	t2, err2 := parseBackendTime(second)
	if err1 != nil || err2 != nil {
		t.Fatalf("unparsable end_timestamp %q / %q", first, second)
	}
	if d := t2.Sub(t1); d < -2*time.Second || d > 2*time.Second {
		t.Errorf("replaying begin-downtime moved the end of the downtime by %v (%s -> %s)", d, first, second)
	}

	// a downtime that already ended is not resurrected
	m2, _ := newTestManager(t, []string{"a"}, nil)
	tn = mustTenant(t, m2, "a")
	applier = NewCommandApplier(m2)
	short := inst.NewDowntime(&key, "me", "r", time.Second)
	payload := mustJSON(t, short)
	time.Sleep(1500 * time.Millisecond)
	if res := applier.ApplyTenantCommand("a", "begin-downtime", payload); res != nil {
		t.Fatal(res)
	}
	if _, rows := endOf(); rows != 0 {
		t.Errorf("replaying an already ended downtime resurrected it")
	}
}
