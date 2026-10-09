package logic

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/proxysql/orchestrator/go/inst"
)

// markerLeadership is a Leadership that is lost once a marker file exists: a hook creates it, so the
// node loses the term at a precise point of the recovery (the hook finishing).
type markerLeadership struct{ marker string }

func (l markerLeadership) lost() bool {
	_, err := os.Stat(l.marker)
	return err == nil
}
func (l markerLeadership) IsLeader() bool         { return !l.lost() }
func (l markerLeadership) IsLeaderOrActive() bool { return !l.lost() }

func raftRecoveries(t *testing.T, enabled bool) {
	t.Helper()
	prev := recoveryRaftEnabled
	recoveryRaftEnabled = func() bool { return enabled }
	t.Cleanup(func() { recoveryRaftEnabled = prev })
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func deadMasterRecovery() *TopologyRecovery {
	return NewTopologyRecovery(inst.ReplicationAnalysis{
		AnalyzedInstanceKey: inst.InstanceKey{Hostname: "db1", Port: 3306},
		Analysis:            inst.DeadMaster,
		ClusterDetails:      inst.ClusterInfo{ClusterName: "cl", ClusterAlias: "al"},
	})
}

// The scenario 3b of the chaos suite: the leader loses the term while its pre-failover hook runs. When
// the hook returns it must not go on to promote a replica.
func TestRecoveryStopsWhenTheLeaderLosesTheTermDuringThePreFailoverHook(t *testing.T) {
	raftRecoveries(t, true)
	dir := t.TempDir()
	lost, pre := filepath.Join(dir, "lost"), filepath.Join(dir, "pre")
	m, _ := newTestManager(t, []string{"a"}, nil, WithLeadership(markerLeadership{marker: lost}))
	a := mustTenant(t, m, "a")
	a.Cfg.PreFailoverProcesses = []string{"touch " + pre + " && touch " + lost}

	tr := deadMasterRecovery()
	attempted, promoted, _, err := a.recoverDeadMaster(tr, nil, false)
	if !exists(pre) {
		t.Fatal("the pre-failover hook should have run (this node was the leader)")
	}
	if !errors.Is(err, errLeadershipLost) {
		t.Fatalf("the recovery must stop with errLeadershipLost once the term is lost, got %v", err)
	}
	if attempted || promoted != nil {
		t.Fatalf("no replica may be promoted by a node that lost the term (attempted=%v promoted=%v)", attempted, promoted)
	}
	if len(tr.AllErrors) == 0 {
		t.Fatal("the abort is not recorded in the recovery")
	}
}

// The hooks of a recovery are not run by a node that is no longer the leader, and a hook list is cut
// in the middle when the term is lost.
func TestHooksAreNotRunAfterTheTermIsLost(t *testing.T) {
	raftRecoveries(t, true)
	dir := t.TempDir()
	lost, h1, h2 := filepath.Join(dir, "lost"), filepath.Join(dir, "h1"), filepath.Join(dir, "h2")
	m, _ := newTestManager(t, []string{"a"}, nil, WithLeadership(markerLeadership{marker: lost}))
	a := mustTenant(t, m, "a")

	err := a.executeProcesses([]string{"touch " + h1 + " && touch " + lost, "touch " + h2}, "PostFailoverProcesses", deadMasterRecovery(), false)
	if !errors.Is(err, errLeadershipLost) {
		t.Fatalf("got %v", err)
	}
	if !exists(h1) || exists(h2) {
		t.Fatalf("hook 1 ran=%v (want true), hook 2 ran=%v (want false)", exists(h1), exists(h2))
	}

	// the post hooks of a successful promotion: refused, and the refusal is not swallowed as a hook failure
	post := filepath.Join(dir, "post")
	a.Cfg.FailRecoveryOnFailedPostFailoverProcesses = false
	err = a.runPostSuccessFailoverProcesses([]string{"touch " + post}, "PostMasterFailoverProcesses", deadMasterRecovery())
	if !errors.Is(err, errLeadershipLost) || exists(post) {
		t.Fatalf("post hook after the term was lost: err=%v ran=%v", err, exists(post))
	}
}

// Nothing changes for a leader that keeps the term, nor without Raft (where the elected node runs
// everything and this check does not apply).
func TestHooksRunForAStableLeaderAndWithoutRaft(t *testing.T) {
	dir := t.TempDir()
	m, leader := newTestManager(t, []string{"a"}, nil)
	a := mustTenant(t, m, "a")

	raftRecoveries(t, true)
	leader.leader.Store(true)
	h1 := filepath.Join(dir, "h1")
	if err := a.executeProcesses([]string{"touch " + h1}, "PostFailoverProcesses", deadMasterRecovery(), true); err != nil || !exists(h1) {
		t.Fatalf("a stable leader: err=%v ran=%v", err, exists(h1))
	}

	raftRecoveries(t, false)
	leader.leader.Store(false)
	h2 := filepath.Join(dir, "h2")
	if err := a.executeProcesses([]string{"touch " + h2}, "PostFailoverProcesses", deadMasterRecovery(), true); err != nil || !exists(h2) {
		t.Fatalf("without raft: err=%v ran=%v", err, exists(h2))
	}
}
