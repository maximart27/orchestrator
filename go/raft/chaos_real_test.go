package orcraft_test

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/logic"
)

// recoveryAck mirrors logic.RecoveryAcknowledgement without its time field.
type recoveryAck = logic.RecoveryAcknowledgement

var realHosts = []string{"db1", "db2", "db3", "db4"}

// realOp draws a random, replicated, tenant-scoped command. Every value carries the ID of
// its tenant so that a leak into another tenant is visible in that tenant's dump.
// Ops that connect to a real server (discover) are left out.
func realOp(t testing.TB, rng *rand.Rand, tenant string) (op string, value []byte) {
	key := inst.InstanceKey{Hostname: realHosts[rng.Intn(len(realHosts))], Port: 3306}
	n := rng.Intn(4)
	switch rng.Intn(16) {
	case 0, 1:
		return "put-instance-tag", jsonOf(t, inst.InstanceTag{Key: key, T: inst.Tag{TagName: fmt.Sprintf("t%d", n), TagValue: "tenant:" + tenant}})
	case 2:
		return "delete-instance-tag", jsonOf(t, inst.InstanceTag{Key: key, T: inst.Tag{TagName: fmt.Sprintf("t%d", n)}})
	case 3, 4:
		return "begin-downtime", jsonOf(t, inst.NewDowntime(&key, "owner:"+tenant, "reason:"+tenant, time.Hour))
	case 5:
		return "end-downtime", jsonOf(t, key)
	case 6:
		return "register-candidate", jsonOf(t, inst.CandidateDatabaseInstance{Hostname: key.Hostname, Port: key.Port, PromotionRule: inst.PreferPromoteRule})
	case 7:
		return "set-cluster-alias-manual-override", jsonOf(t, [2]string{fmt.Sprintf("cluster%d", n), "alias:" + tenant})
	case 8:
		return "disable-global-recoveries", nil
	case 9:
		return "enable-global-recoveries", nil
	case 10:
		return "put-key-value", jsonOf(t, map[string]string{"Key": fmt.Sprintf("%s/k%d", tenant, n), "Value": "tenant:" + tenant})
	case 11:
		return "submit-pool-instances", jsonOf(t, inst.PoolInstancesSubmission{Pool: fmt.Sprintf("pool-%s-%d", tenant, n), DelimitedInstances: "db1:3306,db2:3306"})
	case 12:
		return "register-hostname-unresolve", jsonOf(t, inst.HostnameRegistration{Key: key, Hostname: "unres-" + tenant})
	case 13:
		analysis := inst.ReplicationAnalysis{AnalyzedInstanceKey: key, Analysis: inst.DeadMaster, CountReplicas: 1,
			ClusterDetails: inst.ClusterInfo{ClusterName: fmt.Sprintf("cluster%d", n), ClusterAlias: "alias:" + tenant}}
		return "write-recovery", jsonOf(t, &logic.TopologyRecovery{UID: fmt.Sprintf("uid-%s-%d", tenant, rng.Intn(1000)), AnalysisEntry: analysis})
	case 14:
		// ack-recovery is deliberately NOT drawn: its outcome depends on the second in which each node
		// applies it (logic: TestApplierAckTwiceInTheSameSecondDependsOnTheClock), which is a known,
		// separate source of divergence that would drown the rest here.
		return "injected-pseudo-gtid", jsonOf(t, fmt.Sprintf("cluster%d", n))
	default:
		if rng.Intn(5) == 0 {
			return "forget", jsonOf(t, key)
		}
		return "put-instance-tag", jsonOf(t, inst.InstanceTag{Key: key, T: inst.Tag{TagName: fmt.Sprintf("t%d", n), TagValue: "tenant:" + tenant}})
	}
}

// assertNoCrossTenantLeak fails if the state of tenant `id` mentions a marker of another tenant.
func assertNoCrossTenantLeak(t *testing.T, node int, id string, dump string, tenants []string) {
	t.Helper()
	for _, other := range tenants {
		if other == id {
			continue
		}
		for _, marker := range []string{"tenant:" + other, "owner:" + other, "alias:" + other, "uid-" + other + "-", "unres-" + other, "pool-" + other + "-"} {
			if strings.Contains(dump, marker) {
				t.Errorf("node%d: tenant %s holds %q: a command of tenant %s was applied to it", node, id, marker, other)
			}
		}
	}
}

func runRealChaos(t *testing.T, seed int64, nodes, steps int, tune func(*raft.Config), rebuild bool) {
	t.Helper()
	tenants := []string{"a", "b", "c"}
	apps, reals := newRealApps(t, nodes, tenants, tenants, realHosts...)
	for _, r := range reals {
		r.rebuildOnRestart = rebuild
	}
	c := newChaosCluster(t, apps, tune)
	c.WaitLeader()
	var mu sync.Mutex
	published, acked := 0, 0
	driveChaos(t, c, seed, nodes, steps, func(prng *rand.Rand) {
		tn := tenants[prng.Intn(len(tenants))]
		op, v := realOp(t, prng, tn)
		_, err := c.PublishAny(tn, op, v)
		mu.Lock()
		published++
		if err == nil {
			acked++
		}
		mu.Unlock()
		time.Sleep(time.Duration(2+prng.Intn(6)) * time.Millisecond)
	})
	c.WaitConverged(60 * time.Second)
	c.AssertIdentical(tenants...)
	c.AssertSingleLeaderPerTerm()
	for i, r := range reals {
		if !c.Up(i) {
			continue
		}
		for _, id := range tenants {
			assertNoCrossTenantLeak(t, i, id, dumpTenant(t, r.tenant(id)), tenants)
		}
	}
	t.Logf("published=%d acked=%d", published, acked)
}

// Scenario 1 with the real appliers: random replicated commands for three tenants, published
// against whoever leads, under partitions, crashes (Raft restarted over the same data) and
// leadership changes. At the end every tenant has the same state (SQLite) on every node, and
// no command of one tenant ever touched another tenant.
func TestRealChaosConvergence(t *testing.T) {
	seeds := chaosSeeds(2)
	for s := 0; s < seeds; s++ {
		seed := int64(3000 + s)
		nodes := 3
		t.Run(fmt.Sprintf("seed%d_n%d", seed, nodes), func(t *testing.T) {
			runRealChaos(t, seed, nodes, 30, nil, false)
		})
	}
}

// Same, but a node that crashes comes back as a whole new process: a new TenantManager
// (new Tenants, Stores and Backends) over the same SQLite files, and Raft over the same data.
func TestRealChaosConvergenceWithProcessRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	seeds := chaosSeeds(2)
	for s := 0; s < seeds; s++ {
		seed := int64(4000 + s)
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			runRealChaos(t, seed, 3, 30, nil, true)
		})
	}
}
