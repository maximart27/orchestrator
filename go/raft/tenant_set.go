package orcraft

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/proxysql/golib/log"
)

// TenantSetCommand is the global op by which the leader announces which tenants it
// runs. Every node compares the announcement with its own tenants: a Raft command
// names its tenant, and a node that lacks it drops the command for good (adding the
// tenant later does not recover what was dropped), so a divergence must be loud.
// A node that does not know the op (a version older than this one) answers
// "Unknown command op" like it does for any unknown op: it neither aborts nor diverges.
const TenantSetCommand = "tenant-set"

// TenantSetPublishInterval is how often the leader re-announces its tenant set (it
// also does so as soon as it is elected, together with leader-uri). The announcement
// is what lets a follower restarted with a different tenants file notice it.
const TenantSetPublishInterval = 5 * time.Minute

// TenantSetEntry is one tenant of a TenantSet. Hash is a stable digest of the tenant
// configuration that must be the same on every node (see TenantHash); it never
// covers credentials or any secret. Provider is informational (diagnostics only).
type TenantSetEntry struct {
	ID       string `json:"id"`
	Provider string `json:"provider,omitempty"`
	Hash     string `json:"hash"`
}

// TenantSet is the payload of a TenantSetCommand: the tenants a node runs, sorted by
// ID, and the node that announces them (its Raft advertise address).
type TenantSet struct {
	Node    string           `json:"node,omitempty"`
	Tenants []TenantSetEntry `json:"tenants"`
}

// TenantHash is the stable digest of the configuration of a tenant that every node
// must share: today its ID and its provider (the database engine). Credentials, hosts
// and the rest of the tenant configuration may legitimately differ per node and are
// never hashed, logged nor sent.
func TenantHash(id, provider string) string {
	sum := sha256.Sum256([]byte(id + "\x00" + provider))
	return hex.EncodeToString(sum[:])
}

// NewTenantSet builds a TenantSet from (id, provider) pairs, sorted by ID.
func NewTenantSet(node string, idToProvider map[string]string) TenantSet {
	set := TenantSet{Node: node, Tenants: make([]TenantSetEntry, 0, len(idToProvider))}
	for id, provider := range idToProvider {
		set.Tenants = append(set.Tenants, TenantSetEntry{ID: id, Provider: provider, Hash: TenantHash(id, provider)})
	}
	sort.Slice(set.Tenants, func(i, j int) bool { return set.Tenants[i].ID < set.Tenants[j].ID })
	return set
}

// TenantSetDiff is the difference between the tenants announced by another node
// ("remote") and the ones this node runs ("local").
type TenantSetDiff struct {
	// MissingHere are tenants the remote node runs and this node does not: this node
	// drops their commands.
	MissingHere []string
	// ExtraHere are tenants this node runs and the remote node does not.
	ExtraHere []string
	// Different are tenants both run but with a different hash (e.g. another provider).
	Different []string
}

// Empty tells whether both sets are the same.
func (d TenantSetDiff) Empty() bool {
	return len(d.MissingHere) == 0 && len(d.ExtraHere) == 0 && len(d.Different) == 0
}

func (d TenantSetDiff) String() string {
	return fmt.Sprintf("missing on this node: %v; only on this node: %v; different configuration: %v", d.MissingHere, d.ExtraHere, d.Different)
}

// DiffTenantSets compares what a remote node announces with what this node runs.
func DiffTenantSets(remote, local TenantSet) TenantSetDiff {
	localByID := make(map[string]TenantSetEntry, len(local.Tenants))
	for _, t := range local.Tenants {
		localByID[t.ID] = t
	}
	var d TenantSetDiff
	seen := make(map[string]struct{}, len(remote.Tenants))
	for _, t := range remote.Tenants {
		seen[t.ID] = struct{}{}
		l, found := localByID[t.ID]
		switch {
		case !found:
			d.MissingHere = append(d.MissingHere, t.ID)
		case l.Hash != t.Hash:
			d.Different = append(d.Different, t.ID)
		}
	}
	for _, t := range local.Tenants {
		if _, ok := seen[t.ID]; !ok {
			d.ExtraHere = append(d.ExtraHere, t.ID)
		}
	}
	sort.Strings(d.MissingHere)
	sort.Strings(d.ExtraHere)
	sort.Strings(d.Different)
	return d
}

// SameTenants is DiffTenantSets(a, b).Empty().
func SameTenants(a, b TenantSet) bool { return DiffTenantSets(a, b).Empty() }

var tenantSetMismatch int64

// TenantSetMismatch tells whether the last announcement of the tenant set showed
// tenants different from the ones this node runs (the value of the metric
// tenant_set_mismatch).
func TenantSetMismatch() bool { return atomic.LoadInt64(&tenantSetMismatch) == 1 }

// SetTenantSetMismatch records the outcome of the last tenant-set comparison.
func SetTenantSetMismatch(mismatch bool) {
	var v int64
	if mismatch {
		v = 1
	}
	atomic.StoreInt64(&tenantSetMismatch, v)
}

// tenantSetSource returns the tenants this node runs; set by the logic package.
var tenantSetSource atomic.Pointer[func() map[string]string]

// SetTenantSetSource registers where the leader reads the tenants it announces from
// (tenant ID -> provider).
func SetTenantSetSource(source func() map[string]string) {
	if source == nil {
		tenantSetSource.Store(nil)
		return
	}
	tenantSetSource.Store(&source)
}

// ThisNode is the identity this node announces itself with ("" when Raft is not set up).
func ThisNode() string {
	if store == nil {
		return ""
	}
	return store.raftAdvertise
}

// PublishTenantSet announces the tenants of this node to the group. Only the leader
// can; anywhere else, or without a registered source, it does nothing.
func PublishTenantSet() error {
	src := tenantSetSource.Load()
	if src == nil || !IsRaftEnabled() || !IsLeader() {
		return nil
	}
	set := NewTenantSet(ThisNode(), (*src)())
	if _, err := PublishCommand(TenantSetCommand, set); err != nil {
		log.Warningf("orchestrator/raft: publishing the tenant set: %+v", err)
		return err
	}
	return nil
}

// DescribeTenantSet is a log-friendly rendering (IDs only).
func DescribeTenantSet(s TenantSet) string {
	ids := make([]string, 0, len(s.Tenants))
	for _, t := range s.Tenants {
		ids = append(ids, t.ID)
	}
	return strings.Join(ids, ",")
}
