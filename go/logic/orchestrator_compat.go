// Transitional: package-level wrappers around DefaultTenant(), the tenant of the
// legacy single-tenant setup, so the packages that still use the pre-multi-tenant
// API keep compiling. They are removed once those packages resolve their tenant in
// the TenantManager.

package logic

import (
	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/kv"
)

// DiscoverInstance runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).DiscoverInstance on the tenant it applies to.
func DiscoverInstance(instanceKey inst.InstanceKey) {
	DefaultTenant().DiscoverInstance(instanceKey)
}

// InjectPseudoGTIDOnWriters runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).InjectPseudoGTIDOnWriters on the tenant it applies to.
func InjectPseudoGTIDOnWriters() error {
	return DefaultTenant().InjectPseudoGTIDOnWriters()
}

// SubmitMastersToKvStores runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).SubmitMastersToKvStores on the tenant it applies to.
func SubmitMastersToKvStores(clusterName string, force bool) (kvPairs [](*kv.KVPair), submittedCount int, err error) {
	return DefaultTenant().SubmitMastersToKvStores(clusterName, force)
}
