// Transitional: package-level wrappers around DefaultTenant(), the tenant of the
// legacy single-tenant setup, so the packages that still use the pre-multi-tenant
// API keep compiling. They are removed once those packages resolve their tenant in
// the TenantManager.

package logic

import (
	"github.com/proxysql/orchestrator/go/inst"
)

// PostgreSQLGracefulPrimarySwitchover runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).PostgreSQLGracefulPrimarySwitchover on the tenant it applies to.
func PostgreSQLGracefulPrimarySwitchover(clusterName string, designatedKey *inst.InstanceKey, auto bool) (topologyRecovery *TopologyRecovery, err error) {
	return DefaultTenant().PostgreSQLGracefulPrimarySwitchover(clusterName, designatedKey, auto)
}
