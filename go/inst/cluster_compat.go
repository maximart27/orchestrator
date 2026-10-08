// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

import (
	"github.com/proxysql/orchestrator/go/kv"
)

// GetClusterMasterKVKey runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetClusterMasterKVKey on the tenant's Store.
func GetClusterMasterKVKey(clusterAlias string) string {
	return DefaultStore().GetClusterMasterKVKey(clusterAlias)
}

// GetClusterMasterKVPairs runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetClusterMasterKVPairs on the tenant's Store.
func GetClusterMasterKVPairs(clusterAlias string, masterKey *InstanceKey) (kvPairs [](*kv.KVPair)) {
	return DefaultStore().GetClusterMasterKVPairs(clusterAlias, masterKey)
}

// ReadRecoveryInfo runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ClusterInfoReadRecoveryInfo on the tenant's Store.
func (this *ClusterInfo) ReadRecoveryInfo() {
	DefaultStore().ClusterInfoReadRecoveryInfo(this)
}

// ApplyClusterAlias runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ClusterInfoApplyClusterAlias on the tenant's Store.
func (this *ClusterInfo) ApplyClusterAlias() {
	DefaultStore().ClusterInfoApplyClusterAlias(this)
}
