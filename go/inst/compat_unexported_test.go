// Transitional: wrappers, over DefaultStore(), for the unexported functions that became
// Store methods and that the existing tests still call.

package inst

import (
	"github.com/proxysql/orchestrator/go/kv"
)

// getClusterMasterKVPair runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).getClusterMasterKVPair on the tenant's Store.
func getClusterMasterKVPair(clusterAlias string, masterKey *InstanceKey) *kv.KVPair {
	return DefaultStore().getClusterMasterKVPair(clusterAlias, masterKey)
}

// sortedReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).sortedReplicas on the tenant's Store.
func sortedReplicas(replicas [](*Instance), stopReplicationMethod StopReplicationMethod) [](*Instance) {
	return DefaultStore().sortedReplicas(replicas, stopReplicationMethod)
}

// chooseCandidateReplica runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).chooseCandidateReplica on the tenant's Store.
func chooseCandidateReplica(replicas [](*Instance)) (candidateReplica *Instance, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas [](*Instance), err error) {
	return DefaultStore().chooseCandidateReplica(replicas)
}
