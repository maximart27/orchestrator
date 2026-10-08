// Transitional: package-level wrappers around DefaultTenant(), the tenant of the
// legacy single-tenant setup, so the packages that still use the pre-multi-tenant
// API keep compiling. They are removed once those packages resolve their tenant in
// the TenantManager.

package logic

import (
	"github.com/proxysql/orchestrator/go/inst"
)

// AuditTopologyRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AuditTopologyRecovery on the tenant it applies to.
func AuditTopologyRecovery(topologyRecovery *TopologyRecovery, message string) error {
	return DefaultTenant().AuditTopologyRecovery(topologyRecovery, message)
}

// MasterFailoverGeographicConstraintSatisfied runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).MasterFailoverGeographicConstraintSatisfied on the tenant it applies to.
func MasterFailoverGeographicConstraintSatisfied(analysisEntry *inst.ReplicationAnalysis, suggestedInstance *inst.Instance) (satisfied bool, dissatisfiedReason string) {
	return DefaultTenant().MasterFailoverGeographicConstraintSatisfied(analysisEntry, suggestedInstance)
}

// SuggestReplacementForPromotedReplica runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).SuggestReplacementForPromotedReplica on the tenant it applies to.
func SuggestReplacementForPromotedReplica(topologyRecovery *TopologyRecovery, deadInstanceKey *inst.InstanceKey, promotedReplica *inst.Instance, candidateInstanceKey *inst.InstanceKey) (replacement *inst.Instance, actionRequired bool, err error) {
	return DefaultTenant().SuggestReplacementForPromotedReplica(topologyRecovery, deadInstanceKey, promotedReplica, candidateInstanceKey)
}

// GetCandidateSiblingOfIntermediateMaster runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).GetCandidateSiblingOfIntermediateMaster on the tenant it applies to.
func GetCandidateSiblingOfIntermediateMaster(topologyRecovery *TopologyRecovery, intermediateMasterInstance *inst.Instance) (*inst.Instance, error) {
	return DefaultTenant().GetCandidateSiblingOfIntermediateMaster(topologyRecovery, intermediateMasterInstance)
}

// RecoverDeadIntermediateMaster runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).RecoverDeadIntermediateMaster on the tenant it applies to.
func RecoverDeadIntermediateMaster(topologyRecovery *TopologyRecovery, skipProcesses bool) (successorInstance *inst.Instance, err error) {
	return DefaultTenant().RecoverDeadIntermediateMaster(topologyRecovery, skipProcesses)
}

// RecoverDeadReplicationGroupMemberWithReplicas runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).RecoverDeadReplicationGroupMemberWithReplicas on the tenant it applies to.
func RecoverDeadReplicationGroupMemberWithReplicas(topologyRecovery *TopologyRecovery, skipProcesses bool) (successorInstance *inst.Instance, err error) {
	return DefaultTenant().RecoverDeadReplicationGroupMemberWithReplicas(topologyRecovery, skipProcesses)
}

// RecoverDeadCoMaster runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).RecoverDeadCoMaster on the tenant it applies to.
func RecoverDeadCoMaster(topologyRecovery *TopologyRecovery, skipProcesses bool) (promotedReplica *inst.Instance, lostReplicas [](*inst.Instance), err error) {
	return DefaultTenant().RecoverDeadCoMaster(topologyRecovery, skipProcesses)
}

// CheckAndRecover runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).CheckAndRecover on the tenant it applies to.
func CheckAndRecover(specificInstance *inst.InstanceKey, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedReplicaKey *inst.InstanceKey, err error) {
	return DefaultTenant().CheckAndRecover(specificInstance, candidateInstanceKey, skipProcesses)
}

// ForceExecuteRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ForceExecuteRecovery on the tenant it applies to.
func ForceExecuteRecovery(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	return DefaultTenant().ForceExecuteRecovery(analysisEntry, candidateInstanceKey, skipProcesses)
}

// ForceMasterFailover runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ForceMasterFailover on the tenant it applies to.
func ForceMasterFailover(clusterName string) (topologyRecovery *TopologyRecovery, err error) {
	return DefaultTenant().ForceMasterFailover(clusterName)
}

// ForceMasterTakeover runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ForceMasterTakeover on the tenant it applies to.
func ForceMasterTakeover(clusterName string, destination *inst.Instance) (topologyRecovery *TopologyRecovery, err error) {
	return DefaultTenant().ForceMasterTakeover(clusterName, destination)
}

// GracefulMasterTakeover runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).GracefulMasterTakeover on the tenant it applies to.
func GracefulMasterTakeover(clusterName string, designatedKey *inst.InstanceKey, auto bool) (topologyRecovery *TopologyRecovery, promotedMasterCoordinates *inst.BinlogCoordinates, err error) {
	return DefaultTenant().GracefulMasterTakeover(clusterName, designatedKey, auto)
}
