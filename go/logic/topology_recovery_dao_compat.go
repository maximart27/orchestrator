// Transitional: package-level wrappers around DefaultTenant(), the tenant of the
// legacy single-tenant setup, so the packages that still use the pre-multi-tenant
// API keep compiling. They are removed once those packages resolve their tenant in
// the TenantManager.

package logic

import (
	"github.com/proxysql/orchestrator/go/inst"
)

// AttemptFailureDetectionRegistration runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AttemptFailureDetectionRegistration on the tenant it applies to.
func AttemptFailureDetectionRegistration(analysisEntry *inst.ReplicationAnalysis) (registrationSuccessful bool, err error) {
	return DefaultTenant().AttemptFailureDetectionRegistration(analysisEntry)
}

// ClearActiveFailureDetections runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ClearActiveFailureDetections on the tenant it applies to.
func ClearActiveFailureDetections() error {
	return DefaultTenant().ClearActiveFailureDetections()
}

// AttemptRecoveryRegistration runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AttemptRecoveryRegistration on the tenant it applies to.
func AttemptRecoveryRegistration(analysisEntry *inst.ReplicationAnalysis, failIfFailedInstanceInActiveRecovery bool, failIfClusterInActiveRecovery bool) (*TopologyRecovery, error) {
	return DefaultTenant().AttemptRecoveryRegistration(analysisEntry, failIfFailedInstanceInActiveRecovery, failIfClusterInActiveRecovery)
}

// ClearActiveRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ClearActiveRecoveries on the tenant it applies to.
func ClearActiveRecoveries() error {
	return DefaultTenant().ClearActiveRecoveries()
}

// RegisterBlockedRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).RegisterBlockedRecoveries on the tenant it applies to.
func RegisterBlockedRecoveries(analysisEntry *inst.ReplicationAnalysis, blockingRecoveries []*TopologyRecovery) error {
	return DefaultTenant().RegisterBlockedRecoveries(analysisEntry, blockingRecoveries)
}

// ExpireBlockedRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ExpireBlockedRecoveries on the tenant it applies to.
func ExpireBlockedRecoveries() error {
	return DefaultTenant().ExpireBlockedRecoveries()
}

// AcknowledgeAllRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AcknowledgeAllRecoveries on the tenant it applies to.
func AcknowledgeAllRecoveries(owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return DefaultTenant().AcknowledgeAllRecoveries(owner, comment)
}

// AcknowledgeRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AcknowledgeRecovery on the tenant it applies to.
func AcknowledgeRecovery(recoveryId int64, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return DefaultTenant().AcknowledgeRecovery(recoveryId, owner, comment)
}

// AcknowledgeRecoveryByUID runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AcknowledgeRecoveryByUID on the tenant it applies to.
func AcknowledgeRecoveryByUID(recoveryUID string, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return DefaultTenant().AcknowledgeRecoveryByUID(recoveryUID, owner, comment)
}

// AcknowledgeClusterRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AcknowledgeClusterRecoveries on the tenant it applies to.
func AcknowledgeClusterRecoveries(clusterName string, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return DefaultTenant().AcknowledgeClusterRecoveries(clusterName, owner, comment)
}

// AcknowledgeInstanceRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AcknowledgeInstanceRecoveries on the tenant it applies to.
func AcknowledgeInstanceRecoveries(instanceKey *inst.InstanceKey, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return DefaultTenant().AcknowledgeInstanceRecoveries(instanceKey, owner, comment)
}

// AcknowledgeInstanceCompletedRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AcknowledgeInstanceCompletedRecoveries on the tenant it applies to.
func AcknowledgeInstanceCompletedRecoveries(instanceKey *inst.InstanceKey, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return DefaultTenant().AcknowledgeInstanceCompletedRecoveries(instanceKey, owner, comment)
}

// AcknowledgeCrashedRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).AcknowledgeCrashedRecoveries on the tenant it applies to.
func AcknowledgeCrashedRecoveries() (countAcknowledgedEntries int64, err error) {
	return DefaultTenant().AcknowledgeCrashedRecoveries()
}

// ReadActiveClusterRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadActiveClusterRecovery on the tenant it applies to.
func ReadActiveClusterRecovery(clusterName string) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadActiveClusterRecovery(clusterName)
}

// ReadInActivePeriodClusterRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadInActivePeriodClusterRecovery on the tenant it applies to.
func ReadInActivePeriodClusterRecovery(clusterName string) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadInActivePeriodClusterRecovery(clusterName)
}

// ReadRecentlyActiveClusterRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadRecentlyActiveClusterRecovery on the tenant it applies to.
func ReadRecentlyActiveClusterRecovery(clusterName string) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadRecentlyActiveClusterRecovery(clusterName)
}

// ReadInActivePeriodSuccessorInstanceRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadInActivePeriodSuccessorInstanceRecovery on the tenant it applies to.
func ReadInActivePeriodSuccessorInstanceRecovery(instanceKey *inst.InstanceKey) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadInActivePeriodSuccessorInstanceRecovery(instanceKey)
}

// ReadRecentlyActiveInstanceRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadRecentlyActiveInstanceRecovery on the tenant it applies to.
func ReadRecentlyActiveInstanceRecovery(instanceKey *inst.InstanceKey) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadRecentlyActiveInstanceRecovery(instanceKey)
}

// ReadActiveRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadActiveRecoveries on the tenant it applies to.
func ReadActiveRecoveries() ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadActiveRecoveries()
}

// ReadCompletedRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadCompletedRecoveries on the tenant it applies to.
func ReadCompletedRecoveries(page int) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadCompletedRecoveries(page)
}

// ReadRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadRecovery on the tenant it applies to.
func ReadRecovery(recoveryId int64) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadRecovery(recoveryId)
}

// ReadRecoveryByUID runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadRecoveryByUID on the tenant it applies to.
func ReadRecoveryByUID(recoveryUID string) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadRecoveryByUID(recoveryUID)
}

// ReadRecentRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadRecentRecoveries on the tenant it applies to.
func ReadRecentRecoveries(clusterName string, clusterAlias string, unacknowledgedOnly bool, page int) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadRecentRecoveries(clusterName, clusterAlias, unacknowledgedOnly, page)
}

// ReadRecentFailureDetections runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadRecentFailureDetections on the tenant it applies to.
func ReadRecentFailureDetections(clusterAlias string, page int) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadRecentFailureDetections(clusterAlias, page)
}

// ReadFailureDetection runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadFailureDetection on the tenant it applies to.
func ReadFailureDetection(detectionId int64) ([]*TopologyRecovery, error) {
	return DefaultTenant().ReadFailureDetection(detectionId)
}

// ReadBlockedRecoveries runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadBlockedRecoveries on the tenant it applies to.
func ReadBlockedRecoveries(clusterName string) ([]BlockedTopologyRecovery, error) {
	return DefaultTenant().ReadBlockedRecoveries(clusterName)
}

// ReadTopologyRecoverySteps runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ReadTopologyRecoverySteps on the tenant it applies to.
func ReadTopologyRecoverySteps(recoveryUID string) ([]TopologyRecoveryStep, error) {
	return DefaultTenant().ReadTopologyRecoverySteps(recoveryUID)
}

// ExpireFailureDetectionHistory runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ExpireFailureDetectionHistory on the tenant it applies to.
func ExpireFailureDetectionHistory() error {
	return DefaultTenant().ExpireFailureDetectionHistory()
}

// ExpireTopologyRecoveryHistory runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ExpireTopologyRecoveryHistory on the tenant it applies to.
func ExpireTopologyRecoveryHistory() error {
	return DefaultTenant().ExpireTopologyRecoveryHistory()
}

// ExpireTopologyRecoveryStepsHistory runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).ExpireTopologyRecoveryStepsHistory on the tenant it applies to.
func ExpireTopologyRecoveryStepsHistory() error {
	return DefaultTenant().ExpireTopologyRecoveryStepsHistory()
}
