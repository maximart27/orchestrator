// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

import (
	"database/sql"
	"github.com/proxysql/orchestrator/go/kv"
	"github.com/sjmudd/stopwatch"
)

// ExecDBWriteFunc runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExecDBWriteFunc on the tenant's Store.
func ExecDBWriteFunc(f func() error) error {
	return DefaultStore().ExecDBWriteFunc(f)
}

// ExpireTableData runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireTableData on the tenant's Store.
func ExpireTableData(tableName string, timestampColumn string) error {
	return DefaultStore().ExpireTableData(tableName, timestampColumn)
}

// ReadTopologyInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadTopologyInstance on the tenant's Store.
func ReadTopologyInstance(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().ReadTopologyInstance(instanceKey)
}

// ReadTopologyInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadTopologyInstances on the tenant's Store.
func ReadTopologyInstances(instanceKeys []InstanceKey) ([]*Instance, error) {
	return DefaultStore().ReadTopologyInstances(instanceKeys)
}

// ReadTopologyInstanceBufferable runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadTopologyInstanceBufferable on the tenant's Store.
func ReadTopologyInstanceBufferable(instanceKey *InstanceKey, bufferWrites bool, latency *stopwatch.NamedStopwatch) (inst *Instance, skipped bool, err error) {
	return DefaultStore().ReadTopologyInstanceBufferable(instanceKey, bufferWrites, latency)
}

// ReadClusterAliasOverride runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClusterAliasOverride on the tenant's Store.
func ReadClusterAliasOverride(instance *Instance) (err error) {
	return DefaultStore().ReadClusterAliasOverride(instance)
}

// ReadReplicationGroupPrimary runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadReplicationGroupPrimary on the tenant's Store.
func ReadReplicationGroupPrimary(instance *Instance) (err error) {
	return DefaultStore().ReadReplicationGroupPrimary(instance)
}

// ReadInstanceClusterAttributes runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadInstanceClusterAttributes on the tenant's Store.
func ReadInstanceClusterAttributes(instance *Instance) (err error) {
	return DefaultStore().ReadInstanceClusterAttributes(instance)
}

// BulkReadInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).BulkReadInstance on the tenant's Store.
func BulkReadInstance() ([](*InstanceKey), error) {
	return DefaultStore().BulkReadInstance()
}

// ReadInstancePromotionRule runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadInstancePromotionRule on the tenant's Store.
func ReadInstancePromotionRule(instance *Instance) (err error) {
	return DefaultStore().ReadInstancePromotionRule(instance)
}

// ReadInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadInstance on the tenant's Store.
func ReadInstance(instanceKey *InstanceKey) (*Instance, bool, error) {
	return DefaultStore().ReadInstance(instanceKey)
}

// ReadClusterInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClusterInstances on the tenant's Store.
func ReadClusterInstances(clusterName string) ([](*Instance), error) {
	return DefaultStore().ReadClusterInstances(clusterName)
}

// ReadClusterWriteableMaster runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClusterWriteableMaster on the tenant's Store.
func ReadClusterWriteableMaster(clusterName string) ([](*Instance), error) {
	return DefaultStore().ReadClusterWriteableMaster(clusterName)
}

// ReadClusterMaster runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClusterMaster on the tenant's Store.
func ReadClusterMaster(clusterName string) ([](*Instance), error) {
	return DefaultStore().ReadClusterMaster(clusterName)
}

// ReadWriteableClustersMasters runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadWriteableClustersMasters on the tenant's Store.
func ReadWriteableClustersMasters() (instances [](*Instance), err error) {
	return DefaultStore().ReadWriteableClustersMasters()
}

// ReadReplicaInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadReplicaInstances on the tenant's Store.
func ReadReplicaInstances(masterKey *InstanceKey) ([](*Instance), error) {
	return DefaultStore().ReadReplicaInstances(masterKey)
}

// ReadReplicaInstancesIncludingBinlogServerSubReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadReplicaInstancesIncludingBinlogServerSubReplicas on the tenant's Store.
func ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey *InstanceKey) ([](*Instance), error) {
	return DefaultStore().ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey)
}

// ReadBinlogServerReplicaInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadBinlogServerReplicaInstances on the tenant's Store.
func ReadBinlogServerReplicaInstances(masterKey *InstanceKey) ([](*Instance), error) {
	return DefaultStore().ReadBinlogServerReplicaInstances(masterKey)
}

// ReadUnseenInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadUnseenInstances on the tenant's Store.
func ReadUnseenInstances() ([](*Instance), error) {
	return DefaultStore().ReadUnseenInstances()
}

// ReadProblemInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadProblemInstances on the tenant's Store.
func ReadProblemInstances(clusterName string) ([](*Instance), error) {
	return DefaultStore().ReadProblemInstances(clusterName)
}

// SearchInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).SearchInstances on the tenant's Store.
func SearchInstances(searchString string) ([](*Instance), error) {
	return DefaultStore().SearchInstances(searchString)
}

// FindInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).FindInstances on the tenant's Store.
func FindInstances(regexpPattern string) (result [](*Instance), err error) {
	return DefaultStore().FindInstances(regexpPattern)
}

// ReadFuzzyInstanceKey runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadFuzzyInstanceKey on the tenant's Store.
func ReadFuzzyInstanceKey(fuzzyInstanceKey *InstanceKey) *InstanceKey {
	return DefaultStore().ReadFuzzyInstanceKey(fuzzyInstanceKey)
}

// ReadFuzzyInstanceKeyIfPossible runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadFuzzyInstanceKeyIfPossible on the tenant's Store.
func ReadFuzzyInstanceKeyIfPossible(fuzzyInstanceKey *InstanceKey) *InstanceKey {
	return DefaultStore().ReadFuzzyInstanceKeyIfPossible(fuzzyInstanceKey)
}

// ReadFuzzyInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadFuzzyInstance on the tenant's Store.
func ReadFuzzyInstance(fuzzyInstanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().ReadFuzzyInstance(fuzzyInstanceKey)
}

// ReadLostInRecoveryInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadLostInRecoveryInstances on the tenant's Store.
func ReadLostInRecoveryInstances(clusterName string) ([](*Instance), error) {
	return DefaultStore().ReadLostInRecoveryInstances(clusterName)
}

// ReadDowntimedInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadDowntimedInstances on the tenant's Store.
func ReadDowntimedInstances(clusterName string) ([](*Instance), error) {
	return DefaultStore().ReadDowntimedInstances(clusterName)
}

// ReadClusterCandidateInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClusterCandidateInstances on the tenant's Store.
func ReadClusterCandidateInstances(clusterName string) ([](*Instance), error) {
	return DefaultStore().ReadClusterCandidateInstances(clusterName)
}

// ReadClusterNeutralPromotionRuleInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClusterNeutralPromotionRuleInstances on the tenant's Store.
func ReadClusterNeutralPromotionRuleInstances(clusterName string) (neutralInstances [](*Instance), err error) {
	return DefaultStore().ReadClusterNeutralPromotionRuleInstances(clusterName)
}

// GetClusterOSCReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetClusterOSCReplicas on the tenant's Store.
func GetClusterOSCReplicas(clusterName string) ([](*Instance), error) {
	return DefaultStore().GetClusterOSCReplicas(clusterName)
}

// GetClusterGhostReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetClusterGhostReplicas on the tenant's Store.
func GetClusterGhostReplicas(clusterName string) (result [](*Instance), err error) {
	return DefaultStore().GetClusterGhostReplicas(clusterName)
}

// GetClusterHeuristicLag runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetClusterHeuristicLag on the tenant's Store.
func GetClusterHeuristicLag(clusterName string) (int64, error) {
	return DefaultStore().GetClusterHeuristicLag(clusterName)
}

// GetHeuristicClusterPoolInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetHeuristicClusterPoolInstances on the tenant's Store.
func GetHeuristicClusterPoolInstances(clusterName string, pool string) (result [](*Instance), err error) {
	return DefaultStore().GetHeuristicClusterPoolInstances(clusterName, pool)
}

// GetHeuristicClusterPoolInstancesLag runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetHeuristicClusterPoolInstancesLag on the tenant's Store.
func GetHeuristicClusterPoolInstancesLag(clusterName string, pool string) (int64, error) {
	return DefaultStore().GetHeuristicClusterPoolInstancesLag(clusterName, pool)
}

// ReplaceClusterName runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReplaceClusterName on the tenant's Store.
func ReplaceClusterName(oldClusterName string, newClusterName string) error {
	return DefaultStore().ReplaceClusterName(oldClusterName, newClusterName)
}

// ReviewUnseenInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReviewUnseenInstances on the tenant's Store.
func ReviewUnseenInstances() error {
	return DefaultStore().ReviewUnseenInstances()
}

// InjectSeed runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InjectSeed on the tenant's Store.
func InjectSeed(instanceKey *InstanceKey) error {
	return DefaultStore().InjectSeed(instanceKey)
}

// InjectUnseenMasters runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InjectUnseenMasters on the tenant's Store.
func InjectUnseenMasters() error {
	return DefaultStore().InjectUnseenMasters()
}

// ForgetUnseenInstancesDifferentlyResolved runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ForgetUnseenInstancesDifferentlyResolved on the tenant's Store.
func ForgetUnseenInstancesDifferentlyResolved() error {
	return DefaultStore().ForgetUnseenInstancesDifferentlyResolved()
}

// ResolveUnknownMasterHostnameResolves runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ResolveUnknownMasterHostnameResolves on the tenant's Store.
func ResolveUnknownMasterHostnameResolves() error {
	return DefaultStore().ResolveUnknownMasterHostnameResolves()
}

// ReadCountMySQLSnapshots runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadCountMySQLSnapshots on the tenant's Store.
func ReadCountMySQLSnapshots(hostnames []string) (map[string]int, error) {
	return DefaultStore().ReadCountMySQLSnapshots(hostnames)
}

// PopulateInstancesAgents runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PopulateInstancesAgents on the tenant's Store.
func PopulateInstancesAgents(instances [](*Instance)) error {
	return DefaultStore().PopulateInstancesAgents(instances)
}

// GetClusterName runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetClusterName on the tenant's Store.
func GetClusterName(instanceKey *InstanceKey) (clusterName string, err error) {
	return DefaultStore().GetClusterName(instanceKey)
}

// ReadClusters runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClusters on the tenant's Store.
func ReadClusters() (clusterNames []string, err error) {
	return DefaultStore().ReadClusters()
}

// ReadClusterInfo runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClusterInfo on the tenant's Store.
func ReadClusterInfo(clusterName string) (*ClusterInfo, error) {
	return DefaultStore().ReadClusterInfo(clusterName)
}

// ReadClustersInfo runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClustersInfo on the tenant's Store.
func ReadClustersInfo(clusterName string) ([]ClusterInfo, error) {
	return DefaultStore().ReadClustersInfo(clusterName)
}

// GetMastersKVPairs runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetMastersKVPairs on the tenant's Store.
func GetMastersKVPairs(clusterName string) (kvPairs [](*kv.KVPair), err error) {
	return DefaultStore().GetMastersKVPairs(clusterName)
}

// HeuristicallyApplyClusterDomainInstanceAttribute runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).HeuristicallyApplyClusterDomainInstanceAttribute on the tenant's Store.
func HeuristicallyApplyClusterDomainInstanceAttribute(clusterName string) (instanceKey *InstanceKey, err error) {
	return DefaultStore().HeuristicallyApplyClusterDomainInstanceAttribute(clusterName)
}

// GetHeuristicClusterDomainInstanceAttribute runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetHeuristicClusterDomainInstanceAttribute on the tenant's Store.
func GetHeuristicClusterDomainInstanceAttribute(clusterName string) (instanceKey *InstanceKey, err error) {
	return DefaultStore().GetHeuristicClusterDomainInstanceAttribute(clusterName)
}

// ReadAllInstanceKeys runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadAllInstanceKeys on the tenant's Store.
func ReadAllInstanceKeys() ([]InstanceKey, error) {
	return DefaultStore().ReadAllInstanceKeys()
}

// ReadRecentlySeenInstanceKeyMap runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadRecentlySeenInstanceKeyMap on the tenant's Store.
func ReadRecentlySeenInstanceKeyMap(recencyHours uint) (*InstanceKeyMap, error) {
	return DefaultStore().ReadRecentlySeenInstanceKeyMap(recencyHours)
}

// ReadAllMinimalInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadAllMinimalInstances on the tenant's Store.
func ReadAllMinimalInstances() ([]MinimalInstance, error) {
	return DefaultStore().ReadAllMinimalInstances()
}

// ReadOutdatedInstanceKeys runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadOutdatedInstanceKeys on the tenant's Store.
func ReadOutdatedInstanceKeys() ([]InstanceKey, error) {
	return DefaultStore().ReadOutdatedInstanceKeys()
}

// WriteInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).WriteInstance on the tenant's Store.
func WriteInstance(instance *Instance, instanceWasActuallyFound bool, lastError error) error {
	return DefaultStore().WriteInstance(instance, instanceWasActuallyFound, lastError)
}

// UpdateInstanceLastChecked runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).UpdateInstanceLastChecked on the tenant's Store.
func UpdateInstanceLastChecked(instanceKey *InstanceKey, partialSuccess bool) error {
	return DefaultStore().UpdateInstanceLastChecked(instanceKey, partialSuccess)
}

// UpdateInstanceLastAttemptedCheck runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).UpdateInstanceLastAttemptedCheck on the tenant's Store.
func UpdateInstanceLastAttemptedCheck(instanceKey *InstanceKey) error {
	return DefaultStore().UpdateInstanceLastAttemptedCheck(instanceKey)
}

// InstanceIsForgotten runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceIsForgotten on the tenant's Store.
func InstanceIsForgotten(instanceKey *InstanceKey) bool {
	return DefaultStore().InstanceIsForgotten(instanceKey)
}

// ForgetInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ForgetInstance on the tenant's Store.
func ForgetInstance(instanceKey *InstanceKey) error {
	return DefaultStore().ForgetInstance(instanceKey)
}

// ForgetCluster runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ForgetCluster on the tenant's Store.
func ForgetCluster(clusterName string) error {
	return DefaultStore().ForgetCluster(clusterName)
}

// ForgetLongUnseenInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ForgetLongUnseenInstances on the tenant's Store.
func ForgetLongUnseenInstances() error {
	return DefaultStore().ForgetLongUnseenInstances()
}

// SnapshotTopologies runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).SnapshotTopologies on the tenant's Store.
func SnapshotTopologies() error {
	return DefaultStore().SnapshotTopologies()
}

// ReadHistoryClusterInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadHistoryClusterInstances on the tenant's Store.
func ReadHistoryClusterInstances(clusterName string, historyTimestampPattern string) ([](*Instance), error) {
	return DefaultStore().ReadHistoryClusterInstances(clusterName, historyTimestampPattern)
}

// RecordInstanceCoordinatesHistory runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RecordInstanceCoordinatesHistory on the tenant's Store.
func RecordInstanceCoordinatesHistory() error {
	return DefaultStore().RecordInstanceCoordinatesHistory()
}

// GetHeuristiclyRecentCoordinatesForInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetHeuristiclyRecentCoordinatesForInstance on the tenant's Store.
func GetHeuristiclyRecentCoordinatesForInstance(instanceKey *InstanceKey) (selfCoordinates *BinlogCoordinates, relayLogCoordinates *BinlogCoordinates, err error) {
	return DefaultStore().GetHeuristiclyRecentCoordinatesForInstance(instanceKey)
}

// RecordStaleInstanceBinlogCoordinates runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RecordStaleInstanceBinlogCoordinates on the tenant's Store.
func RecordStaleInstanceBinlogCoordinates(instanceKey *InstanceKey, binlogCoordinates *BinlogCoordinates) error {
	return DefaultStore().RecordStaleInstanceBinlogCoordinates(instanceKey, binlogCoordinates)
}

// ExpireStaleInstanceBinlogCoordinates runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireStaleInstanceBinlogCoordinates on the tenant's Store.
func ExpireStaleInstanceBinlogCoordinates() error {
	return DefaultStore().ExpireStaleInstanceBinlogCoordinates()
}

// GetPreviousKnownRelayLogCoordinatesForInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetPreviousKnownRelayLogCoordinatesForInstance on the tenant's Store.
func GetPreviousKnownRelayLogCoordinatesForInstance(instance *Instance) (relayLogCoordinates *BinlogCoordinates, err error) {
	return DefaultStore().GetPreviousKnownRelayLogCoordinatesForInstance(instance)
}

// ResetInstanceRelaylogCoordinatesHistory runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ResetInstanceRelaylogCoordinatesHistory on the tenant's Store.
func ResetInstanceRelaylogCoordinatesHistory(instanceKey *InstanceKey) error {
	return DefaultStore().ResetInstanceRelaylogCoordinatesHistory(instanceKey)
}

// FigureClusterName runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).FigureClusterName on the tenant's Store.
func FigureClusterName(clusterHint string, instanceKey *InstanceKey, thisInstanceKey *InstanceKey) (clusterName string, err error) {
	return DefaultStore().FigureClusterName(clusterHint, instanceKey, thisInstanceKey)
}

// FigureInstanceKey runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).FigureInstanceKey on the tenant's Store.
func FigureInstanceKey(instanceKey *InstanceKey, thisInstanceKey *InstanceKey) (*InstanceKey, error) {
	return DefaultStore().FigureInstanceKey(instanceKey, thisInstanceKey)
}

// PopulateGroupReplicationInformation runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PopulateGroupReplicationInformation on the tenant's Store.
func PopulateGroupReplicationInformation(instance *Instance, db *sql.DB) error {
	return DefaultStore().PopulateGroupReplicationInformation(instance, db)
}

// RegisterInjectedPseudoGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RegisterInjectedPseudoGTID on the tenant's Store.
func RegisterInjectedPseudoGTID(clusterName string) error {
	return DefaultStore().RegisterInjectedPseudoGTID(clusterName)
}

// ExpireInjectedPseudoGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireInjectedPseudoGTID on the tenant's Store.
func ExpireInjectedPseudoGTID() error {
	return DefaultStore().ExpireInjectedPseudoGTID()
}
