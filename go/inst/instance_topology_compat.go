// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// ASCIITopology runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ASCIITopology on the tenant's Store.
func ASCIITopology(clusterName string, historyTimestampPattern string, tabulated bool, printTags bool) (result string, err error) {
	return DefaultStore().ASCIITopology(clusterName, historyTimestampPattern, tabulated, printTags)
}

// GetInstanceMaster runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetInstanceMaster on the tenant's Store.
func GetInstanceMaster(instance *Instance) (*Instance, error) {
	return DefaultStore().GetInstanceMaster(instance)
}

// MoveEquivalent runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MoveEquivalent on the tenant's Store.
func MoveEquivalent(instanceKey *InstanceKey, otherKey *InstanceKey) (*Instance, error) {
	return DefaultStore().MoveEquivalent(instanceKey, otherKey)
}

// MoveUp runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MoveUp on the tenant's Store.
func MoveUp(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().MoveUp(instanceKey)
}

// MoveUpReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MoveUpReplicas on the tenant's Store.
func MoveUpReplicas(instanceKey *InstanceKey, pattern string) ([](*Instance), *Instance, []error, error) {
	return DefaultStore().MoveUpReplicas(instanceKey, pattern)
}

// MoveBelow runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MoveBelow on the tenant's Store.
func MoveBelow(instanceKey *InstanceKey, siblingKey *InstanceKey) (*Instance, error) {
	return DefaultStore().MoveBelow(instanceKey, siblingKey)
}

// CheckMoveViaGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).CheckMoveViaGTID on the tenant's Store.
func CheckMoveViaGTID(instance *Instance, otherInstance *Instance) (err error) {
	return DefaultStore().CheckMoveViaGTID(instance, otherInstance)
}

// MoveBelowGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MoveBelowGTID on the tenant's Store.
func MoveBelowGTID(instanceKey *InstanceKey, otherKey *InstanceKey) (*Instance, error) {
	return DefaultStore().MoveBelowGTID(instanceKey, otherKey)
}

// MoveReplicasGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MoveReplicasGTID on the tenant's Store.
func MoveReplicasGTID(masterKey *InstanceKey, belowKey *InstanceKey, pattern string) (movedReplicas [](*Instance), unmovedReplicas [](*Instance), errs []error, err error) {
	return DefaultStore().MoveReplicasGTID(masterKey, belowKey, pattern)
}

// Repoint runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).Repoint on the tenant's Store.
func Repoint(instanceKey *InstanceKey, masterKey *InstanceKey, gtidHint OperationGTIDHint) (*Instance, error) {
	return DefaultStore().Repoint(instanceKey, masterKey, gtidHint)
}

// RepointTo runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RepointTo on the tenant's Store.
func RepointTo(replicas [](*Instance), belowKey *InstanceKey) ([](*Instance), []error, error) {
	return DefaultStore().RepointTo(replicas, belowKey)
}

// RepointReplicasTo runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RepointReplicasTo on the tenant's Store.
func RepointReplicasTo(instanceKey *InstanceKey, pattern string, belowKey *InstanceKey) ([](*Instance), []error, error) {
	return DefaultStore().RepointReplicasTo(instanceKey, pattern, belowKey)
}

// RepointReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RepointReplicas on the tenant's Store.
func RepointReplicas(instanceKey *InstanceKey, pattern string) ([](*Instance), []error, error) {
	return DefaultStore().RepointReplicas(instanceKey, pattern)
}

// MakeCoMaster runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MakeCoMaster on the tenant's Store.
func MakeCoMaster(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().MakeCoMaster(instanceKey)
}

// ResetReplicationOperation runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ResetReplicationOperation on the tenant's Store.
func ResetReplicationOperation(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().ResetReplicationOperation(instanceKey)
}

// DetachReplicaMasterHost runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).DetachReplicaMasterHost on the tenant's Store.
func DetachReplicaMasterHost(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().DetachReplicaMasterHost(instanceKey)
}

// ReattachReplicaMasterHost runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReattachReplicaMasterHost on the tenant's Store.
func ReattachReplicaMasterHost(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().ReattachReplicaMasterHost(instanceKey)
}

// EnableGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).EnableGTID on the tenant's Store.
func EnableGTID(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().EnableGTID(instanceKey)
}

// DisableGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).DisableGTID on the tenant's Store.
func DisableGTID(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().DisableGTID(instanceKey)
}

// LocateErrantGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).LocateErrantGTID on the tenant's Store.
func LocateErrantGTID(instanceKey *InstanceKey) (errantBinlogs []string, err error) {
	return DefaultStore().LocateErrantGTID(instanceKey)
}

// ErrantGTIDResetMaster runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ErrantGTIDResetMaster on the tenant's Store.
func ErrantGTIDResetMaster(instanceKey *InstanceKey) (instance *Instance, err error) {
	return DefaultStore().ErrantGTIDResetMaster(instanceKey)
}

// ErrantGTIDInjectEmpty runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ErrantGTIDInjectEmpty on the tenant's Store.
func ErrantGTIDInjectEmpty(instanceKey *InstanceKey) (instance *Instance, clusterMaster *Instance, countInjectedTransactions int64, err error) {
	return DefaultStore().ErrantGTIDInjectEmpty(instanceKey)
}

// FindLastPseudoGTIDEntry runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).FindLastPseudoGTIDEntry on the tenant's Store.
func FindLastPseudoGTIDEntry(instance *Instance, recordedInstanceRelayLogCoordinates BinlogCoordinates, maxBinlogCoordinates *BinlogCoordinates, exhaustiveSearch bool, expectedBinlogFormat *string) (instancePseudoGtidCoordinates *BinlogCoordinates, instancePseudoGtidText string, err error) {
	return DefaultStore().FindLastPseudoGTIDEntry(instance, recordedInstanceRelayLogCoordinates, maxBinlogCoordinates, exhaustiveSearch, expectedBinlogFormat)
}

// CorrelateBinlogCoordinates runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).CorrelateBinlogCoordinates on the tenant's Store.
func CorrelateBinlogCoordinates(instance *Instance, binlogCoordinates *BinlogCoordinates, otherInstance *Instance) (*BinlogCoordinates, int, error) {
	return DefaultStore().CorrelateBinlogCoordinates(instance, binlogCoordinates, otherInstance)
}

// CorrelateRelaylogCoordinates runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).CorrelateRelaylogCoordinates on the tenant's Store.
func CorrelateRelaylogCoordinates(instance *Instance, relaylogCoordinates *BinlogCoordinates, otherInstance *Instance) (instanceCoordinates, correlatedCoordinates, nextCoordinates *BinlogCoordinates, found bool, err error) {
	return DefaultStore().CorrelateRelaylogCoordinates(instance, relaylogCoordinates, otherInstance)
}

// MatchBelow runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MatchBelow on the tenant's Store.
func MatchBelow(instanceKey *InstanceKey, otherKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	return DefaultStore().MatchBelow(instanceKey, otherKey, requireInstanceMaintenance)
}

// RematchReplica runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RematchReplica on the tenant's Store.
func RematchReplica(instanceKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	return DefaultStore().RematchReplica(instanceKey, requireInstanceMaintenance)
}

// MakeMaster runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MakeMaster on the tenant's Store.
func MakeMaster(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().MakeMaster(instanceKey)
}

// TakeSiblings runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).TakeSiblings on the tenant's Store.
func TakeSiblings(instanceKey *InstanceKey) (instance *Instance, takenSiblings int, err error) {
	return DefaultStore().TakeSiblings(instanceKey)
}

// TakeMasterHook runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).TakeMasterHook on the tenant's Store.
func TakeMasterHook(successor *Instance, demoted *Instance) {
	DefaultStore().TakeMasterHook(successor, demoted)
}

// TakeMaster runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).TakeMaster on the tenant's Store.
func TakeMaster(instanceKey *InstanceKey, allowTakingCoMaster bool) (*Instance, error) {
	return DefaultStore().TakeMaster(instanceKey, allowTakingCoMaster)
}

// MakeLocalMaster runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MakeLocalMaster on the tenant's Store.
func MakeLocalMaster(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().MakeLocalMaster(instanceKey)
}

// GetSortedReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetSortedReplicas on the tenant's Store.
func GetSortedReplicas(masterKey *InstanceKey, stopReplicationMethod StopReplicationMethod) (replicas [](*Instance), err error) {
	return DefaultStore().GetSortedReplicas(masterKey, stopReplicationMethod)
}

// MultiMatchBelow runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MultiMatchBelow on the tenant's Store.
func MultiMatchBelow(replicas [](*Instance), belowKey *InstanceKey, postponedFunctionsContainer *PostponedFunctionsContainer) (matchedReplicas [](*Instance), belowInstance *Instance, errs []error, err error) {
	return DefaultStore().MultiMatchBelow(replicas, belowKey, postponedFunctionsContainer)
}

// MultiMatchReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MultiMatchReplicas on the tenant's Store.
func MultiMatchReplicas(masterKey *InstanceKey, belowKey *InstanceKey, pattern string) ([](*Instance), *Instance, []error, error) {
	return DefaultStore().MultiMatchReplicas(masterKey, belowKey, pattern)
}

// MatchUp runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MatchUp on the tenant's Store.
func MatchUp(instanceKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	return DefaultStore().MatchUp(instanceKey, requireInstanceMaintenance)
}

// MatchUpReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).MatchUpReplicas on the tenant's Store.
func MatchUpReplicas(masterKey *InstanceKey, pattern string) ([](*Instance), *Instance, []error, error) {
	return DefaultStore().MatchUpReplicas(masterKey, pattern)
}

// IsBannedFromBeingCandidateReplica runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).IsBannedFromBeingCandidateReplica on the tenant's Store.
func IsBannedFromBeingCandidateReplica(replica *Instance) bool {
	return DefaultStore().IsBannedFromBeingCandidateReplica(replica)
}

// GetCandidateReplica runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetCandidateReplica on the tenant's Store.
func GetCandidateReplica(masterKey *InstanceKey, forRematchPurposes bool) (*Instance, [](*Instance), [](*Instance), [](*Instance), [](*Instance), error) {
	return DefaultStore().GetCandidateReplica(masterKey, forRematchPurposes)
}

// GetCandidateReplicaOfBinlogServerTopology runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetCandidateReplicaOfBinlogServerTopology on the tenant's Store.
func GetCandidateReplicaOfBinlogServerTopology(masterKey *InstanceKey) (candidateReplica *Instance, err error) {
	return DefaultStore().GetCandidateReplicaOfBinlogServerTopology(masterKey)
}

// RegroupReplicasPseudoGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RegroupReplicasPseudoGTID on the tenant's Store.
func RegroupReplicasPseudoGTID(masterKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool, onCandidateReplicaChosen func(*Instance), postponedFunctionsContainer *PostponedFunctionsContainer, postponeAllMatchOperations func(*Instance, bool) bool) (aheadReplicas [](*Instance), equalReplicas [](*Instance), laterReplicas [](*Instance), cannotReplicateReplicas [](*Instance), candidateReplica *Instance, err error) {
	return DefaultStore().RegroupReplicasPseudoGTID(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer, postponeAllMatchOperations)
}

// RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers on the tenant's Store.
func RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(masterKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool, onCandidateReplicaChosen func(*Instance), postponedFunctionsContainer *PostponedFunctionsContainer, postponeAllMatchOperations func(*Instance, bool) bool) (aheadReplicas [](*Instance), equalReplicas [](*Instance), laterReplicas [](*Instance), cannotReplicateReplicas [](*Instance), candidateReplica *Instance, err error) {
	return DefaultStore().RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer, postponeAllMatchOperations)
}

// RegroupReplicasGTID runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RegroupReplicasGTID on the tenant's Store.
func RegroupReplicasGTID(masterKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool, startReplicationOnCandidate bool, onCandidateReplicaChosen func(*Instance), postponedFunctionsContainer *PostponedFunctionsContainer, postponeAllMatchOperations func(*Instance, bool) bool) (lostReplicas [](*Instance), movedReplicas [](*Instance), cannotReplicateReplicas [](*Instance), candidateReplica *Instance, err error) {
	return DefaultStore().RegroupReplicasGTID(masterKey, returnReplicaEvenOnFailureToRegroup, startReplicationOnCandidate, onCandidateReplicaChosen, postponedFunctionsContainer, postponeAllMatchOperations)
}

// RegroupReplicasGTIDWithValidation runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RegroupReplicasGTIDWithValidation on the tenant's Store.
func RegroupReplicasGTIDWithValidation(masterKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool, startReplicationOnCandidate bool, onCandidateReplicaChosen func(*Instance), postponedFunctionsContainer *PostponedFunctionsContainer, postponeAllMatchOperations func(*Instance, bool) bool, validateCandidate func(*Instance) error) (lostReplicas [](*Instance), movedReplicas [](*Instance), cannotReplicateReplicas [](*Instance), candidateReplica *Instance, err error) {
	return DefaultStore().RegroupReplicasGTIDWithValidation(masterKey, returnReplicaEvenOnFailureToRegroup, startReplicationOnCandidate, onCandidateReplicaChosen, postponedFunctionsContainer, postponeAllMatchOperations, validateCandidate)
}

// RegroupReplicasBinlogServers runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RegroupReplicasBinlogServers on the tenant's Store.
func RegroupReplicasBinlogServers(masterKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool) (repointedBinlogServers [](*Instance), promotedBinlogServer *Instance, err error) {
	return DefaultStore().RegroupReplicasBinlogServers(masterKey, returnReplicaEvenOnFailureToRegroup)
}

// RegroupReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RegroupReplicas on the tenant's Store.
func RegroupReplicas(masterKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool, onCandidateReplicaChosen func(*Instance), postponedFunctionsContainer *PostponedFunctionsContainer) (aheadReplicas [](*Instance), equalReplicas [](*Instance), laterReplicas [](*Instance), cannotReplicateReplicas [](*Instance), instance *Instance, err error) {
	return DefaultStore().RegroupReplicas(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer)
}

// RelocateBelow runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RelocateBelow on the tenant's Store.
func RelocateBelow(instanceKey *InstanceKey, otherKey *InstanceKey) (*Instance, error) {
	return DefaultStore().RelocateBelow(instanceKey, otherKey)
}

// RelocateReplicas runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RelocateReplicas on the tenant's Store.
func RelocateReplicas(instanceKey *InstanceKey, otherKey *InstanceKey, pattern string) (replicas [](*Instance), other *Instance, errs []error, err error) {
	return DefaultStore().RelocateReplicas(instanceKey, otherKey, pattern)
}

// PurgeBinaryLogsTo runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PurgeBinaryLogsTo on the tenant's Store.
func PurgeBinaryLogsTo(instanceKey *InstanceKey, logFile string, force bool) (*Instance, error) {
	return DefaultStore().PurgeBinaryLogsTo(instanceKey, logFile, force)
}

// PurgeBinaryLogsToLatest runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PurgeBinaryLogsToLatest on the tenant's Store.
func PurgeBinaryLogsToLatest(instanceKey *InstanceKey, force bool) (*Instance, error) {
	return DefaultStore().PurgeBinaryLogsToLatest(instanceKey, force)
}
