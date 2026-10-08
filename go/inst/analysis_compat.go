// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// ReadReplicaHostsFromString runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReplicationAnalysisReadReplicaHostsFromString on the tenant's Store.
func (this *ReplicationAnalysis) ReadReplicaHostsFromString(replicaHostsString string) error {
	return DefaultStore().ReplicationAnalysisReadReplicaHostsFromString(this, replicaHostsString)
}

// ValidSecondsFromSeenToLastAttemptedCheck runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ValidSecondsFromSeenToLastAttemptedCheck on the tenant's Store.
func ValidSecondsFromSeenToLastAttemptedCheck() uint {
	return DefaultStore().ValidSecondsFromSeenToLastAttemptedCheck()
}
