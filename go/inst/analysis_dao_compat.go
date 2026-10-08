// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// GetReplicationAnalysis runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetReplicationAnalysis on the tenant's Store.
func GetReplicationAnalysis(clusterName string, hints *ReplicationAnalysisHints) ([]ReplicationAnalysis, error) {
	return DefaultStore().GetReplicationAnalysis(clusterName, hints)
}

// ExpireInstanceAnalysisChangelog runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireInstanceAnalysisChangelog on the tenant's Store.
func ExpireInstanceAnalysisChangelog() error {
	return DefaultStore().ExpireInstanceAnalysisChangelog()
}

// ReadReplicationAnalysisChangelog runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadReplicationAnalysisChangelog on the tenant's Store.
func ReadReplicationAnalysisChangelog() (res [](*ReplicationAnalysisChangelog), err error) {
	return DefaultStore().ReadReplicationAnalysisChangelog()
}

// ReadPeerAnalysisMap runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadPeerAnalysisMap on the tenant's Store.
func ReadPeerAnalysisMap() (peerAnalysisMap PeerAnalysisMap, err error) {
	return DefaultStore().ReadPeerAnalysisMap()
}
