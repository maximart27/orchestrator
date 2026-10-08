// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// GetPostgreSQLReplicationAnalysis runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetPostgreSQLReplicationAnalysis on the tenant's Store.
func GetPostgreSQLReplicationAnalysis(clusterName string, hints *ReplicationAnalysisHints) ([]ReplicationAnalysis, error) {
	return DefaultStore().GetPostgreSQLReplicationAnalysis(clusterName, hints)
}
