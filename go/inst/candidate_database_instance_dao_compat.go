// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// RegisterCandidateInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).RegisterCandidateInstance on the tenant's Store.
func RegisterCandidateInstance(candidate *CandidateDatabaseInstance) error {
	return DefaultStore().RegisterCandidateInstance(candidate)
}

// ExpireCandidateInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireCandidateInstances on the tenant's Store.
func ExpireCandidateInstances() error {
	return DefaultStore().ExpireCandidateInstances()
}

// BulkReadCandidateDatabaseInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).BulkReadCandidateDatabaseInstance on the tenant's Store.
func BulkReadCandidateDatabaseInstance() ([]CandidateDatabaseInstance, error) {
	return DefaultStore().BulkReadCandidateDatabaseInstance()
}
