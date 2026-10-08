// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// WithCurrentTime runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).CandidateDatabaseInstanceWithCurrentTime on the tenant's Store.
func (cdi *CandidateDatabaseInstance) WithCurrentTime() *CandidateDatabaseInstance {
	return DefaultStore().CandidateDatabaseInstanceWithCurrentTime(cdi)
}
