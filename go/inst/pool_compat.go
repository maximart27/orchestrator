// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// ApplyPoolInstances runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ApplyPoolInstances on the tenant's Store.
func ApplyPoolInstances(submission *PoolInstancesSubmission) error {
	return DefaultStore().ApplyPoolInstances(submission)
}
