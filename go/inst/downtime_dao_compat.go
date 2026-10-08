// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// BeginDowntime runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).BeginDowntime on the tenant's Store.
func BeginDowntime(downtime *Downtime) (err error) {
	return DefaultStore().BeginDowntime(downtime)
}

// EndDowntime runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).EndDowntime on the tenant's Store.
func EndDowntime(instanceKey *InstanceKey) (wasDowntimed bool, err error) {
	return DefaultStore().EndDowntime(instanceKey)
}

// ExpireDowntime runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireDowntime on the tenant's Store.
func ExpireDowntime() error {
	return DefaultStore().ExpireDowntime()
}

// ReadDowntime runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadDowntime on the tenant's Store.
func ReadDowntime() (result []Downtime, err error) {
	return DefaultStore().ReadDowntime()
}
