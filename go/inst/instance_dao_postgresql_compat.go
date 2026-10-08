// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// ReadPostgreSQLTopologyInstance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadPostgreSQLTopologyInstance on the tenant's Store.
func ReadPostgreSQLTopologyInstance(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().ReadPostgreSQLTopologyInstance(instanceKey)
}
