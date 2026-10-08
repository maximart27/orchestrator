// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// SetClusterAlias runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).SetClusterAlias on the tenant's Store.
func SetClusterAlias(clusterName string, alias string) error {
	return DefaultStore().SetClusterAlias(clusterName, alias)
}

// SetClusterAliasManualOverride runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).SetClusterAliasManualOverride on the tenant's Store.
func SetClusterAliasManualOverride(clusterName string, alias string) error {
	return DefaultStore().SetClusterAliasManualOverride(clusterName, alias)
}

// GetClusterByAlias runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetClusterByAlias on the tenant's Store.
func GetClusterByAlias(alias string) (string, error) {
	return DefaultStore().GetClusterByAlias(alias)
}
