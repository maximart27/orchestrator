// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// IsSQLite runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).IsSQLite on the tenant's Store.
func IsSQLite() bool {
	return DefaultStore().IsSQLite()
}

// ReadClusterNameByAlias runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadClusterNameByAlias on the tenant's Store.
func ReadClusterNameByAlias(alias string) (clusterName string, err error) {
	return DefaultStore().ReadClusterNameByAlias(alias)
}

// DeduceClusterName runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).DeduceClusterName on the tenant's Store.
func DeduceClusterName(nameOrAlias string) (clusterName string, err error) {
	return DefaultStore().DeduceClusterName(nameOrAlias)
}

// ReadAliasByClusterName runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadAliasByClusterName on the tenant's Store.
func ReadAliasByClusterName(clusterName string) (alias string, err error) {
	return DefaultStore().ReadAliasByClusterName(clusterName)
}

// UpdateClusterAliases runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).UpdateClusterAliases on the tenant's Store.
func UpdateClusterAliases() error {
	return DefaultStore().UpdateClusterAliases()
}

// ForgetLongUnseenClusterAliases runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ForgetLongUnseenClusterAliases on the tenant's Store.
func ForgetLongUnseenClusterAliases() error {
	return DefaultStore().ForgetLongUnseenClusterAliases()
}

// ReplaceAliasClusterName runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReplaceAliasClusterName on the tenant's Store.
func ReplaceAliasClusterName(oldClusterName string, newClusterName string) (err error) {
	return DefaultStore().ReplaceAliasClusterName(oldClusterName, newClusterName)
}

// ReadUnambiguousSuggestedClusterAliases runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadUnambiguousSuggestedClusterAliases on the tenant's Store.
func ReadUnambiguousSuggestedClusterAliases() (result map[string]InstanceKey, err error) {
	return DefaultStore().ReadUnambiguousSuggestedClusterAliases()
}
