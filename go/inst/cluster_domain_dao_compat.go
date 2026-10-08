// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// WriteClusterDomainName runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).WriteClusterDomainName on the tenant's Store.
func WriteClusterDomainName(clusterName string, domainName string) error {
	return DefaultStore().WriteClusterDomainName(clusterName, domainName)
}

// ExpireClusterDomainName runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireClusterDomainName on the tenant's Store.
func ExpireClusterDomainName() error {
	return DefaultStore().ExpireClusterDomainName()
}
