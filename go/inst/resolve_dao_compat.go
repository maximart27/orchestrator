// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// WriteResolvedHostname runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).WriteResolvedHostname on the tenant's Store.
func WriteResolvedHostname(hostname string, resolvedHostname string) error {
	return DefaultStore().WriteResolvedHostname(hostname, resolvedHostname)
}

// ReadResolvedHostname runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadResolvedHostname on the tenant's Store.
func ReadResolvedHostname(hostname string) (string, error) {
	return DefaultStore().ReadResolvedHostname(hostname)
}

// ReadAllHostnameResolves runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadAllHostnameResolves on the tenant's Store.
func ReadAllHostnameResolves() ([]HostnameResolve, error) {
	return DefaultStore().ReadAllHostnameResolves()
}

// ReadAllHostnameUnresolves runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadAllHostnameUnresolves on the tenant's Store.
func ReadAllHostnameUnresolves() ([]HostnameUnresolve, error) {
	return DefaultStore().ReadAllHostnameUnresolves()
}

// ReadAllHostnameUnresolvesRegistrations runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadAllHostnameUnresolvesRegistrations on the tenant's Store.
func ReadAllHostnameUnresolvesRegistrations() (registrations []HostnameRegistration, err error) {
	return DefaultStore().ReadAllHostnameUnresolvesRegistrations()
}

// WriteHostnameUnresolve runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).WriteHostnameUnresolve on the tenant's Store.
func WriteHostnameUnresolve(instanceKey *InstanceKey, unresolvedHostname string) error {
	return DefaultStore().WriteHostnameUnresolve(instanceKey, unresolvedHostname)
}

// DeleteHostnameUnresolve runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).DeleteHostnameUnresolve on the tenant's Store.
func DeleteHostnameUnresolve(instanceKey *InstanceKey) error {
	return DefaultStore().DeleteHostnameUnresolve(instanceKey)
}

// ExpireHostnameUnresolve runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireHostnameUnresolve on the tenant's Store.
func ExpireHostnameUnresolve() error {
	return DefaultStore().ExpireHostnameUnresolve()
}

// ForgetExpiredHostnameResolves runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ForgetExpiredHostnameResolves on the tenant's Store.
func ForgetExpiredHostnameResolves() error {
	return DefaultStore().ForgetExpiredHostnameResolves()
}

// DeleteInvalidHostnameResolves runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).DeleteInvalidHostnameResolves on the tenant's Store.
func DeleteInvalidHostnameResolves() error {
	return DefaultStore().DeleteInvalidHostnameResolves()
}
