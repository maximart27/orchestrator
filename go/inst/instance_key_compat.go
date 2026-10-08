// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// NewResolveInstanceKey runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).NewResolveInstanceKey on the tenant's Store.
func NewResolveInstanceKey(hostname string, port int) (instanceKey *InstanceKey, err error) {
	return DefaultStore().NewResolveInstanceKey(hostname, port)
}

// NewResolveInstanceKeyStrings runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).NewResolveInstanceKeyStrings on the tenant's Store.
func NewResolveInstanceKeyStrings(hostname string, port string) (*InstanceKey, error) {
	return DefaultStore().NewResolveInstanceKeyStrings(hostname, port)
}

// ParseResolveInstanceKey runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ParseResolveInstanceKey on the tenant's Store.
func ParseResolveInstanceKey(hostPort string) (instanceKey *InstanceKey, err error) {
	return DefaultStore().ParseResolveInstanceKey(hostPort)
}

// ParseRawInstanceKey runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ParseRawInstanceKey on the tenant's Store.
func ParseRawInstanceKey(hostPort string) (instanceKey *InstanceKey, err error) {
	return DefaultStore().ParseRawInstanceKey(hostPort)
}

// NewRawInstanceKeyStrings runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).NewRawInstanceKeyStrings on the tenant's Store.
func NewRawInstanceKeyStrings(hostname string, port string) (*InstanceKey, error) {
	return DefaultStore().NewRawInstanceKeyStrings(hostname, port)
}

// ResolveHostname runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceKeyResolveHostname on the tenant's Store.
func (this *InstanceKey) ResolveHostname() (*InstanceKey, error) {
	return DefaultStore().InstanceKeyResolveHostname(this)
}
