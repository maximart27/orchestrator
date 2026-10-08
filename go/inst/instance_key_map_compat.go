// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// ReadCommaDelimitedList runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceKeyMapReadCommaDelimitedList on the tenant's Store.
func (this *InstanceKeyMap) ReadCommaDelimitedList(list string) error {
	return DefaultStore().InstanceKeyMapReadCommaDelimitedList(this, list)
}
