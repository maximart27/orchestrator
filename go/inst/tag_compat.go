// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// GetInstanceKeysByTags runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetInstanceKeysByTags on the tenant's Store.
func GetInstanceKeysByTags(tagsString string) (tagged *InstanceKeyMap, err error) {
	return DefaultStore().GetInstanceKeysByTags(tagsString)
}
