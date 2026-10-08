// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// PutInstanceTag runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PutInstanceTag on the tenant's Store.
func PutInstanceTag(instanceKey *InstanceKey, tag *Tag) (err error) {
	return DefaultStore().PutInstanceTag(instanceKey, tag)
}

// Untag runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).Untag on the tenant's Store.
func Untag(instanceKey *InstanceKey, tag *Tag) (tagged *InstanceKeyMap, err error) {
	return DefaultStore().Untag(instanceKey, tag)
}

// ReadInstanceTag runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadInstanceTag on the tenant's Store.
func ReadInstanceTag(instanceKey *InstanceKey, tag *Tag) (tagExists bool, err error) {
	return DefaultStore().ReadInstanceTag(instanceKey, tag)
}

// InstanceTagExists runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceTagExists on the tenant's Store.
func InstanceTagExists(instanceKey *InstanceKey, tag *Tag) (tagExists bool, err error) {
	return DefaultStore().InstanceTagExists(instanceKey, tag)
}

// ReadInstanceTags runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadInstanceTags on the tenant's Store.
func ReadInstanceTags(instanceKey *InstanceKey) (tags [](*Tag), err error) {
	return DefaultStore().ReadInstanceTags(instanceKey)
}

// GetInstanceKeysByTag runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetInstanceKeysByTag on the tenant's Store.
func GetInstanceKeysByTag(tag *Tag) (tagged *InstanceKeyMap, err error) {
	return DefaultStore().GetInstanceKeysByTag(tag)
}
