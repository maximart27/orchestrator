// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// WriteMasterPositionEquivalence runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).WriteMasterPositionEquivalence on the tenant's Store.
func WriteMasterPositionEquivalence(master1Key *InstanceKey, master1BinlogCoordinates *BinlogCoordinates, master2Key *InstanceKey, master2BinlogCoordinates *BinlogCoordinates) error {
	return DefaultStore().WriteMasterPositionEquivalence(master1Key, master1BinlogCoordinates, master2Key, master2BinlogCoordinates)
}

// GetEquivalentMasterCoordinates runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetEquivalentMasterCoordinates on the tenant's Store.
func GetEquivalentMasterCoordinates(instanceCoordinates *InstanceBinlogCoordinates) (result [](*InstanceBinlogCoordinates), err error) {
	return DefaultStore().GetEquivalentMasterCoordinates(instanceCoordinates)
}

// GetEquivalentBinlogCoordinatesFor runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).GetEquivalentBinlogCoordinatesFor on the tenant's Store.
func GetEquivalentBinlogCoordinatesFor(instanceCoordinates *InstanceBinlogCoordinates, belowKey *InstanceKey) (*BinlogCoordinates, error) {
	return DefaultStore().GetEquivalentBinlogCoordinatesFor(instanceCoordinates, belowKey)
}

// ExpireMasterPositionEquivalence runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireMasterPositionEquivalence on the tenant's Store.
func ExpireMasterPositionEquivalence() error {
	return DefaultStore().ExpireMasterPositionEquivalence()
}
