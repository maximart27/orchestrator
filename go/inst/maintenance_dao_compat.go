// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// ReadActiveMaintenance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadActiveMaintenance on the tenant's Store.
func ReadActiveMaintenance() ([]Maintenance, error) {
	return DefaultStore().ReadActiveMaintenance()
}

// BeginBoundedMaintenance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).BeginBoundedMaintenance on the tenant's Store.
func BeginBoundedMaintenance(instanceKey *InstanceKey, owner string, reason string, durationSeconds uint, explicitlyBounded bool) (int64, error) {
	return DefaultStore().BeginBoundedMaintenance(instanceKey, owner, reason, durationSeconds, explicitlyBounded)
}

// BeginMaintenance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).BeginMaintenance on the tenant's Store.
func BeginMaintenance(instanceKey *InstanceKey, owner string, reason string) (int64, error) {
	return DefaultStore().BeginMaintenance(instanceKey, owner, reason)
}

// EndMaintenanceByInstanceKey runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).EndMaintenanceByInstanceKey on the tenant's Store.
func EndMaintenanceByInstanceKey(instanceKey *InstanceKey) (wasMaintenance bool, err error) {
	return DefaultStore().EndMaintenanceByInstanceKey(instanceKey)
}

// InMaintenance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InMaintenance on the tenant's Store.
func InMaintenance(instanceKey *InstanceKey) (inMaintenance bool, err error) {
	return DefaultStore().InMaintenance(instanceKey)
}

// ReadMaintenanceInstanceKey runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadMaintenanceInstanceKey on the tenant's Store.
func ReadMaintenanceInstanceKey(maintenanceToken int64) (*InstanceKey, error) {
	return DefaultStore().ReadMaintenanceInstanceKey(maintenanceToken)
}

// EndMaintenance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).EndMaintenance on the tenant's Store.
func EndMaintenance(maintenanceToken int64) (wasMaintenance bool, err error) {
	return DefaultStore().EndMaintenance(maintenanceToken)
}

// ExpireMaintenance runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireMaintenance on the tenant's Store.
func ExpireMaintenance() error {
	return DefaultStore().ExpireMaintenance()
}
