// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

import "time"

// PostgreSQLPromoteStandby runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PostgreSQLPromoteStandby on the tenant's Store.
func PostgreSQLPromoteStandby(instanceKey *InstanceKey) (*Instance, error) {
	return DefaultStore().PostgreSQLPromoteStandby(instanceKey)
}

// PostgreSQLReconfigureStandby runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PostgreSQLReconfigureStandby on the tenant's Store.
func PostgreSQLReconfigureStandby(standbyKey *InstanceKey, newPrimaryKey *InstanceKey) error {
	return DefaultStore().PostgreSQLReconfigureStandby(standbyKey, newPrimaryKey)
}

// PostgreSQLSetReadOnly runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PostgreSQLSetReadOnly on the tenant's Store.
func PostgreSQLSetReadOnly(instanceKey *InstanceKey, readOnly bool) (*Instance, error) {
	return DefaultStore().PostgreSQLSetReadOnly(instanceKey, readOnly)
}

// PostgreSQLRepositionAsStandby runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PostgreSQLRepositionAsStandby on the tenant's Store.
func PostgreSQLRepositionAsStandby(instanceKey *InstanceKey, newPrimaryKey *InstanceKey) error {
	return DefaultStore().PostgreSQLRepositionAsStandby(instanceKey, newPrimaryKey)
}

// PostgreSQLGetCurrentWALLSN runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PostgreSQLGetCurrentWALLSN on the tenant's Store.
func PostgreSQLGetCurrentWALLSN(instanceKey *InstanceKey) (string, error) {
	return DefaultStore().PostgreSQLGetCurrentWALLSN(instanceKey)
}

// PostgreSQLWaitForStandbyLSN runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).PostgreSQLWaitForStandbyLSN on the tenant's Store.
func PostgreSQLWaitForStandbyLSN(standbyKey *InstanceKey, targetLSN string, timeout time.Duration) error {
	return DefaultStore().PostgreSQLWaitForStandbyLSN(standbyKey, targetLSN, timeout)
}
