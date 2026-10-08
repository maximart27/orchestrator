// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// CanReplicateFrom runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceCanReplicateFrom on the tenant's Store.
func (this *Instance) CanReplicateFrom(other *Instance) (bool, error) {
	return DefaultStore().InstanceCanReplicateFrom(this, other)
}

// CanReplicateFromEx runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceCanReplicateFromEx on the tenant's Store.
func (this *Instance) CanReplicateFromEx(other *Instance, logContext string) (bool, error) {
	return DefaultStore().InstanceCanReplicateFromEx(this, other, logContext)
}

// HasReasonableMaintenanceReplicationLag runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceHasReasonableMaintenanceReplicationLag on the tenant's Store.
func (this *Instance) HasReasonableMaintenanceReplicationLag() bool {
	return DefaultStore().InstanceHasReasonableMaintenanceReplicationLag(this)
}

// CanMove runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceCanMove on the tenant's Store.
func (this *Instance) CanMove() (bool, error) {
	return DefaultStore().InstanceCanMove(this)
}

// StatusString runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceStatusString on the tenant's Store.
func (this *Instance) StatusString() string {
	return DefaultStore().InstanceStatusString(this)
}

// LagStatusString runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceLagStatusString on the tenant's Store.
func (this *Instance) LagStatusString() string {
	return DefaultStore().InstanceLagStatusString(this)
}

// HumanReadableDescription runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceHumanReadableDescription on the tenant's Store.
func (this *Instance) HumanReadableDescription() string {
	return DefaultStore().InstanceHumanReadableDescription(this)
}

// TabulatedDescription runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).InstanceTabulatedDescription on the tenant's Store.
func (this *Instance) TabulatedDescription(separator string) string {
	return DefaultStore().InstanceTabulatedDescription(this, separator)
}
