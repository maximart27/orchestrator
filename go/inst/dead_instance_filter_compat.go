package inst

// legacyDeadInstancesFilter keeps the old package-level `inst.DeadInstancesFilter`
// API alive: every method delegates to the filter of DefaultStore(). It holds no
// state of its own.
type legacyDeadInstancesFilter struct{}

// DeadInstancesFilter is the dead instances filter of the legacy default tenant.
//
// Deprecated: use Store.DeadInstancesFilter on the tenant's Store.
var DeadInstancesFilter legacyDeadInstancesFilter

// RegisterInstance registers the instance in the DefaultStore() dead instances filter.
func (legacyDeadInstancesFilter) RegisterInstance(instanceKey *InstanceKey) {
	DefaultStore().DeadInstancesFilter.RegisterInstance(instanceKey)
}

// UnregisterInstance removes the instance from the DefaultStore() dead instances filter.
func (legacyDeadInstancesFilter) UnregisterInstance(instanceKey *InstanceKey) {
	DefaultStore().DeadInstancesFilter.UnregisterInstance(instanceKey)
}

// InstanceRecheckNeeded queries the DefaultStore() dead instances filter.
func (legacyDeadInstancesFilter) InstanceRecheckNeeded(instanceKey *InstanceKey) (bool, bool) {
	return DefaultStore().DeadInstancesFilter.InstanceRecheckNeeded(instanceKey)
}
