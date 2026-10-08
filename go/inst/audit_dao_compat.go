// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

// AuditOperation runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).AuditOperation on the tenant's Store.
func AuditOperation(auditType string, instanceKey *InstanceKey, message string) error {
	return DefaultStore().AuditOperation(auditType, instanceKey, message)
}

// ReadRecentAudit runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ReadRecentAudit on the tenant's Store.
func ReadRecentAudit(instanceKey *InstanceKey, page int) ([]Audit, error) {
	return DefaultStore().ReadRecentAudit(instanceKey, page)
}

// ExpireAudit runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).ExpireAudit on the tenant's Store.
func ExpireAudit() error {
	return DefaultStore().ExpireAudit()
}
