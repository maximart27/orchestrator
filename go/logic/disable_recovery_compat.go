// Transitional: package-level wrappers around DefaultTenant(), the tenant of the
// legacy single-tenant setup, so the packages that still use the pre-multi-tenant
// API keep compiling. They are removed once those packages resolve their tenant in
// the TenantManager.

package logic

// IsRecoveryDisabled runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).IsRecoveryDisabled on the tenant it applies to.
func IsRecoveryDisabled() (disabled bool, err error) {
	return DefaultTenant().IsRecoveryDisabled()
}

// DisableRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).DisableRecovery on the tenant it applies to.
func DisableRecovery() error {
	return DefaultTenant().DisableRecovery()
}

// EnableRecovery runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).EnableRecovery on the tenant it applies to.
func EnableRecovery() error {
	return DefaultTenant().EnableRecovery()
}

// SetRecoveryDisabled runs against DefaultTenant(), the tenant of the legacy single-tenant setup.
//
// Deprecated: use (*Tenant).SetRecoveryDisabled on the tenant it applies to.
func SetRecoveryDisabled(disabled bool) error {
	return DefaultTenant().SetRecoveryDisabled(disabled)
}
