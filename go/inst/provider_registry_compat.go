package inst

// SetProvider sets the database provider of the legacy default store.
//
// Deprecated: a tenant's provider is fixed by its configuration; use
// Store.SetProvider only on the legacy default Store.
func SetProvider(p DatabaseProvider) {
	DefaultStore().SetProvider(p)
}

// GetProvider returns the database provider of the legacy default store.
//
// Deprecated: use Store.GetProvider on the tenant's Store.
func GetProvider() DatabaseProvider {
	return DefaultStore().GetProvider()
}
