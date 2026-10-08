// Transitional: package-level wrappers around DefaultStore(), the legacy single-tenant
// store, so the packages that still use the pre-multi-tenant API keep compiling.
// They are removed once those packages use their tenant's Store.

package inst

import (
	"github.com/proxysql/orchestrator/go/collection"
	"time"
)

// AggregatedSince runs against DefaultStore(), the legacy single-tenant store.
//
// Deprecated: use (*Store).AggregatedSince on the tenant's Store.
func AggregatedSince(c *collection.Collection, t time.Time) AggregatedWriteBufferMetric {
	return DefaultStore().AggregatedSince(c, t)
}
