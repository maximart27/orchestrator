/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package metrics

import (
	"fmt"
	"strings"
	"sync"
	"time"

	gometrics "github.com/rcrowley/go-metrics"

	"github.com/proxysql/orchestrator/go/config"
)

var (
	tickCallbacksMutex sync.Mutex
	tickCallbacks      = map[uint64]func(){}
	tickCallbacksSeq   uint64
)

// InitMetrics is called once in the lifetime of the app, after config has been loaded
func InitMetrics() error {
	go func() {
		metricsCallbackTick := time.Tick(time.Duration(config.DebugMetricsIntervalSeconds) * time.Second)
		for range metricsCallbackTick {
			for _, f := range snapshotTickCallbacks() {
				go f()
			}
		}
	}()

	return nil
}

func snapshotTickCallbacks() []func() {
	tickCallbacksMutex.Lock()
	defer tickCallbacksMutex.Unlock()
	callbacks := make([]func(), 0, len(tickCallbacks))
	for _, f := range tickCallbacks {
		callbacks = append(callbacks, f)
	}
	return callbacks
}

// OnMetricsTick registers a callback that runs on every metrics tick, for the lifetime of the process.
func OnMetricsTick(f func()) {
	_ = RegisterMetricsTick(f)
}

// RegisterMetricsTick registers a callback that runs on every metrics tick and returns
// a function that unregisters it (to be called when a tenant stops). Safe for concurrent use.
func RegisterMetricsTick(f func()) (unregister func()) {
	tickCallbacksMutex.Lock()
	defer tickCallbacksMutex.Unlock()
	tickCallbacksSeq++
	id := tickCallbacksSeq
	tickCallbacks[id] = f
	return func() {
		tickCallbacksMutex.Lock()
		defer tickCallbacksMutex.Unlock()
		delete(tickCallbacks, id)
	}
}

// TenantName returns the go-metrics (graphite) name of a tenant-scoped metric. The default
// tenant keeps the bare name, so legacy metric paths do not change; any other tenant is
// namespaced as "tenants.<tenantID>.<name>".
func TenantName(tenantID, name string) string {
	if tenantID == "" || tenantID == DefaultTenant {
		return name
	}
	return fmt.Sprintf("tenants.%s.%s", tenantID, name)
}

// RegisterTenantMetric registers a go-metrics metric under the tenant's name in the default
// registry. Unlike a bare "_ = metrics.Register(...)", a duplicate is reported to the caller.
func RegisterTenantMetric(tenantID, name string, metric interface{}) error {
	if err := gometrics.Register(TenantName(tenantID, name), metric); err != nil {
		return fmt.Errorf("registering metric %q for tenant %q: %v", name, tenantID, err)
	}
	return nil
}

// GetOrRegisterTenantMetric is the idempotent form of RegisterTenantMetric: if the tenant's metric
// already exists it is returned (and metric is discarded), otherwise metric is registered and returned.
func GetOrRegisterTenantMetric(tenantID, name string, metric interface{}) interface{} {
	return gometrics.GetOrRegister(TenantName(tenantID, name), metric)
}

// UnregisterTenantMetrics removes every go-metrics metric of a tenant from the default registry
// (when the tenant stops). It is a no-op for the default tenant, whose metrics are process-wide.
func UnregisterTenantMetrics(tenantID string) {
	if tenantID == "" || tenantID == DefaultTenant {
		return
	}
	prefix := TenantName(tenantID, "")
	var names []string
	gometrics.DefaultRegistry.Each(func(name string, _ interface{}) {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	})
	for _, name := range names {
		gometrics.Unregister(name)
	}
}
