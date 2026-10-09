/*
   Copyright 2024 Percona LLC

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
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/proxysql/golib/log"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/process"
	orcraft "github.com/proxysql/orchestrator/go/raft"
)

// DefaultTenant is the value of the "tenant" label in single tenant (legacy) mode.
const DefaultTenant = "default"

// Tenant-scoped metrics are vectors with a "tenant" label, registered ONCE (see
// InitPrometheus). Tenants never register anything: they obtain their children
// through ForTenant. Process-wide metrics (health, raft) have no tenant label.
var (
	discoveriesTotalVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_discoveries_total",
		Help: "Total number of discovery attempts.",
	}, []string{"tenant"})

	discoveryErrorsTotalVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_discovery_errors_total",
		Help: "Total number of failed discoveries.",
	}, []string{"tenant"})

	instancesTotalVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "orchestrator_instances_total",
		Help: "Total number of known instances.",
	}, []string{"tenant"})

	clustersTotalVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "orchestrator_clusters_total",
		Help: "Total number of known clusters.",
	}, []string{"tenant"})

	recoveriesTotalVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_recoveries_total",
		Help: "Total number of recovery attempts.",
	}, []string{"tenant", "type", "result"})

	recoveryDurationSecondsVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "orchestrator_recovery_duration_seconds",
		Help:    "Duration of recovery operations in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 12), // 0.5s to ~1024s
	}, []string{"tenant"})

	discoveryQueueLengthVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "orchestrator_discovery_queue_length",
		Help: "Length of the discovery queue.",
	}, []string{"tenant"})

	deadInstancesDiscoveryQueueLengthVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "orchestrator_dead_instances_discovery_queue_length",
		Help: "Length of the dead instances discovery queue.",
	}, []string{"tenant"})

	activeRecoveriesVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "orchestrator_active_recoveries",
		Help: "Number of active (in-progress) recoveries.",
	}, []string{"tenant"})

	downtimedInstancesVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "orchestrator_downtimed_instances_total",
		Help: "Number of instances currently marked as downtimed.",
	}, []string{"tenant"})

	activeTopologyIssuesVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "orchestrator_active_topology_issues",
		Help: "Number of active topology issues, broken down by issue type.",
	}, []string{"tenant", "issue_type"})
)

// tenantVec is a vector with a "tenant" label.
type tenantVec interface {
	prometheus.Collector
	DeletePartialMatch(labels prometheus.Labels) int
}

// tenantVecs lists every tenant-labelled vector, for registration and cleanup.
var tenantVecs = []tenantVec{
	discoveriesTotalVec,
	discoveryErrorsTotalVec,
	instancesTotalVec,
	clustersTotalVec,
	recoveriesTotalVec,
	recoveryDurationSecondsVec,
	discoveryQueueLengthVec,
	deadInstancesDiscoveryQueueLengthVec,
	activeRecoveriesVec,
	downtimedInstancesVec,
	activeTopologyIssuesVec,
}

// TenantMetrics gives one tenant access to its own series of the tenant-scoped
// Prometheus metrics. Obtain it with ForTenant; it is safe for concurrent use.
type TenantMetrics struct {
	TenantID string

	DiscoveriesTotal                  prometheus.Counter
	DiscoveryErrorsTotal              prometheus.Counter
	InstancesTotal                    prometheus.Gauge
	ClustersTotal                     prometheus.Gauge
	RecoveryDurationSeconds           prometheus.Observer
	DiscoveryQueueLength              prometheus.Gauge
	DeadInstancesDiscoveryQueueLength prometheus.Gauge
	ActiveRecoveries                  prometheus.Gauge
	DowntimedInstances                prometheus.Gauge

	// RecoveriesTotal has the labels "type" and "result" (the tenant is already bound).
	RecoveriesTotal *prometheus.CounterVec
	// ActiveTopologyIssues has the label "issue_type" (the tenant is already bound).
	// Do NOT call Reset() on it, that clears every tenant: use ResetActiveTopologyIssues.
	ActiveTopologyIssues *prometheus.GaugeVec
}

var (
	tenantMetricsMu sync.Mutex
	tenantMetrics   = map[string]*TenantMetrics{}
)

// ForTenant returns the metrics of a tenant, creating its series on first use.
// It never registers anything with Prometheus, so it can be called any number
// of times for any tenant.
func ForTenant(tenantID string) *TenantMetrics {
	tenantMetricsMu.Lock()
	defer tenantMetricsMu.Unlock()
	if m, found := tenantMetrics[tenantID]; found {
		return m
	}
	m := &TenantMetrics{
		TenantID:                          tenantID,
		DiscoveriesTotal:                  discoveriesTotalVec.WithLabelValues(tenantID),
		DiscoveryErrorsTotal:              discoveryErrorsTotalVec.WithLabelValues(tenantID),
		InstancesTotal:                    instancesTotalVec.WithLabelValues(tenantID),
		ClustersTotal:                     clustersTotalVec.WithLabelValues(tenantID),
		RecoveryDurationSeconds:           recoveryDurationSecondsVec.WithLabelValues(tenantID),
		DiscoveryQueueLength:              discoveryQueueLengthVec.WithLabelValues(tenantID),
		DeadInstancesDiscoveryQueueLength: deadInstancesDiscoveryQueueLengthVec.WithLabelValues(tenantID),
		ActiveRecoveries:                  activeRecoveriesVec.WithLabelValues(tenantID),
		DowntimedInstances:                downtimedInstancesVec.WithLabelValues(tenantID),
		RecoveriesTotal:                   recoveriesTotalVec.MustCurryWith(prometheus.Labels{"tenant": tenantID}),
		ActiveTopologyIssues:              activeTopologyIssuesVec.MustCurryWith(prometheus.Labels{"tenant": tenantID}),
	}
	tenantMetrics[tenantID] = m
	return m
}

// ResetActiveTopologyIssues drops this tenant's topology issue series, leaving other tenants' untouched.
func (m *TenantMetrics) ResetActiveTopologyIssues() {
	activeTopologyIssuesVec.DeletePartialMatch(prometheus.Labels{"tenant": m.TenantID})
}

// Forget removes every series of the tenant (when it is removed from the process) and
// forgets the TenantMetrics; a later ForTenant call starts from zero.
func (m *TenantMetrics) Forget() {
	tenantMetricsMu.Lock()
	defer tenantMetricsMu.Unlock()
	for _, v := range tenantVecs {
		v.DeletePartialMatch(prometheus.Labels{"tenant": m.TenantID})
	}
	if tenantMetrics[m.TenantID] == m {
		delete(tenantMetrics, m.TenantID)
	}
}

// Legacy (single tenant) metric variables: the series of the DefaultTenant, exported so
// other packages can keep updating them. Per-tenant code must use ForTenant instead.
var (
	defaultTenantMetrics = ForTenant(DefaultTenant)

	DiscoveriesTotal                  = defaultTenantMetrics.DiscoveriesTotal
	DiscoveryErrorsTotal              = defaultTenantMetrics.DiscoveryErrorsTotal
	InstancesTotal                    = defaultTenantMetrics.InstancesTotal
	ClustersTotal                     = defaultTenantMetrics.ClustersTotal
	RecoveriesTotal                   = defaultTenantMetrics.RecoveriesTotal
	RecoveryDurationSeconds           = defaultTenantMetrics.RecoveryDurationSeconds.(prometheus.Histogram)
	DiscoveryQueueLength              = defaultTenantMetrics.DiscoveryQueueLength
	DeadInstancesDiscoveryQueueLength = defaultTenantMetrics.DeadInstancesDiscoveryQueueLength
	ActiveRecoveries                  = defaultTenantMetrics.ActiveRecoveries
	DowntimedInstances                = defaultTenantMetrics.DowntimedInstances
	ActiveTopologyIssues              = defaultTenantMetrics.ActiveTopologyIssues
)

// Process-wide metrics: node health and raft, not tenant-scoped (no tenant label).
var (
	HealthIsReady = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_health_is_ready",
		Help: "1 if the node is ready, 0 otherwise.",
	}, func() float64 {
		if atomic.LoadInt64(&process.LastContinousCheckHealthy) == 1 {
			return 1
		}
		return 0
	})

	HealthIsLive = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_health_is_live",
		Help: "1 if the process recently ran a health check tick (liveness), 0 otherwise.",
	}, func() float64 {
		// Liveness: health subsystem has run recently (even if the check failed).
		// Readiness is LastContinousCheckHealthy (successful registration).
		maxAge := time.Duration(config.HealthPollSeconds*3) * time.Second
		if d := process.SinceLastHealthCheck(); d > 0 && d <= maxAge {
			return 1
		}
		if d := process.SinceLastGoodHealthCheck(); d > 0 && d <= maxAge {
			return 1
		}
		// ContinuousRegistration updates LastContinousCheckHealthy without touching
		// SinceLastHealthCheck; treat a current healthy flag as live too.
		if atomic.LoadInt64(&process.LastContinousCheckHealthy) == 1 {
			return 1
		}
		return 0
	})

	RaftIsHealthy = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_raft_is_healthy",
		Help: "1 if raft is enabled and this node is healthy; 0 if unhealthy or raft disabled.",
	}, func() float64 {
		if orcraft.IsRaftEnabled() && orcraft.IsHealthy() {
			return 1
		}
		return 0
	})

	RaftIsLeader = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_raft_is_leader",
		Help: "1 if the node is the raft leader, 0 otherwise.",
	}, func() float64 {
		if orcraft.IsRaftEnabled() && orcraft.IsLeader() {
			return 1
		}
		return 0
	})

	RaftPeersTotal = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_raft_peers_total",
		Help: "Number of known raft peers.",
	}, func() float64 {
		if orcraft.IsRaftEnabled() {
			if peers, err := orcraft.GetPeers(); err == nil {
				return float64(len(peers))
			}
		}
		return 0
	})

	TenantSetMismatch = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_tenant_set_mismatch",
		Help: "1 if the tenants of this node differ from the ones announced by the raft leader, 0 otherwise.",
	}, func() float64 {
		if orcraft.TenantSetMismatch() {
			return 1
		}
		return 0
	})

	RaftUnknownTenantCommands = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "orchestrator_raft_unknown_tenant_commands_total",
		Help: "Raft commands this node could not apply since start because their tenant is unknown here (the tenants differ between nodes).",
	}, func() float64 {
		return float64(orcraft.UnknownTenantCommands())
	})

	RaftIsPartOfQuorum = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_raft_is_part_of_quorum",
		Help: "1 if the node is part of raft quorum, 0 otherwise.",
	}, func() float64 {
		if orcraft.IsRaftEnabled() && orcraft.IsPartOfQuorum() {
			return 1
		}
		return 0
	})
)

// InitPrometheus registers all Prometheus metrics. Should be called once at
// startup when PrometheusEnabled is true. Registration happens once for the
// whole process, never per tenant.
func InitPrometheus() {
	if !config.Config.PrometheusEnabled {
		return
	}
	log.Info("Initializing Prometheus metrics")
	registerPrometheus(prometheus.DefaultRegisterer)
}

var prometheusRegisterOnce sync.Once

func registerPrometheus(reg prometheus.Registerer) {
	prometheusRegisterOnce.Do(func() {
		registerPrometheusUnchecked(reg)
	})
}

// registerPrometheusUnchecked registers every collector, panicking on duplicates.
func registerPrometheusUnchecked(reg prometheus.Registerer) {
	for _, v := range tenantVecs {
		reg.MustRegister(v)
	}
	reg.MustRegister(
		HealthIsReady,
		HealthIsLive,
		RaftIsHealthy,
		RaftIsLeader,
		RaftPeersTotal,
		RaftIsPartOfQuorum,
		TenantSetMismatch,
		RaftUnknownTenantCommands,
	)
}
