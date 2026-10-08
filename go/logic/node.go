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

package logic

// This file is the node-level part of the continuous discovery: what is global
// to the process whatever the number of tenants -- the leadership, Raft, the
// health of the node and its backends, the signal handlers, the process metrics.
// The per-tenant work is in tenant_run.go.

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/proxysql/golib/log"
	"github.com/rcrowley/go-metrics"

	"github.com/proxysql/orchestrator/go/config"
	ometrics "github.com/proxysql/orchestrator/go/metrics"
	"github.com/proxysql/orchestrator/go/process"
	orcraft "github.com/proxysql/orchestrator/go/raft"
)

const (
	// yieldAfterUnhealthyDuration is how long the node may fail to register its
	// health (control backend) before it yields the Raft leadership.
	yieldAfterUnhealthyDuration = 5 * config.HealthPollSeconds * time.Second
	// fatalAfterUnhealthyDuration is how long before the node gives up altogether.
	fatalAfterUnhealthyDuration = 30 * config.HealthPollSeconds * time.Second
	// tenantYieldAfterUnhealthyDuration is how long the backend of a tenant may
	// keep failing before the node yields the Raft leadership so that another
	// node takes over every tenant. A broken tenant never stops the process
	// (there is no fatal threshold for tenants): it keeps serving whatever a
	// non-leader node serves.
	tenantYieldAfterUnhealthyDuration = yieldAfterUnhealthyDuration
	// sigtermStopTimeout bounds the orderly stop of the tenants on SIGTERM.
	sigtermStopTimeout = 30 * time.Second
)

var (
	// isElectedNode is the election outcome of this node when Raft is not used.
	isElectedNode int64

	isElectedGauge     = metrics.NewGauge()
	isHealthyGauge     = metrics.NewGauge()
	isRaftHealthyGauge = metrics.NewGauge()
	isRaftLeaderGauge  = metrics.NewGauge()

	registerNodeMetricsOnce sync.Once
	processMetricsOnce      sync.Once
	signalsOnce             sync.Once
	// signalManager is the manager the signal handlers act on.
	signalManager atomic.Pointer[TenantManager]
)

// IsLeader returns true if the current orchestrator instance is the cluster leader.
// The leadership is global to the process: a single leader runs every tenant.
func IsLeader() bool {
	if orcraft.IsRaftEnabled() {
		return orcraft.IsLeader()
	}
	return atomic.LoadInt64(&isElectedNode) == 1
}

// IsLeaderOrActive returns true if the current orchestrator instance is either the cluster leader
// or an active member (when Raft is enabled, returns true if part of quorum).
func IsLeaderOrActive() bool {
	if orcraft.IsRaftEnabled() {
		return orcraft.IsPartOfQuorum()
	}
	return atomic.LoadInt64(&isElectedNode) == 1
}

// registerNodeMetrics registers, once, the go-metrics of the node and the
// callbacks that refresh them.
func registerNodeMetrics() {
	registerNodeMetricsOnce.Do(func() {
		_ = metrics.Register("elect.is_elected", isElectedGauge)
		_ = metrics.Register("health.is_healthy", isHealthyGauge)
		_ = metrics.Register("raft.is_healthy", isRaftHealthyGauge)
		_ = metrics.Register("raft.is_leader", isRaftLeaderGauge)

		ometrics.OnMetricsTick(func() {
			isElectedGauge.Update(atomic.LoadInt64(&isElectedNode))
		})
		ometrics.OnMetricsTick(func() {
			isHealthyGauge.Update(atomic.LoadInt64(&process.LastContinousCheckHealthy))
		})
		ometrics.OnMetricsTick(func() {
			var healthy int64
			if orcraft.IsHealthy() {
				healthy = 1
			}
			isRaftHealthyGauge.Update(healthy)
		})
		ometrics.OnMetricsTick(func() {
			isRaftLeaderGauge.Update(atomic.LoadInt64(&isElectedNode))
		})
	})
}

// bootstrapProcess does, once for the process, what is global: metrics, signal
// handlers, Raft and the election grab flag.
func (m *TenantManager) bootstrapProcess() error {
	registerNodeMetrics()
	processMetricsOnce.Do(func() {
		go func() { _ = ometrics.InitMetrics() }()
		go func() { _ = ometrics.InitGraphiteMetrics() }()
	})
	signalManager.Store(m)
	signalsOnce.Do(func() { go acceptSignals() })
	if m.base.RaftEnabled {
		if err := orcraft.Setup(NewCommandApplier(m), NewSnapshotDataCreatorApplier(m), process.ThisHostname); err != nil {
			return fmt.Errorf("raft: %w", err)
		}
		go orcraft.Monitor()
	}
	if grab := config.RuntimeCLIFlags.GrabElection; grab != nil && *grab {
		_ = m.control.GrabElection()
	}
	return nil
}

// acceptSignals registers for OS signals
func acceptSignals() {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		for sig := range c {
			m := signalManager.Load()
			if m == nil {
				continue
			}
			switch sig {
			case syscall.SIGHUP:
				log.Infof("Received SIGHUP. Reloading configuration")
				for _, t := range m.tenants {
					_ = t.Store.AuditOperation("reload-configuration", nil, "Triggered via SIGHUP")
				}
				// This reloads the global configuration, which is the configuration of the
				// legacy tenant; the tenants of a tenants file keep the configuration they
				// started with.
				config.Reload()
				for _, t := range m.tenants {
					t.discoveryMetrics.SetExpirePeriod(time.Duration(t.Cfg.DiscoveryCollectionRetentionSeconds) * time.Second)
				}
			case syscall.SIGTERM:
				log.Infof("Received SIGTERM. Shutting down orchestrator")
				for _, t := range m.tenants {
					t.discoveryMetrics.StopAutoExpiration()
					_ = t.Store.AuditOperation("shutdown", nil, "Triggered via SIGTERM")
				}
				// stop the tenants in an orderly way (flush write buffers, close backends),
				// but do not let a stuck operation keep the process from exiting
				stopped := make(chan struct{})
				go func() {
					m.Stop()
					close(stopped)
				}()
				select {
				case <-stopped:
				case <-time.After(sigtermStopTimeout):
					log.Warningf("Tenants did not stop within %v; exiting anyway", sigtermStopTimeout)
				}
				os.Exit(0)
			}
		}
	}()
}

// runNode runs the node-level periodic work until ctx is done: the health tick
// and the maintenance of the control data.
func (m *TenantManager) runNode(ctx context.Context) {
	healthTick := newTicker(config.HealthPollSeconds * time.Second)
	defer healthTick.stop()
	caretakingTick := newTicker(caretakingInterval)
	defer caretakingTick.stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	spawn := func(f func()) {
		if ctx.Err() != nil {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-healthTick.C():
			spawn(m.onHealthTick)
		case <-caretakingTick.C():
			spawn(m.onCaretakingTick)
		}
	}
}

// onHealthTick is the node part of the health tick: it settles the election
// (Raft's or, without Raft, the one of the control backend) and watches the
// health of the node and of the backends of the tenants.
func (m *TenantManager) onHealthTick() {
	wasAlreadyElected := IsLeader()

	if orcraft.IsRaftEnabled() {
		if orcraft.IsLeader() {
			atomic.StoreInt64(&isElectedNode, 1)
		} else {
			atomic.StoreInt64(&isElectedNode, 0)
		}
		// The health of the node is the one of its control backend (and Raft).
		if process.SinceLastGoodHealthCheck() > yieldAfterUnhealthyDuration {
			_ = log.Errorf("Health test is failing for over %+v seconds. raft yielding", yieldAfterUnhealthyDuration.Seconds())
			_ = orcraft.Yield()
		}
		if process.SinceLastGoodHealthCheck() > fatalAfterUnhealthyDuration {
			_ = orcraft.FatalRaftError(fmt.Errorf("node is unable to register health, please check database connectivity and/or time synchronization"))
		}
		m.yieldForUnhealthyTenants()
	}
	if !orcraft.IsRaftEnabled() {
		myIsElectedNode, err := m.control.AttemptElection()
		if err != nil {
			_ = log.Errore(err)
		}
		if myIsElectedNode {
			atomic.StoreInt64(&isElectedNode, 1)
		} else {
			atomic.StoreInt64(&isElectedNode, 0)
		}
		if !myIsElectedNode {
			if electedNode, _, err := m.control.ElectedNode(); err == nil {
				log.Infof("Not elected as active node; active node: %v; polling", electedNode.Hostname)
			} else {
				log.Infof("Not elected as active node; active node: Unable to determine: %v; polling", err)
			}
		}
	}
	if !IsLeaderOrActive() {
		return
	}
	if !wasAlreadyElected {
		// Just turned to be leader!
		go func() { _, _ = m.control.RegisterNode(process.ThisNodeHealth) }()
	}
}

// yieldForUnhealthyTenants makes the node give up the Raft leadership when the
// backend of a tenant has been failing for tenantYieldAfterUnhealthyDuration:
// leadership is global, so another node takes over all tenants. The backend of
// a tenant never makes the node fatal.
func (m *TenantManager) yieldForUnhealthyTenants() {
	if !orcraft.IsLeader() {
		return
	}
	for _, th := range m.control.Health().UnhealthyTenants(tenantYieldAfterUnhealthyDuration) {
		_ = orcraft.YieldForTenantFailure(th.TenantID, th.Error)
		return
	}
}

// onCaretakingTick runs the maintenance of the control data (node health
// history, access tokens), which is node-level, not tenant-level.
func (m *TenantManager) onCaretakingTick() {
	if !IsLeaderOrActive() {
		return
	}
	var wg sync.WaitGroup
	for _, expire := range []func() error{m.control.ExpireNodesHistory, m.control.ExpireAccessTokens, m.control.ExpireAvailableNodes} {
		expire := expire
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = expire()
		}()
	}
	wg.Wait()
}
