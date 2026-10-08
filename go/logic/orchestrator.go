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

import (
	"math/rand"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/proxysql/golib/log"
	"github.com/proxysql/orchestrator/go/agent"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/discovery"
	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/kv"
	orcraft "github.com/proxysql/orchestrator/go/raft"
	"github.com/proxysql/orchestrator/go/util"
	"github.com/sjmudd/stopwatch"
)

// used in several places
func (t *Tenant) instancePollSecondsDuration() time.Duration {
	return time.Duration(t.Cfg.InstancePollSeconds) * time.Second
}

// DiscoverInstance will attempt to discover (poll) an instance (unless
// it is already up to date) and will also ensure that its master and
// replicas (if any) are also checked.
func (t *Tenant) DiscoverInstance(instanceKey inst.InstanceKey) {
	if t.Store.InstanceIsForgotten(&instanceKey) {
		log.Debugf("discoverInstance: skipping discovery of %+v because it is set to be forgotten", instanceKey)
		return
	}
	if inst.FiltersMatchInstanceKey(&instanceKey, t.Cfg.DiscoveryIgnoreHostnameFilters) {
		log.Debugf("discoverInstance: skipping discovery of %+v because it matches DiscoveryIgnoreHostnameFilters", instanceKey)
		return
	}

	// create stopwatch entries
	latency := stopwatch.NewNamedStopwatch()
	_ = latency.AddMany([]string{
		"backend",
		"instance",
		"total"})
	latency.Start("total") // start the total stopwatch (not changed anywhere else)

	defer func() {
		latency.Stop("total")
		discoveryTime := latency.Elapsed("total")
		if discoveryTime > t.instancePollSecondsDuration() {
			t.instancePollSecondsExceededCounter.Inc(1)
			_ = log.Warningf("discoverInstance exceeded InstancePollSeconds for %+v, took %.4fs", instanceKey, discoveryTime.Seconds())
		}
	}()

	_, _ = t.Store.InstanceKeyResolveHostname(&instanceKey)
	if !instanceKey.IsValid() {
		return
	}

	// Calculate the expiry period each time as InstancePollSeconds
	// _may_ change during the run of the process (via SIGHUP) and
	// it is not possible to change the cache's default expiry..
	if existsInCacheError := t.recentDiscoveryOperationKeys.Add(instanceKey.DisplayString(), true, t.instancePollSecondsDuration()); existsInCacheError != nil {
		// Just recently attempted
		return
	}

	latency.Start("backend")
	instance, found, _ := t.Store.ReadInstance(&instanceKey)
	latency.Stop("backend")
	if found && instance.IsUpToDate && instance.IsLastCheckValid {
		// we've already discovered this one. Skip!
		return
	}

	t.discoveriesCounter.Inc(1)
	t.Metrics.DiscoveriesTotal.Inc()

	// First we've ever heard of this instance. Continue investigation:
	var err error
	skipped := false
	instance, skipped, err = t.Store.ReadTopologyInstanceBufferable(&instanceKey, t.Cfg.BufferInstanceWrites, latency)
	// panic can occur (IO stuff). Therefore it may happen
	// that instance is nil. Check it, but first get the timing metrics.
	totalLatency := latency.Elapsed("total")
	backendLatency := latency.Elapsed("backend")
	instanceLatency := latency.Elapsed("instance")

	if skipped {
		if t.Cfg.EnableDiscoveryFiltersLogs {
			log.Infof("discoverInstance: skipping discovery of %+v because its replication user matches DiscoveryIgnoreReplicationUsernameFilters", instanceKey)
		}
		return
	}

	if instance == nil {
		t.failedDiscoveriesCounter.Inc(1)
		t.Metrics.DiscoveryErrorsTotal.Inc()
		_ = t.discoveryMetrics.Append(&discovery.Metric{
			Timestamp:       time.Now(),
			InstanceKey:     instanceKey,
			TotalLatency:    totalLatency,
			BackendLatency:  backendLatency,
			InstanceLatency: instanceLatency,
			Err:             err,
		})
		if util.ClearToLog("discoverInstance", t.ID+"/"+instanceKey.StringCode()) {
			_ = log.Warningf("DiscoverInstance(%+v) instance is nil in %.3fs (Backend: %.3fs, Instance: %.3fs), error=%+v",
				instanceKey,
				totalLatency.Seconds(),
				backendLatency.Seconds(),
				instanceLatency.Seconds(),
				err)
		}
		return
	}

	_ = t.discoveryMetrics.Append(&discovery.Metric{
		Timestamp:       time.Now(),
		InstanceKey:     instanceKey,
		TotalLatency:    totalLatency,
		BackendLatency:  backendLatency,
		InstanceLatency: instanceLatency,
		Err:             nil,
	})

	if !t.isLeaderOrActive() {
		// Maybe this node was elected before, but isn't elected anymore.
		// If not elected, stop drilling up/down the topology
		return
	}

	// Investigate replicas and members of the same replication group:
	for _, replicaKey := range append(instance.ReplicationGroupMembers.GetInstanceKeys(), instance.Replicas.GetInstanceKeys()...) {
		replicaKey := replicaKey // not needed? no concurrency here?

		// Avoid noticing some hosts we would otherwise discover
		if inst.FiltersMatchInstanceKey(&replicaKey, t.Cfg.DiscoveryIgnoreReplicaHostnameFilters) {
			continue
		}

		if !replicaKey.IsValid() {
			continue
		}

		dead, recheck := t.Store.DeadInstancesFilter.InstanceRecheckNeeded(&replicaKey)
		if dead {
			if recheck {
				t.deadInstancesDiscoveryQueue.Push(replicaKey)
			}
			// dead, but not recheck time
			continue
		} else {
			t.discoveryQueue.Push(replicaKey)
		}
	}
	// Investigate master:
	if instance.MasterKey.IsValid() {
		if !inst.FiltersMatchInstanceKey(&instance.MasterKey, t.Cfg.DiscoveryIgnoreMasterHostnameFilters) {
			dead, recheck := t.Store.DeadInstancesFilter.InstanceRecheckNeeded(&instance.MasterKey)

			if dead {
				if recheck {
					t.deadInstancesDiscoveryQueue.Push(instance.MasterKey)
				}
			} else {
				t.discoveryQueue.Push(instance.MasterKey)
			}
		}
	}
}

// publishDiscoverMasters will publish to raft a discovery request for all known masters.
// This makes for a best-effort keep-in-sync between raft nodes, where some may have
// inconsistent data due to hosts being forgotten, for example.
func (t *Tenant) publishDiscoverMasters() error {
	instances, err := t.Store.ReadWriteableClustersMasters()
	if err == nil {
		for _, instance := range instances {
			key := instance.Key
			go func() { _, _ = orcraft.PublishTenantCommand(t.ID, "discover", key) }()
		}
	}
	return log.Errore(err)
}

// InjectPseudoGTIDOnWriters will inject a PseudoGTID entry on all writable, accessible,
// supported writers.
func (t *Tenant) InjectPseudoGTIDOnWriters() error {
	instances, err := t.Store.ReadWriteableClustersMasters()
	if err != nil {
		return log.Errore(err)
	}
	for i := range rand.Perm(len(instances)) {
		instance := instances[i]
		go func() {
			if injected, _ := t.Store.CheckAndInjectPseudoGTIDOnWriter(instance); injected {
				clusterName := instance.ClusterName
				if orcraft.IsRaftEnabled() {
					// We prefer not saturating our raft communication. Pseudo-GTID information is
					// OK to be cached for a while.
					if _, found := t.pseudoGTIDPublishCache.Get(clusterName); !found {
						t.pseudoGTIDPublishCache.Set(clusterName, true, cache.DefaultExpiration)
						_, _ = orcraft.PublishTenantCommand(t.ID, "injected-pseudo-gtid", clusterName)
					}
				} else {
					_ = t.Store.RegisterInjectedPseudoGTID(clusterName)
				}
			}
		}()
	}
	return nil
}

// Write a cluster's master (or all clusters masters) to kv stores.
// This should generally only happen once in a lifetime of a cluster. Otherwise KV
// stores are updated via failovers.
func (t *Tenant) SubmitMastersToKvStores(clusterName string, force bool) (kvPairs [](*kv.KVPair), submittedCount int, err error) {
	kvPairs, err = t.Store.GetMastersKVPairs(clusterName)
	log.Debugf("kv.SubmitMastersToKvStores, clusterName: %s, force: %+v: numPairs: %+v", clusterName, force, len(kvPairs))
	if err != nil {
		return kvPairs, submittedCount, log.Errore(err)
	}
	var selectedError error
	var submitKvPairs [](*kv.KVPair)
	for _, kvPair := range kvPairs {
		if !force {
			// !force: Called periodically to auto-populate KV
			// We'd like to avoid some overhead.
			if _, found := t.kvFoundCache.Get(kvPair.Key); found {
				// Let's not overload database with queries. Let's not overload raft with events.
				continue
			}
			v, found, err := t.KV.GetValue(kvPair.Key)
			if err == nil && found && v == kvPair.Value {
				// Already has the right value.
				t.kvFoundCache.Set(kvPair.Key, true, cache.DefaultExpiration)
				continue
			}
		}
		submitKvPairs = append(submitKvPairs, kvPair)
	}
	log.Debugf("kv.SubmitMastersToKvStores: submitKvPairs: %+v", len(submitKvPairs))
	if orcraft.IsRaftEnabled() {
		for _, kvPair := range submitKvPairs {
			_, err := orcraft.PublishTenantCommand(t.ID, "put-key-value", kvPair)
			if err == nil {
				submittedCount++
			} else {
				selectedError = err
			}
		}
	} else {
		err := t.KV.PutKVPairs(submitKvPairs)
		if err == nil {
			submittedCount += len(submitKvPairs)
		} else {
			selectedError = err
		}
	}
	if err := t.KV.DistributePairs(kvPairs); err != nil {
		_ = log.Errore(err)
	}
	return kvPairs, submittedCount, log.Errore(selectedError)
}

func pollAgent(hostname string) error {
	polledAgent, err := agent.GetAgent(hostname)
	_ = agent.UpdateAgentLastChecked(hostname)

	if err != nil {
		return log.Errore(err)
	}

	err = agent.UpdateAgentInfo(hostname, polledAgent)
	if err != nil {
		return log.Errore(err)
	}

	return nil
}

// ContinuousAgentsPoll starts an asynchronuous infinite process where agents are
// periodically investigated and their status captured, and long since unseen agents are
// purged and forgotten.
//
// The agents subsystem is not multi-tenant: it works on the legacy default
// backend and store, like the agent package itself.
func ContinuousAgentsPoll() {
	log.Infof("Starting continuous agents poll")

	go discoverSeededAgents()

	pollTicker := time.NewTicker(config.HealthPollSeconds * time.Second)
	defer pollTicker.Stop()
	caretakingTicker := time.NewTicker(time.Hour)
	defer caretakingTicker.Stop()
	caretakingTick := caretakingTicker.C
	for range pollTicker.C {
		agentsHosts, _ := agent.ReadOutdatedAgentsHosts()
		log.Debugf("outdated agents hosts: %+v", agentsHosts)
		for _, hostname := range agentsHosts {
			go pollAgent(hostname)
		}
		// See if we should also forget agents (lower frequency)
		select {
		case <-caretakingTick:
			_ = agent.ForgetLongUnseenAgents()
			_ = agent.FailStaleSeeds()
		default:
		}
	}
}

func discoverSeededAgents() {
	for seededAgent := range agent.SeededAgents {
		instanceKey := &inst.InstanceKey{Hostname: seededAgent.Hostname, Port: int(seededAgent.MySQLPort)}
		go func() { _, _ = DefaultTenant().Store.ReadTopologyInstance(instanceKey) }()
	}
}
