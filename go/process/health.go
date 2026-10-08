/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package process

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/util"

	"github.com/patrickmn/go-cache"
	"github.com/proxysql/golib/log"
	"github.com/proxysql/orchestrator/go/raft"
)

// Node health timestamps. They are process-wide (there is one node health per
// process) and are driven by the control heartbeat only, never by a tenant.
var lastHealthCheckUnixNano int64
var lastGoodHealthCheckUnixNano int64
var LastContinousCheckHealthy int64

type NodeHealth struct {
	Hostname        string
	Token           string
	AppVersion      string
	FirstSeenActive string
	LastSeenActive  string
	ExtraInfo       string
	Command         string
	DBBackend       string

	LastReported time.Time
	onceUpdate   sync.Once
}

func NewNodeHealth() *NodeHealth {
	return &NodeHealth{
		Hostname:   ThisHostname,
		Token:      util.ProcessToken.Hash,
		AppVersion: config.RuntimeCLIFlags.ConfiguredVersion,
	}
}

func (nodeHealth *NodeHealth) Update() *NodeHealth {
	nodeHealth.onceUpdate.Do(func() {
		nodeHealth.Hostname = ThisHostname
		nodeHealth.Token = util.ProcessToken.Hash
		nodeHealth.AppVersion = config.RuntimeCLIFlags.ConfiguredVersion
	})
	nodeHealth.LastReported = time.Now()
	return nodeHealth
}

var ThisNodeHealth = NewNodeHealth()

type HealthStatus struct {
	Healthy            bool
	Hostname           string
	Token              string
	IsActiveNode       bool
	ActiveNode         *NodeHealth
	Error              error
	AvailableNodes     [](*NodeHealth)
	RaftLeader         string
	IsRaftLeader       bool
	RaftLeaderURI      string
	RaftAdvertise      string
	RaftHealthyMembers []string
}

type OrchestratorExecutionMode string

const (
	OrchestratorExecutionCliMode  OrchestratorExecutionMode = "CLIMode"
	OrchestratorExecutionHttpMode OrchestratorExecutionMode = "HttpMode"
)

func markHealthCheck(healthy bool) {
	atomic.StoreInt64(&lastHealthCheckUnixNano, time.Now().UnixNano())
	if healthy {
		atomic.StoreInt64(&lastGoodHealthCheckUnixNano, time.Now().UnixNano())
	}
}

// RegisterNode writes the heartbeat of this node (see Heartbeat) and returns
// the outcome of the control backend, which is the node health. Tenant mirror
// failures are not part of this result: use Heartbeat to get them.
func (c *Control) RegisterNode(nodeHealth *NodeHealth) (healthy bool, err error) {
	res := c.Heartbeat(nodeHealth)
	return res.Control.Healthy, res.Control.Err
}

// HealthTest attempts to write to the control backend and get a result. Tenant
// mirror failures do not make the node unhealthy here; see TenantsHealth.
func (c *Control) HealthTest() (health *HealthStatus, err error) {
	cacheKey := util.ProcessToken.Hash
	if healthStatus, found := c.healthCache.Get(cacheKey); found {
		return healthStatus.(*HealthStatus), nil
	}

	health = &HealthStatus{Healthy: false, Hostname: ThisHostname, Token: util.ProcessToken.Hash, ActiveNode: &NodeHealth{}}
	defer c.healthCache.Set(cacheKey, health, cache.DefaultExpiration)

	if healthy, err := c.RegisterNode(ThisNodeHealth); err != nil {
		health.Error = err
		return health, log.Errore(err)
	} else {
		health.Healthy = healthy
	}

	if orcraft.IsRaftEnabled() {
		health.ActiveNode.Hostname = orcraft.GetLeader()
		health.IsActiveNode = orcraft.IsLeader()
		health.RaftLeader = orcraft.GetLeader()
		health.RaftLeaderURI = orcraft.LeaderURI.Get()
		health.IsRaftLeader = orcraft.IsLeader()
		health.RaftAdvertise = c.control.backend.Config().RaftAdvertise
		health.RaftHealthyMembers = orcraft.HealthyMembers()
	} else {
		if health.ActiveNode, health.IsActiveNode, err = c.ElectedNode(); err != nil {
			health.Error = err
			return health, log.Errore(err)
		}
	}
	health.AvailableNodes, _ = c.ReadAvailableNodes(true)

	return health, nil
}

// ContinuousRegistration will continuously update the node_health
// table showing that the current process is still running.
func (c *Control) ContinuousRegistration(extraInfo string, command string) {
	ThisNodeHealth.ExtraInfo = extraInfo
	ThisNodeHealth.Command = command
	c.registerOnce.Do(func() {
		tickOperation := func() {
			healthy, err := c.RegisterNode(ThisNodeHealth)
			if err != nil {
				_ = log.Errorf("ContinuousRegistration: RegisterNode failed: %+v", err)
			}
			if healthy {
				atomic.StoreInt64(&LastContinousCheckHealthy, 1)
			} else {
				atomic.StoreInt64(&LastContinousCheckHealthy, 0)
			}
		}
		// First one is synchronous
		tickOperation()
		go func() {
			registrationTick := time.Tick(config.HealthPollSeconds * time.Second)
			for range registrationTick {
				// We already run inside a go-routine so
				// do not do this asynchronously.  If we
				// get stuck then we don't want to fill up
				// the backend pool with connections running
				// this maintenance operation.
				tickOperation()
			}
		}()
	})
}

// RegisterNode is the legacy wrapper of (*Control).RegisterNode on the default control.
func RegisterNode(nodeHealth *NodeHealth) (healthy bool, err error) {
	return DefaultControl().RegisterNode(nodeHealth)
}

// HealthTest is the legacy wrapper of (*Control).HealthTest on the default control.
func HealthTest() (health *HealthStatus, err error) {
	return DefaultControl().HealthTest()
}

// ContinuousRegistration is the legacy wrapper of (*Control).ContinuousRegistration
// on the default control.
func ContinuousRegistration(extraInfo string, command string) {
	DefaultControl().ContinuousRegistration(extraInfo, command)
}

func SinceLastHealthCheck() time.Duration {
	timeNano := atomic.LoadInt64(&lastHealthCheckUnixNano)
	if timeNano == 0 {
		return 0
	}
	return time.Since(time.Unix(0, timeNano))
}

func SinceLastGoodHealthCheck() time.Duration {
	timeNano := atomic.LoadInt64(&lastGoodHealthCheckUnixNano)
	if timeNano == 0 {
		return 0
	}
	return time.Since(time.Unix(0, timeNano))
}
