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

package logic

import (
	"encoding/json"
	"fmt"

	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/kv"
	orcraft "github.com/proxysql/orchestrator/go/raft"

	"github.com/proxysql/golib/log"
)

// CommandApplier applies the commands of the Raft log. Global commands
// (orcraft.IsGlobalOp) are applied by the node; every other command carries the
// tenant it belongs to and is applied on the runtime of that tenant, and on no
// other. It implements orcraft.CommandApplier and orcraft.TenantCommandApplier.
type CommandApplier struct {
	manager *TenantManager
}

// NewCommandApplier returns the applier of the commands of manager's tenants.
// A nil manager is the default manager (see DefaultManager).
func NewCommandApplier(manager *TenantManager) *CommandApplier {
	return &CommandApplier{manager: manager}
}

func (applier *CommandApplier) tenants() *TenantManager {
	if applier.manager == nil {
		return DefaultManager()
	}
	return applier.manager
}

// ApplyCommand applies a global command. A tenant-scoped command that reaches it
// (an applier that is not tenant-aware cannot be told otherwise) belongs to the
// legacy tenant.
func (applier *CommandApplier) ApplyCommand(op string, value []byte) interface{} {
	switch op {
	case orcraft.HeartbeatCommand:
		return nil
	case orcraft.AsyncSnapshotCommand:
		return applier.asyncSnapshot(value)
	case orcraft.RegisterNodeCommand:
		return applier.registerNode(value)
	case orcraft.LeaderURICommand:
		return applier.leaderURI(value)
	case orcraft.RequestHealthReportCommand:
		return applier.healthReport(value)
	case orcraft.TenantSetCommand:
		return applier.tenants().applyTenantSet(value)
	}
	return applier.ApplyTenantCommand(orcraft.LegacyTenantID, op, value)
}

// ApplyTenantCommand applies a tenant-scoped command on the runtime of tenantID.
// A tenant this node has no runtime for is not an error to panic about: the
// command is not applied and the error wraps orcraft.ErrUnknownTenant.
func (applier *CommandApplier) ApplyTenantCommand(tenantID, op string, value []byte) interface{} {
	if orcraft.IsGlobalOp(op) {
		return applier.ApplyCommand(op, value)
	}
	t, found := applier.tenants().Get(tenantID)
	if !found {
		return fmt.Errorf("%w: %s", orcraft.ErrUnknownTenant, tenantID)
	}
	return t.applyCommand(op, value)
}

func (applier *CommandApplier) asyncSnapshot(value []byte) interface{} {
	err := orcraft.AsyncSnapshot()
	return err
}

func (applier *CommandApplier) registerNode(value []byte) interface{} {
	return nil
}

func (applier *CommandApplier) leaderURI(value []byte) interface{} {
	var uri string
	if err := json.Unmarshal(value, &uri); err != nil {
		return log.Errore(err)
	}
	orcraft.LeaderURI.Set(uri)
	return nil
}

func (applier *CommandApplier) healthReport(value []byte) interface{} {
	var authenticationToken string
	if err := json.Unmarshal(value, &authenticationToken); err != nil {
		return log.Errore(err)
	}
	_ = orcraft.ReportToRaftLeader(authenticationToken)
	return nil
}

// applyCommand applies a tenant-scoped Raft command on this tenant.
func (t *Tenant) applyCommand(op string, value []byte) interface{} {
	switch op {
	case "discover":
		return t.applyDiscover(value)
	case "injected-pseudo-gtid":
		return t.applyInjectedPseudoGTID(value)
	case "forget":
		return t.applyForget(value)
	case "forget-cluster":
		return t.applyForgetCluster(value)
	case "begin-downtime":
		return t.applyBeginDowntime(value)
	case "end-downtime":
		return t.applyEndDowntime(value)
	case "register-candidate":
		return t.applyRegisterCandidate(value)
	case "ack-recovery":
		return t.applyAckRecovery(value)
	case "register-hostname-unresolve":
		return t.applyRegisterHostnameUnresolve(value)
	case "submit-pool-instances":
		return t.applySubmitPoolInstances(value)
	case "register-failure-detection":
		return t.applyRegisterFailureDetection(value)
	case "write-recovery":
		return t.applyWriteRecovery(value)
	case "write-recovery-step":
		return t.applyWriteRecoveryStep(value)
	case "resolve-recovery":
		return t.applyResolveRecovery(value)
	case "disable-global-recoveries":
		return t.DisableRecovery()
	case "enable-global-recoveries":
		return t.EnableRecovery()
	case "put-key-value":
		return t.applyPutKeyValue(value)
	case "put-instance-tag":
		return t.applyPutInstanceTag(value)
	case "delete-instance-tag":
		return t.applyDeleteInstanceTag(value)
	case "set-cluster-alias-manual-override":
		return t.applySetClusterAliasManualOverride(value)
	}
	return log.Errorf("Unknown command op: %s", op)
}

func (t *Tenant) applyDiscover(value []byte) interface{} {
	instanceKey := inst.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		return log.Errore(err)
	}
	t.DiscoverInstance(instanceKey)
	return nil
}

func (t *Tenant) applyInjectedPseudoGTID(value []byte) interface{} {
	var clusterName string
	if err := json.Unmarshal(value, &clusterName); err != nil {
		return log.Errore(err)
	}
	_ = t.Store.RegisterInjectedPseudoGTID(clusterName)
	return nil
}

func (t *Tenant) applyForget(value []byte) interface{} {
	instanceKey := inst.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		return log.Errore(err)
	}
	err := t.Store.ForgetInstance(&instanceKey)
	return err
}

func (t *Tenant) applyForgetCluster(value []byte) interface{} {
	var clusterName string
	if err := json.Unmarshal(value, &clusterName); err != nil {
		return log.Errore(err)
	}
	err := t.Store.ForgetCluster(clusterName)
	return err
}

func (t *Tenant) applyBeginDowntime(value []byte) interface{} {
	downtime := inst.Downtime{}
	if err := json.Unmarshal(value, &downtime); err != nil {
		return log.Errore(err)
	}
	err := t.Store.BeginDowntime(&downtime)
	return err
}

func (t *Tenant) applyEndDowntime(value []byte) interface{} {
	instanceKey := inst.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		return log.Errore(err)
	}
	_, err := t.Store.EndDowntime(&instanceKey)
	return err
}

func (t *Tenant) applyRegisterCandidate(value []byte) interface{} {
	candidate := inst.CandidateDatabaseInstance{}
	if err := json.Unmarshal(value, &candidate); err != nil {
		return log.Errore(err)
	}
	err := t.Store.RegisterCandidateInstance(&candidate)
	return err
}

func (t *Tenant) applyAckRecovery(value []byte) interface{} {
	ack := RecoveryAcknowledgement{}
	err := json.Unmarshal(value, &ack)
	if err != nil {
		return log.Errore(err)
	}
	if ack.AllRecoveries {
		_, err = t.AcknowledgeAllRecoveries(ack.Owner, ack.Comment)
	}
	if ack.ClusterName != "" {
		_, err = t.AcknowledgeClusterRecoveries(ack.ClusterName, ack.Owner, ack.Comment)
	}
	if ack.Key.IsValid() {
		_, err = t.AcknowledgeInstanceRecoveries(&ack.Key, ack.Owner, ack.Comment)
	}
	if ack.Id > 0 {
		_, err = t.AcknowledgeRecovery(ack.Id, ack.Owner, ack.Comment)
	}
	if ack.UID != "" {
		_, err = t.AcknowledgeRecoveryByUID(ack.UID, ack.Owner, ack.Comment)
	}
	return err
}

func (t *Tenant) applyRegisterHostnameUnresolve(value []byte) interface{} {
	registration := inst.HostnameRegistration{}
	if err := json.Unmarshal(value, &registration); err != nil {
		return log.Errore(err)
	}
	err := t.Store.RegisterHostnameUnresolve(&registration)
	return err
}

func (t *Tenant) applySubmitPoolInstances(value []byte) interface{} {
	submission := inst.PoolInstancesSubmission{}
	if err := json.Unmarshal(value, &submission); err != nil {
		return log.Errore(err)
	}
	err := t.Store.ApplyPoolInstances(&submission)
	return err
}

func (t *Tenant) applyRegisterFailureDetection(value []byte) interface{} {
	analysisEntry := inst.ReplicationAnalysis{}
	if err := json.Unmarshal(value, &analysisEntry); err != nil {
		return log.Errore(err)
	}
	_, err := t.AttemptFailureDetectionRegistration(&analysisEntry)
	return err
}

func (t *Tenant) applyWriteRecovery(value []byte) interface{} {
	topologyRecovery := TopologyRecovery{}
	if err := json.Unmarshal(value, &topologyRecovery); err != nil {
		return log.Errore(err)
	}
	if _, err := t.writeTopologyRecovery(&topologyRecovery); err != nil {
		return err
	}
	return nil
}

func (t *Tenant) applyWriteRecoveryStep(value []byte) interface{} {
	topologyRecoveryStep := TopologyRecoveryStep{}
	if err := json.Unmarshal(value, &topologyRecoveryStep); err != nil {
		return log.Errore(err)
	}
	err := t.writeTopologyRecoveryStep(&topologyRecoveryStep)
	return err
}

func (t *Tenant) applyResolveRecovery(value []byte) interface{} {
	topologyRecovery := TopologyRecovery{}
	if err := json.Unmarshal(value, &topologyRecovery); err != nil {
		return log.Errore(err)
	}
	if err := t.writeResolveRecovery(&topologyRecovery); err != nil {
		return log.Errore(err)
	}
	return nil
}

func (t *Tenant) applyPutKeyValue(value []byte) interface{} {
	kvPair := &kv.KVPair{}
	if err := json.Unmarshal(value, kvPair); err != nil {
		return log.Errore(err)
	}
	err := t.KV.PutKVPairs([]*kv.KVPair{kvPair})
	return err
}

func (t *Tenant) applyPutInstanceTag(value []byte) interface{} {
	instanceTag := inst.InstanceTag{}
	if err := json.Unmarshal(value, &instanceTag); err != nil {
		return log.Errore(err)
	}
	err := t.Store.PutInstanceTag(&instanceTag.Key, &instanceTag.T)
	return err
}

// applyDeleteInstanceTag answers the instances the tag was removed from (the API of the leader
// reports them) or the error.
func (t *Tenant) applyDeleteInstanceTag(value []byte) interface{} {
	untag := inst.InstanceUntag{}
	if err := json.Unmarshal(value, &untag); err != nil {
		return log.Errore(err)
	}
	untagged, err := t.Store.Untag(untag.Key, &untag.T)
	if err != nil {
		return err
	}
	return untagged
}

func (t *Tenant) applySetClusterAliasManualOverride(value []byte) interface{} {
	var params [2]string
	if err := json.Unmarshal(value, &params); err != nil {
		return log.Errore(err)
	}
	clusterName, alias := params[0], params[1]
	err := t.Store.SetClusterAliasManualOverride(clusterName, alias)
	return err
}
