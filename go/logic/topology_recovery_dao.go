/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/proxysql/golib/log"
	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/process"
	"github.com/proxysql/orchestrator/go/raft"
	"github.com/proxysql/orchestrator/go/util"
)

// AttemptFailureDetectionRegistration tries to add a failure-detection entry; if this fails that means the problem has already been detected
func (t *Tenant) AttemptFailureDetectionRegistration(analysisEntry *inst.ReplicationAnalysis) (registrationSuccessful bool, err error) {
	args := sqlutils.Args(
		analysisEntry.AnalyzedInstanceKey.Hostname,
		analysisEntry.AnalyzedInstanceKey.Port,
		process.ThisHostname,
		util.ProcessToken.Hash,
		string(analysisEntry.Analysis),
		analysisEntry.ClusterDetails.ClusterName,
		analysisEntry.ClusterDetails.ClusterAlias,
		analysisEntry.CountReplicas,
		analysisEntry.Replicas.ToCommaDelimitedList(),
		analysisEntry.IsActionableRecovery,
	)
	startActivePeriodHint := "now()"
	if analysisEntry.StartActivePeriod != "" {
		startActivePeriodHint = "?"
		args = append(args, analysisEntry.StartActivePeriod)
	}

	query := fmt.Sprintf(`
			insert ignore
				into topology_failure_detection (
					hostname,
					port,
					in_active_period,
					end_active_period_unixtime,
					processing_node_hostname,
					processcing_node_token,
					analysis,
					cluster_name,
					cluster_alias,
					count_affected_slaves,
					slave_hosts,
					is_actionable,
					start_active_period
				) values (
					?,
					?,
					1,
					0,
					?,
					?,
					?,
					?,
					?,
					?,
					?,
					?,
					%s
				)
			`, startActivePeriodHint)

	sqlResult, err := t.Backend.Exec(query, args...)
	if err != nil {
		return false, log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return false, log.Errore(err)
	}
	return (rows > 0), nil
}

// timeColumn is how a time column is compared with NOW() in this tenant's backend. SQLite compares
// text, so a value stored in a form that is not the canonical "YYYY-MM-DD HH:MM:SS" (an RFC3339
// 'T' sorts after every ' ') would never be older than anything: datetime() reads both forms.
func (t *Tenant) timeColumn(column string) string {
	if t.Backend.IsSQLite() {
		return fmt.Sprintf("datetime(%s)", column)
	}
	return column
}

// ClearActiveFailureDetections clears the "in_active_period" flag for old-enough detections, thereby allowing for
// further detections on cleared instances.
func (t *Tenant) ClearActiveFailureDetections() error {
	_, err := t.Backend.Exec(fmt.Sprintf(`
			update topology_failure_detection set
				in_active_period = 0,
				end_active_period_unixtime = UNIX_TIMESTAMP()
			where
				in_active_period = 1
				AND %s < NOW() - INTERVAL ? MINUTE
			`, t.timeColumn("start_active_period")),
		t.Cfg.FailureDetectionPeriodBlockMinutes,
	)
	return log.Errore(err)
}

// clearAcknowledgedFailureDetections clears the "in_active_period" flag for detections
// that were acknowledged
func (t *Tenant) clearAcknowledgedFailureDetections(whereClause string, args []interface{}) error {
	query := fmt.Sprintf(`
			update topology_failure_detection set
				in_active_period = 0,
				end_active_period_unixtime = UNIX_TIMESTAMP()
			where
				in_active_period = 1
				and %s
			`, whereClause)
	_, err := t.Backend.Exec(query, args...)
	return log.Errore(err)
}

// AcknowledgeInstanceFailureDetection clears a failure detection for a particular
// instance. This is automated by recovery process: it makes sense to acknowledge
// the detection of an instance just recovered.
func (t *Tenant) acknowledgeInstanceFailureDetection(instanceKey *inst.InstanceKey) error {
	whereClause := `
			hostname = ?
			and port = ?
		`
	args := sqlutils.Args(instanceKey.Hostname, instanceKey.Port)
	return t.clearAcknowledgedFailureDetections(whereClause, args)
}

// recoveryInsertStatement is the statement that inserts a topology_recovery row, and its arguments.
//
// The row records the node that processes the recovery: the one that originated it, which travels in the
// replicated entry. This node's identity is only the fallback, for the entries of a log written before
// the leader stamped them. Stamping the node that applies the row instead would make every replicated
// recovery look as if it belonged to a live node (the applying one), so that the recovery a leader
// left behind when it died would never be recognised as crashed (AcknowledgeCrashedRecoveries).
func recoveryInsertStatement(topologyRecovery *TopologyRecovery) (string, []interface{}) {
	analysisEntry := topologyRecovery.AnalysisEntry
	processingNodeHostname, processingNodeToken := topologyRecovery.ProcessingNodeHostname, topologyRecovery.ProcessingNodeToken
	if processingNodeHostname == "" || processingNodeToken == "" {
		processingNodeHostname, processingNodeToken = process.ThisHostname, util.ProcessToken.Hash
	}
	return `
			insert ignore
				into topology_recovery (
					recovery_id,
					uid,
					hostname,
					port,
					in_active_period,
					start_active_period,
					end_active_period_unixtime,
					processing_node_hostname,
					processcing_node_token,
					analysis,
					cluster_name,
					cluster_alias,
					count_affected_slaves,
					slave_hosts,
					last_detection_id
				) values (
					?,
					?,
					?,
					?,
					1,
					NOW(),
					0,
					?,
					?,
					?,
					?,
					?,
					?,
					?,
					(select ifnull(max(detection_id), 0) from topology_failure_detection where hostname=? and port=?)
				)
			`, sqlutils.Args(
			sqlutils.NilIfZero(topologyRecovery.Id),
			topologyRecovery.UID,
			analysisEntry.AnalyzedInstanceKey.Hostname, analysisEntry.AnalyzedInstanceKey.Port,
			processingNodeHostname, processingNodeToken,
			string(analysisEntry.Analysis),
			analysisEntry.ClusterDetails.ClusterName,
			analysisEntry.ClusterDetails.ClusterAlias,
			analysisEntry.CountReplicas, analysisEntry.Replicas.ToCommaDelimitedList(),
			analysisEntry.AnalyzedInstanceKey.Hostname, analysisEntry.AnalyzedInstanceKey.Port,
		)
}

// registerTopologyRecovery registers a recovery this node (the leader) starts. The unique index of the
// table (hostname, port, in_active_period, end_active_period_unixtime) is what makes a second active
// recovery of the same instance impossible: it returns nil, nil when the instance already has one.
func (t *Tenant) registerTopologyRecovery(topologyRecovery *TopologyRecovery) (*TopologyRecovery, error) {
	// this node originates the recovery: the entry that is replicated carries who it is
	topologyRecovery.ProcessingNodeHostname, topologyRecovery.ProcessingNodeToken = process.ThisHostname, util.ProcessToken.Hash
	query, args := recoveryInsertStatement(topologyRecovery)
	sqlResult, err := t.Backend.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, nil
	}
	lastInsertId, err := sqlResult.LastInsertId()
	if err != nil {
		return nil, err
	}
	topologyRecovery.Id = lastInsertId
	return topologyRecovery, nil
}

// writeTopologyRecovery applies a recovery the leader registered and replicated ("write-recovery").
//
// The leader has decided that this recovery starts, so it must land here whatever this node's own
// bookkeeping says. That bookkeeping is local: every node closes the active period of its recoveries
// (ClearActiveRecoveries) by its own clock, from the instant it applied them, so a follower can still hold
// the previous recovery of the instance as active when the next one arrives. The unique index would then
// make a plain "insert ignore" drop the new row, silently, and the nodes would differ for good. The
// instance's active recovery is therefore closed first, in the same transaction, with the end stamp
// acknowledgeRecoveries uses (never lower than the last of the instance plus one).
//
// Applying a recovery this node already has (the leader's own application of what it registered, a log
// replayed over a backend that kept its data) does nothing: it must not close what came after it.
func (t *Tenant) writeTopologyRecovery(topologyRecovery *TopologyRecovery) (*TopologyRecovery, error) {
	analysisEntry := topologyRecovery.AnalysisEntry
	instanceKey := analysisEntry.AnalyzedInstanceKey
	if topologyRecovery.UID == "" && instanceKey.Hostname == "" {
		// an empty entry (a leader that published a nil recovery, see AttemptRecoveryRegistration)
		return nil, nil
	}
	query, args := recoveryInsertStatement(topologyRecovery)
	query = t.Backend.TranslateStatement(query)

	inserted := false
	err := t.Store.ExecDBWriteFunc(func() error {
		orcdb, err := t.Backend.Open()
		if err != nil {
			return err
		}
		tx, err := orcdb.Begin()
		if err != nil {
			return err
		}
		fail := func(err error) error {
			_ = tx.Rollback()
			return err
		}

		var known int
		if topologyRecovery.UID != "" {
			err = tx.QueryRow(`select count(*) from topology_recovery where uid = ?`, topologyRecovery.UID).Scan(&known)
		} else if topologyRecovery.Id != 0 {
			err = tx.QueryRow(`select count(*) from topology_recovery where recovery_id = ?`, topologyRecovery.Id).Scan(&known)
		}
		if err != nil {
			return fail(err)
		}
		if known > 0 {
			return tx.Commit()
		}

		var activeId int64
		err = tx.QueryRow(`select recovery_id from topology_recovery where hostname = ? and port = ? and in_active_period = 1`,
			instanceKey.Hostname, instanceKey.Port).Scan(&activeId)
		switch {
		case err == sql.ErrNoRows:
		case err != nil:
			return fail(err)
		default:
			var lastEnd int64
			if err := tx.QueryRow(`select IFNULL(max(end_active_period_unixtime), 0) from topology_recovery where hostname = ? and port = ? and in_active_period = 0`,
				instanceKey.Hostname, instanceKey.Port).Scan(&lastEnd); err != nil {
				return fail(err)
			}
			end := time.Now().Unix()
			if end <= lastEnd {
				end = lastEnd + 1
			}
			if _, err := tx.Exec(`update topology_recovery set in_active_period = 0, end_active_period_unixtime = ? where recovery_id = ?`, end, activeId); err != nil {
				return fail(err)
			}
		}

		sqlResult, err := tx.Exec(query, args...)
		if err != nil {
			return fail(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return fail(err)
		}
		if rows == 0 {
			return fail(fmt.Errorf("recovery %s of %+v was not written: its id %d is taken", topologyRecovery.UID, instanceKey, topologyRecovery.Id))
		}
		inserted = true
		return tx.Commit()
	})
	if err != nil {
		return nil, err
	}
	if !inserted {
		return nil, nil
	}
	return topologyRecovery, nil
}

// AttemptRecoveryRegistration tries to add a recovery entry; if this fails that means recovery is already in place.
func (t *Tenant) AttemptRecoveryRegistration(analysisEntry *inst.ReplicationAnalysis, failIfFailedInstanceInActiveRecovery bool, failIfClusterInActiveRecovery bool) (*TopologyRecovery, error) {
	if failIfFailedInstanceInActiveRecovery {
		// Let's check if this instance has just been promoted recently and is still in active period.
		// If so, we reject recovery registration to avoid flapping.
		recoveries, err := t.ReadInActivePeriodSuccessorInstanceRecovery(&analysisEntry.AnalyzedInstanceKey)
		if err != nil {
			return nil, log.Errore(err)
		}
		if len(recoveries) > 0 {
			t.RegisterBlockedRecoveries(analysisEntry, recoveries)
			return nil, log.Errorf("AttemptRecoveryRegistration: instance %+v has recently been promoted (by failover of %+v) and is in active period. It will not be failed over. You may acknowledge the failure on %+v (-c ack-instance-recoveries) to remove this blockage", analysisEntry.AnalyzedInstanceKey, recoveries[0].AnalysisEntry.AnalyzedInstanceKey, recoveries[0].AnalysisEntry.AnalyzedInstanceKey)
		}
	}
	if failIfClusterInActiveRecovery {
		// Let's check if this cluster has just experienced a failover and is still in active period.
		// If so, we reject recovery registration to avoid flapping.
		recoveries, err := t.ReadInActivePeriodClusterRecovery(analysisEntry.ClusterDetails.ClusterName)
		if err != nil {
			return nil, log.Errore(err)
		}
		if len(recoveries) > 0 {
			t.RegisterBlockedRecoveries(analysisEntry, recoveries)
			return nil, log.Errorf("AttemptRecoveryRegistration: cluster %+v has recently experienced a failover (of %+v) and is in active period. It will not be failed over again. You may acknowledge the failure on this cluster (-c ack-cluster-recoveries) or on %+v (-c ack-instance-recoveries) to remove this blockage", analysisEntry.ClusterDetails.ClusterName, recoveries[0].AnalysisEntry.AnalyzedInstanceKey, recoveries[0].AnalysisEntry.AnalyzedInstanceKey)
		}
	}
	if !failIfFailedInstanceInActiveRecovery {
		// Implicitly acknowledge this instance's possibly existing active recovery, provided they are completed.
		t.AcknowledgeInstanceCompletedRecoveries(&analysisEntry.AnalyzedInstanceKey, "orchestrator", fmt.Sprintf("implicit acknowledge due to user invocation of recovery on same instance: %+v", analysisEntry.AnalyzedInstanceKey))
		// The fact we only acknowledge a completed recovery solves the possible case of two DBAs simultaneously
		// trying to recover the same instance at the same time
	}

	topologyRecovery := NewTopologyRecovery(*analysisEntry)

	topologyRecovery, err := t.registerTopologyRecovery(topologyRecovery)
	if err != nil {
		return nil, log.Errore(err)
	}
	if topologyRecovery == nil {
		// the instance already has an active recovery: nothing was registered, nothing to replicate
		return nil, nil
	}
	if orcraft.IsRaftEnabled() {
		if _, err := orcraft.PublishTenantCommand(t.ID, "write-recovery", topologyRecovery); err != nil {
			return nil, log.Errore(err)
		}
	}
	return topologyRecovery, nil
}

// ClearActiveRecoveries clears the "in_active_period" flag for old-enough recoveries, thereby allowing for
// further recoveries on cleared instances.
func (t *Tenant) ClearActiveRecoveries() error {
	_, err := t.Backend.Exec(fmt.Sprintf(`
			update topology_recovery set
				in_active_period = 0,
				end_active_period_unixtime = UNIX_TIMESTAMP()
			where
				in_active_period = 1
				AND %s < NOW() - INTERVAL ? SECOND
			`, t.timeColumn("start_active_period")),
		t.Cfg.RecoveryPeriodBlockSeconds,
	)
	return log.Errore(err)
}

// RegisterBlockedRecoveries writes down currently blocked recoveries, and indicates what recovery they are blocked on.
// Recoveries are blocked through the in_active_period flag, which comes to avoid flapping.
func (t *Tenant) RegisterBlockedRecoveries(analysisEntry *inst.ReplicationAnalysis, blockingRecoveries []*TopologyRecovery) error {
	for _, recovery := range blockingRecoveries {
		_, err := t.Backend.Exec(`
			insert
				into blocked_topology_recovery (
					hostname,
					port,
					cluster_name,
					analysis,
					last_blocked_timestamp,
					blocking_recovery_id
				) values (
					?,
					?,
					?,
					?,
					NOW(),
					?
				)
				on duplicate key update
					cluster_name=values(cluster_name),
					analysis=values(analysis),
					last_blocked_timestamp=values(last_blocked_timestamp),
					blocking_recovery_id=values(blocking_recovery_id)
			`, analysisEntry.AnalyzedInstanceKey.Hostname,
			analysisEntry.AnalyzedInstanceKey.Port,
			analysisEntry.ClusterDetails.ClusterName,
			string(analysisEntry.Analysis),
			recovery.Id,
		)
		if err != nil {
			log.Errore(err)
		}
	}
	return nil
}

// ExpireBlockedRecoveries clears listing of blocked recoveries that are no longer actually blocked.
func (t *Tenant) ExpireBlockedRecoveries() error {
	// Older recovery is acknowledged by now, hence blocked recovery should be released.
	// Do NOTE that the data in blocked_topology_recovery is only used for auditing: it is NOT the data
	// based on which we make automated decisions.

	query := `
		select
				blocked_topology_recovery.hostname,
				blocked_topology_recovery.port
			from
				blocked_topology_recovery
				left join topology_recovery on (blocking_recovery_id = topology_recovery.recovery_id and acknowledged = 0)
			where
				acknowledged is null
		`
	expiredKeys := inst.NewInstanceKeyMap()
	err := t.Backend.Query(query, sqlutils.Args(), func(m sqlutils.RowMap) error {
		key := inst.InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		expiredKeys.AddKey(key)
		return nil
	})

	for _, expiredKey := range expiredKeys.GetInstanceKeys() {
		_, err := t.Backend.Exec(`
				delete
					from blocked_topology_recovery
				where
						hostname = ?
						and port = ?
				`,
			expiredKey.Hostname, expiredKey.Port,
		)
		if err != nil {
			return log.Errore(err)
		}
	}

	if err != nil {
		return log.Errore(err)
	}
	// Some oversampling, if a problem has not been noticed for some time (e.g. the server came up alive
	// before action was taken), expire it.
	// Recall that RegisterBlockedRecoveries continuously updates the last_blocked_timestamp column.
	_, err = t.Backend.Exec(`
			delete
				from blocked_topology_recovery
				where
					last_blocked_timestamp < NOW() - interval ? second
			`, (config.RecoveryPollSeconds * 2),
	)
	return log.Errore(err)
}

// acknowledgeRecoveries sets acknowledged* details and clears the in_active_period flags from a set of entries.
//
// Closing the active period of a recovery stamps end_active_period_unixtime, which is part of the unique
// index (hostname, port, in_active_period, end_active_period_unixtime) of topology_recovery. Stamping it
// with the second of the node's clock made the outcome depend on that clock: two recoveries of an instance
// acknowledged within the same second collided on the node that applied them in that second, and not on a
// node that applied them in different seconds. The stamp is now never lower than the last one of the
// instance plus one, so an acknowledgement does not fail whatever second it is applied in, on any node.
func (t *Tenant) acknowledgeRecoveries(owner string, comment string, opts ackOptions, whereClause string, args []interface{}) (countAcknowledgedEntries int64, err error) {
	// the instant of the acknowledgement: the one the leader stamped in the entry, so that every node writes
	// the same (acknowledged_at, end_recovery); the clock of the node only when the entry carries none
	nowExpr, nowArgs := "NOW()", []interface{}{}
	if !opts.at.IsZero() {
		nowExpr, nowArgs = "?", []interface{}{t.backendTimestamp(opts.at)}
	}
	additionalSet, endRecoveryArgs := ``, []interface{}{}
	if opts.markEndRecovery {
		additionalSet = fmt.Sprintf(`
				end_recovery=IFNULL(end_recovery, %s),
			`, nowExpr)
		endRecoveryArgs = nowArgs
	}
	selectQuery := t.Backend.TranslateStatement(fmt.Sprintf(`
			select
				recovery_id, hostname, port, end_active_period_unixtime
			from
				topology_recovery
			where
				acknowledged = 0
				and
				%s
		`, whereClause))
	updateQuery := t.Backend.TranslateStatement(fmt.Sprintf(`
			update topology_recovery set
				in_active_period = 0,
				end_active_period_unixtime = ?,
				%s
				acknowledged = 1,
				acknowledged_at = %s,
				acknowledged_by = ?,
				acknowledge_comment = ?
			where
				recovery_id = ?
		`, additionalSet, nowExpr))
	lastEndQuery := `
			select
				IFNULL(max(end_active_period_unixtime), 0) as last_end
			from
				topology_recovery
			where
				hostname = ? and port = ? and in_active_period = 0
		`

	err = t.Store.ExecDBWriteFunc(func() error {
		orcdb, err := t.Backend.Open()
		if err != nil {
			return err
		}
		tx, err := orcdb.Begin()
		if err != nil {
			return err
		}
		type target struct {
			id       int64
			hostname string
			port     int
			end      int64
		}
		var targets []target
		rows, err := tx.Query(selectQuery, args...)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		for rows.Next() {
			var tg target
			var end sql.NullInt64
			if err := rows.Scan(&tg.id, &tg.hostname, &tg.port, &end); err != nil {
				_ = rows.Close()
				_ = tx.Rollback()
				return err
			}
			tg.end = end.Int64
			targets = append(targets, tg)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			return err
		}
		_ = rows.Close()

		var acknowledged int64
		now := opts.at.Unix()
		if opts.at.IsZero() {
			now = time.Now().Unix()
		}
		for _, tg := range targets {
			end := tg.end
			if end == 0 {
				var lastEnd int64
				if err := tx.QueryRow(lastEndQuery, tg.hostname, tg.port).Scan(&lastEnd); err != nil {
					_ = tx.Rollback()
					return err
				}
				end = now
				if end <= lastEnd {
					end = lastEnd + 1
				}
			}
			updateArgs := append([]interface{}{end}, endRecoveryArgs...)
			updateArgs = append(updateArgs, nowArgs...)
			updateArgs = append(updateArgs, owner, comment, tg.id)
			result, err := tx.Exec(updateQuery, updateArgs...)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			n, err := result.RowsAffected()
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			acknowledged += n
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		countAcknowledgedEntries = acknowledged
		return nil
	})
	if err != nil {
		return 0, log.Errore(err)
	}
	return countAcknowledgedEntries, nil
}

// ackOptions are the two things an acknowledgement can carry from the leader that decided it.
type ackOptions struct {
	// at is the instant of the acknowledgement; zero means the clock of this node
	at time.Time
	// markEndRecovery also stamps end_recovery, for the recoveries that will never finish
	markEndRecovery bool
}

// ackOptionsOf are the options of an acknowledgement that came through Raft.
func ackOptionsOf(ack *RecoveryAcknowledgement) ackOptions {
	return ackOptions{at: ack.CreatedAt, markEndRecovery: ack.EndRecovery}
}

// backendTimestamp renders an instant the way NOW() of this tenant's backend does (UTC in SQLite).
func (t *Tenant) backendTimestamp(at time.Time) string {
	if t.Backend.IsSQLite() {
		at = at.UTC()
	}
	return at.Format("2006-01-02 15:04:05")
}

// AcknowledgeAllRecoveries acknowledges all unacknowledged recoveries.
func (t *Tenant) AcknowledgeAllRecoveries(owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return t.acknowledgeAllRecoveries(ackOptions{}, owner, comment)
}

func (t *Tenant) acknowledgeAllRecoveries(opts ackOptions, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `1 = 1`
	return t.acknowledgeRecoveries(owner, comment, opts, whereClause, sqlutils.Args())
}

// AcknowledgeRecovery acknowledges a particular recovery.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func (t *Tenant) AcknowledgeRecovery(recoveryId int64, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return t.acknowledgeRecovery(ackOptions{}, recoveryId, owner, comment)
}

func (t *Tenant) acknowledgeRecovery(opts ackOptions, recoveryId int64, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `recovery_id = ?`
	return t.acknowledgeRecoveries(owner, comment, opts, whereClause, sqlutils.Args(recoveryId))
}

// AcknowledgeRecoveryByUID acknowledges a particular recovery.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func (t *Tenant) AcknowledgeRecoveryByUID(recoveryUID string, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return t.acknowledgeRecoveryByUID(ackOptions{}, recoveryUID, owner, comment)
}

func (t *Tenant) acknowledgeRecoveryByUID(opts ackOptions, recoveryUID string, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `uid = ?`
	return t.acknowledgeRecoveries(owner, comment, opts, whereClause, sqlutils.Args(recoveryUID))
}

// AcknowledgeClusterRecoveries marks active recoveries for given cluster as acknowledged.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func (t *Tenant) AcknowledgeClusterRecoveries(clusterName string, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return t.acknowledgeClusterRecoveries(ackOptions{}, clusterName, owner, comment)
}

func (t *Tenant) acknowledgeClusterRecoveries(opts ackOptions, clusterName string, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	{
		whereClause := `cluster_name = ?`
		args := sqlutils.Args(clusterName)
		t.clearAcknowledgedFailureDetections(whereClause, args)
		count, err := t.acknowledgeRecoveries(owner, comment, opts, whereClause, args)
		if err != nil {
			return count, err
		}
		countAcknowledgedEntries = countAcknowledgedEntries + count
	}
	{
		clusterInfo, err := t.Store.ReadClusterInfo(clusterName)
		if err != nil {
			return 0, err
		}
		whereClause := `cluster_alias = ? and cluster_alias != ''`
		args := sqlutils.Args(clusterInfo.ClusterAlias)
		t.clearAcknowledgedFailureDetections(whereClause, args)
		count, err := t.acknowledgeRecoveries(owner, comment, opts, whereClause, args)
		if err != nil {
			return count, err
		}
		countAcknowledgedEntries = countAcknowledgedEntries + count

	}
	return countAcknowledgedEntries, nil
}

// AcknowledgeInstanceRecoveries marks active recoveries for given instance as acknowledged.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func (t *Tenant) AcknowledgeInstanceRecoveries(instanceKey *inst.InstanceKey, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	return t.acknowledgeInstanceRecoveries(ackOptions{}, instanceKey, owner, comment)
}

func (t *Tenant) acknowledgeInstanceRecoveries(opts ackOptions, instanceKey *inst.InstanceKey, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `
			hostname = ?
			and port = ?
		`
	args := sqlutils.Args(instanceKey.Hostname, instanceKey.Port)
	t.clearAcknowledgedFailureDetections(whereClause, args)
	return t.acknowledgeRecoveries(owner, comment, opts, whereClause, args)
}

// AcknowledgeInstanceCompletedRecoveries marks active and COMPLETED recoveries for given instance as acknowledged.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func (t *Tenant) AcknowledgeInstanceCompletedRecoveries(instanceKey *inst.InstanceKey, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `
			hostname = ?
			and port = ?
			and end_recovery is not null
		`
	return t.acknowledgeRecoveries(owner, comment, ackOptions{}, whereClause, sqlutils.Args(instanceKey.Hostname, instanceKey.Port))
}

// crashedRecoveriesWhere selects the active recoveries that are not finished and whose processing node is
// not in node_health.
const crashedRecoveriesWhere = `
			in_active_period = 1
			and end_recovery is null
			and concat(processing_node_hostname, ':', processcing_node_token) not in (
				select concat(hostname, ':', token) from node_health
			)
		`

// crashedRecoveryComment and crashedRecoveryOwner are what the rows of an acknowledged crashed recovery say.
const (
	crashedRecoveryOwner   = "orchestrator"
	crashedRecoveryComment = "detected crashed recovery"
)

// recoveryPublish is how a recovery acknowledgement reaches the other nodes. A seam for the tests.
var recoveryPublish = orcraft.PublishTenantCommand

// CrashedRecoveryAcknowledgements are the "ack-recovery" entries that acknowledge the recoveries whose
// processing node has crashed, one per recovery, all stamped with the same instant. The leader publishes
// them (see AcknowledgeCrashedRecoveries); a node that is not the leader has none.
func (t *Tenant) CrashedRecoveryAcknowledgements() ([]*RecoveryAcknowledgement, error) {
	if !t.isLeader() {
		return nil, nil
	}
	var acks []*RecoveryAcknowledgement
	now := time.Now()
	err := t.Backend.Query(t.Backend.TranslateStatement(`
			select recovery_id, uid
			from topology_recovery
			where acknowledged = 0 and `+crashedRecoveriesWhere+`
			order by recovery_id
		`), sqlutils.Args(), func(m sqlutils.RowMap) error {
		ack := &RecoveryAcknowledgement{CreatedAt: now, Owner: crashedRecoveryOwner, Comment: crashedRecoveryComment, EndRecovery: true}
		if uid := m.GetString("uid"); uid != "" {
			ack.UID = uid
		} else {
			ack.Id = m.GetInt64("recovery_id")
		}
		acks = append(acks, ack)
		return nil
	})
	return acks, log.Errore(err)
}

// AcknowledgeCrashedRecoveries marks recoveries whose processing nodes has crashed as acknowledged.
//
// node_health only knows the nodes this node heard from (each node has its own, and itself), and a
// replicated recovery carries the node that originated it: a recovery of any other node than the one
// that acknowledges is orphaned. Only the leader may acknowledge them: it is the one that needs the
// instance released. Without Raft the leader is the elected node, which is who runs the recovery loop
// anyway, and the acknowledgement is local. With Raft it is replicated ("ack-recovery", stamped by the
// leader), so that every node ends with the same acknowledged rows and none of them keeps an orphan
// recovery that the leader closed.
func (t *Tenant) AcknowledgeCrashedRecoveries() (countAcknowledgedEntries int64, err error) {
	if !t.isLeader() {
		return 0, nil
	}
	if recoveryRaftEnabled() {
		acks, err := t.CrashedRecoveryAcknowledgements()
		if err != nil {
			return 0, err
		}
		for _, ack := range acks {
			if _, err := recoveryPublish(t.ID, "ack-recovery", ack); err != nil {
				return countAcknowledgedEntries, log.Errore(err)
			}
			countAcknowledgedEntries++
		}
		return countAcknowledgedEntries, nil
	}
	return t.acknowledgeRecoveries(crashedRecoveryOwner, crashedRecoveryComment, ackOptions{markEndRecovery: true}, crashedRecoveriesWhere, sqlutils.Args())
}

// ResolveRecovery is called on completion of a recovery process and updates the recovery status.
// It does not clear the "active period" as this still takes place in order to avoid flapping.
func (t *Tenant) writeResolveRecovery(topologyRecovery *TopologyRecovery) error {
	var successorKeyToWrite inst.InstanceKey
	if topologyRecovery.IsSuccessful {
		successorKeyToWrite = *topologyRecovery.SuccessorKey
	}
	_, err := t.Backend.Exec(`
			update topology_recovery set
				is_successful = ?,
				successor_hostname = ?,
				successor_port = ?,
				successor_alias = ?,
				lost_slaves = ?,
				participating_instances = ?,
				all_errors = ?,
				end_recovery = NOW()
			where
				uid = ?
			`, topologyRecovery.IsSuccessful, successorKeyToWrite.Hostname, successorKeyToWrite.Port,
		topologyRecovery.SuccessorAlias, topologyRecovery.LostReplicas.ToCommaDelimitedList(),
		topologyRecovery.ParticipatingInstanceKeys.ToCommaDelimitedList(),
		strings.Join(topologyRecovery.AllErrors, "\n"),
		topologyRecovery.UID,
	)
	return log.Errore(err)
}

// readRecoveries reads recovery entry/audit entries from topology_recovery
func (t *Tenant) readRecoveries(whereCondition string, limit string, args []interface{}) ([]*TopologyRecovery, error) {
	res := []*TopologyRecovery{}
	query := fmt.Sprintf(`
		select
      recovery_id,
			uid,
      hostname,
      port,
      (IFNULL(end_active_period_unixtime, 0) = 0) as is_active,
      start_active_period,
      IFNULL(end_active_period_unixtime, 0) as end_active_period_unixtime,
      IFNULL(end_recovery, '') AS end_recovery,
      is_successful,
      processing_node_hostname,
      processcing_node_token,
      ifnull(successor_hostname, '') as successor_hostname,
      ifnull(successor_port, 0) as successor_port,
      ifnull(successor_alias, '') as successor_alias,
      analysis,
      cluster_name,
      cluster_alias,
      count_affected_slaves,
      slave_hosts,
      participating_instances,
      lost_slaves,
      all_errors,
      acknowledged,
      acknowledged_at,
      acknowledged_by,
      acknowledge_comment,
      last_detection_id
		from
			topology_recovery
		%s
		order by
			recovery_id desc
		%s
		`, whereCondition, limit)
	err := t.Backend.Query(query, args, func(m sqlutils.RowMap) error {
		topologyRecovery := *NewTopologyRecovery(inst.ReplicationAnalysis{})
		topologyRecovery.Id = m.GetInt64("recovery_id")
		topologyRecovery.UID = m.GetString("uid")

		topologyRecovery.IsActive = m.GetBool("is_active")
		topologyRecovery.RecoveryStartTimestamp = m.GetString("start_active_period")
		topologyRecovery.RecoveryEndTimestamp = m.GetString("end_recovery")
		topologyRecovery.IsSuccessful = m.GetBool("is_successful")
		topologyRecovery.ProcessingNodeHostname = m.GetString("processing_node_hostname")
		topologyRecovery.ProcessingNodeToken = m.GetString("processcing_node_token")

		topologyRecovery.AnalysisEntry.AnalyzedInstanceKey.Hostname = m.GetString("hostname")
		topologyRecovery.AnalysisEntry.AnalyzedInstanceKey.Port = m.GetInt("port")
		topologyRecovery.AnalysisEntry.Analysis = inst.AnalysisCode(m.GetString("analysis"))
		topologyRecovery.AnalysisEntry.ClusterDetails.ClusterName = m.GetString("cluster_name")
		topologyRecovery.AnalysisEntry.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		topologyRecovery.AnalysisEntry.CountReplicas = m.GetUint("count_affected_slaves")
		_ = t.Store.ReplicationAnalysisReadReplicaHostsFromString(&topologyRecovery.AnalysisEntry, m.GetString("slave_hosts"))

		topologyRecovery.SuccessorKey = &inst.InstanceKey{}
		topologyRecovery.SuccessorKey.Hostname = m.GetString("successor_hostname")
		topologyRecovery.SuccessorKey.Port = m.GetInt("successor_port")
		topologyRecovery.SuccessorAlias = m.GetString("successor_alias")

		t.Store.ClusterInfoReadRecoveryInfo(&topologyRecovery.AnalysisEntry.ClusterDetails)

		topologyRecovery.AllErrors = strings.Split(m.GetString("all_errors"), "\n")
		_ = t.Store.InstanceKeyMapReadCommaDelimitedList(&topologyRecovery.LostReplicas, m.GetString("lost_slaves"))
		_ = t.Store.InstanceKeyMapReadCommaDelimitedList(&topologyRecovery.ParticipatingInstanceKeys, m.GetString("participating_instances"))

		topologyRecovery.Acknowledged = m.GetBool("acknowledged")
		topologyRecovery.AcknowledgedAt = m.GetString("acknowledged_at")
		topologyRecovery.AcknowledgedBy = m.GetString("acknowledged_by")
		topologyRecovery.AcknowledgedComment = m.GetString("acknowledge_comment")

		topologyRecovery.LastDetectionId = m.GetInt64("last_detection_id")

		res = append(res, &topologyRecovery)
		return nil
	})

	return res, log.Errore(err)
}

// ReadActiveRecoveries reads active recovery entry/audit entries from topology_recovery
func (t *Tenant) ReadActiveClusterRecovery(clusterName string) ([]*TopologyRecovery, error) {
	whereClause := `
		where
			in_active_period=1
			and end_recovery is null
			and cluster_name=?`
	return t.readRecoveries(whereClause, ``, sqlutils.Args(clusterName))
}

// ReadInActivePeriodClusterRecovery reads recoveries (possibly complete!) that are in active period.
// (may be used to block further recoveries on this cluster)
func (t *Tenant) ReadInActivePeriodClusterRecovery(clusterName string) ([]*TopologyRecovery, error) {
	whereClause := `
		where
			in_active_period=1
			and cluster_name=?`
	return t.readRecoveries(whereClause, ``, sqlutils.Args(clusterName))
}

// ReadRecentlyActiveClusterRecovery reads recently completed entries for a given cluster
func (t *Tenant) ReadRecentlyActiveClusterRecovery(clusterName string) ([]*TopologyRecovery, error) {
	whereClause := `
		where
			end_recovery > now() - interval 5 minute
			and cluster_name=?`
	return t.readRecoveries(whereClause, ``, sqlutils.Args(clusterName))
}

// ReadInActivePeriodSuccessorInstanceRecovery reads completed recoveries for a given instance, where said instance
// was promoted as result, still in active period (may be used to block further recoveries should this instance die)
func (t *Tenant) ReadInActivePeriodSuccessorInstanceRecovery(instanceKey *inst.InstanceKey) ([]*TopologyRecovery, error) {
	whereClause := `
		where
			in_active_period=1
			and
				successor_hostname=? and successor_port=?`
	return t.readRecoveries(whereClause, ``, sqlutils.Args(instanceKey.Hostname, instanceKey.Port))
}

// ReadRecentlyActiveInstanceRecovery reads recently completed entries for a given instance
func (t *Tenant) ReadRecentlyActiveInstanceRecovery(instanceKey *inst.InstanceKey) ([]*TopologyRecovery, error) {
	whereClause := `
		where
			end_recovery > now() - interval 5 minute
			and
				successor_hostname=? and successor_port=?`
	return t.readRecoveries(whereClause, ``, sqlutils.Args(instanceKey.Hostname, instanceKey.Port))
}

// ReadActiveRecoveries reads active recovery entry/audit entries from topology_recovery
func (t *Tenant) ReadActiveRecoveries() ([]*TopologyRecovery, error) {
	return t.readRecoveries(`
		where
			in_active_period=1
			and end_recovery is null`,
		``, sqlutils.Args())
}

// ReadCompletedRecoveries reads completed recovery entry/audit entries from topology_recovery
func (t *Tenant) ReadCompletedRecoveries(page int) ([]*TopologyRecovery, error) {
	limit := `
		limit ?
		offset ?`
	return t.readRecoveries(`where end_recovery is not null`, limit, sqlutils.Args(config.AuditPageSize, page*config.AuditPageSize))
}

// ReadRecovery reads completed recovery entry/audit entries from topology_recovery
func (t *Tenant) ReadRecovery(recoveryId int64) ([]*TopologyRecovery, error) {
	whereClause := `where recovery_id = ?`
	return t.readRecoveries(whereClause, ``, sqlutils.Args(recoveryId))
}

// ReadRecoveryByUID reads completed recovery entry/audit entries from topology_recovery
func (t *Tenant) ReadRecoveryByUID(recoveryUID string) ([]*TopologyRecovery, error) {
	whereClause := `where uid = ?`
	return t.readRecoveries(whereClause, ``, sqlutils.Args(recoveryUID))
}

// ReadCRecoveries reads latest recovery entries from topology_recovery
func (t *Tenant) ReadRecentRecoveries(clusterName string, clusterAlias string, unacknowledgedOnly bool, page int) ([]*TopologyRecovery, error) {
	whereConditions := []string{}
	whereClause := ""
	args := sqlutils.Args()
	if unacknowledgedOnly {
		whereConditions = append(whereConditions, `acknowledged=0`)
	}
	if clusterName != "" {
		whereConditions = append(whereConditions, `cluster_name=?`)
		args = append(args, clusterName)
	} else if clusterAlias != "" {
		whereConditions = append(whereConditions, `cluster_alias=?`)
		args = append(args, clusterAlias)
	}
	if len(whereConditions) > 0 {
		whereClause = fmt.Sprintf("where %s", strings.Join(whereConditions, " and "))
	}
	limit := `
		limit ?
		offset ?`
	args = append(args, config.AuditPageSize, page*config.AuditPageSize)
	return t.readRecoveries(whereClause, limit, args)
}

// readRecoveries reads recovery entry/audit entries from topology_recovery
func (t *Tenant) readFailureDetections(whereCondition string, limit string, args []interface{}) ([]*TopologyRecovery, error) {
	res := []*TopologyRecovery{}
	query := fmt.Sprintf(`
		select
      detection_id,
      hostname,
      port,
      in_active_period as is_active,
      start_active_period,
      end_active_period_unixtime,
      processing_node_hostname,
      processcing_node_token,
      analysis,
      cluster_name,
      cluster_alias,
      count_affected_slaves,
      slave_hosts,
      (select max(recovery_id) from topology_recovery where topology_recovery.last_detection_id = detection_id) as related_recovery_id
		from
			topology_failure_detection
		%s
		order by
			detection_id desc
		%s
		`, whereCondition, limit)
	err := t.Backend.Query(query, args, func(m sqlutils.RowMap) error {
		failureDetection := TopologyRecovery{}
		failureDetection.Id = m.GetInt64("detection_id")

		failureDetection.IsActive = m.GetBool("is_active")
		failureDetection.RecoveryStartTimestamp = m.GetString("start_active_period")
		failureDetection.ProcessingNodeHostname = m.GetString("processing_node_hostname")
		failureDetection.ProcessingNodeToken = m.GetString("processcing_node_token")

		failureDetection.AnalysisEntry.AnalyzedInstanceKey.Hostname = m.GetString("hostname")
		failureDetection.AnalysisEntry.AnalyzedInstanceKey.Port = m.GetInt("port")
		failureDetection.AnalysisEntry.Analysis = inst.AnalysisCode(m.GetString("analysis"))
		failureDetection.AnalysisEntry.ClusterDetails.ClusterName = m.GetString("cluster_name")
		failureDetection.AnalysisEntry.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		failureDetection.AnalysisEntry.CountReplicas = m.GetUint("count_affected_slaves")
		_ = t.Store.ReplicationAnalysisReadReplicaHostsFromString(&failureDetection.AnalysisEntry, m.GetString("slave_hosts"))
		failureDetection.AnalysisEntry.StartActivePeriod = m.GetString("start_active_period")

		failureDetection.RelatedRecoveryId = m.GetInt64("related_recovery_id")

		t.Store.ClusterInfoReadRecoveryInfo(&failureDetection.AnalysisEntry.ClusterDetails)

		res = append(res, &failureDetection)
		return nil
	})

	return res, log.Errore(err)
}

// ReadRecentFailureDetections
func (t *Tenant) ReadRecentFailureDetections(clusterAlias string, page int) ([]*TopologyRecovery, error) {
	whereClause := ""
	args := sqlutils.Args()
	if clusterAlias != "" {
		whereClause = `where cluster_alias = ?`
		args = append(args, clusterAlias)
	}
	limit := `
		limit ?
		offset ?`
	args = append(args, config.AuditPageSize, page*config.AuditPageSize)
	return t.readFailureDetections(whereClause, limit, args)
}

// ReadFailureDetection
func (t *Tenant) ReadFailureDetection(detectionId int64) ([]*TopologyRecovery, error) {
	whereClause := `where detection_id = ?`
	return t.readFailureDetections(whereClause, ``, sqlutils.Args(detectionId))
}

// ReadBlockedRecoveries reads blocked recovery entries, potentially filtered by cluster name (empty to unfilter)
func (t *Tenant) ReadBlockedRecoveries(clusterName string) ([]BlockedTopologyRecovery, error) {
	res := []BlockedTopologyRecovery{}
	whereClause := ""
	args := sqlutils.Args()
	if clusterName != "" {
		whereClause = `where cluster_name = ?`
		args = append(args, clusterName)
	}
	query := fmt.Sprintf(`
		select
				hostname,
				port,
				cluster_name,
				analysis,
				last_blocked_timestamp,
				blocking_recovery_id
			from
				blocked_topology_recovery
			%s
			order by
				last_blocked_timestamp desc
		`, whereClause)
	err := t.Backend.Query(query, args, func(m sqlutils.RowMap) error {
		blockedTopologyRecovery := BlockedTopologyRecovery{}
		blockedTopologyRecovery.FailedInstanceKey.Hostname = m.GetString("hostname")
		blockedTopologyRecovery.FailedInstanceKey.Port = m.GetInt("port")
		blockedTopologyRecovery.ClusterName = m.GetString("cluster_name")
		blockedTopologyRecovery.Analysis = inst.AnalysisCode(m.GetString("analysis"))
		blockedTopologyRecovery.LastBlockedTimestamp = m.GetString("last_blocked_timestamp")
		blockedTopologyRecovery.BlockingRecoveryId = m.GetInt64("blocking_recovery_id")

		res = append(res, blockedTopologyRecovery)
		return nil
	})

	return res, log.Errore(err)
}

// writeTopologyRecoveryStep writes down a single step in a recovery process.
//
// A step that comes with its AuditAt (the leader stamps it when it publishes the step) is written
// at most once: Raft re-applies log entries after a restart over a backend that kept its data, and
// the same step (recovery, instant, message) is not written again. A step without AuditAt (written
// without Raft, or an entry of a log of a version that did not stamp it) has no natural key and is
// always inserted, with the clock of the node.
func (t *Tenant) writeTopologyRecoveryStep(topologyRecoveryStep *TopologyRecoveryStep) error {
	var sqlResult sql.Result
	var err error
	if topologyRecoveryStep.AuditAt == "" {
		sqlResult, err = t.Backend.Exec(`
			insert ignore
				into topology_recovery_steps (
					recovery_step_id, recovery_uid, audit_at, message
				) values (?, ?, now(), ?)
			`, sqlutils.NilIfZero(topologyRecoveryStep.Id), topologyRecoveryStep.RecoveryUID, topologyRecoveryStep.Message,
		)
	} else {
		// (select 1) as d: a SELECT with a WHERE needs a FROM clause in MySQL, and this is valid in SQLite too
		sqlResult, err = t.Backend.Exec(`
			insert ignore
				into topology_recovery_steps (
					recovery_step_id, recovery_uid, audit_at, message
				)
				select ?, ?, ?, ?
				from (select 1) as d
				where not exists (
					select 1
						from topology_recovery_steps
						where recovery_uid = ? and audit_at = ? and message = ?
				)
			`, sqlutils.NilIfZero(topologyRecoveryStep.Id), topologyRecoveryStep.RecoveryUID, topologyRecoveryStep.AuditAt, topologyRecoveryStep.Message,
			topologyRecoveryStep.RecoveryUID, topologyRecoveryStep.AuditAt, topologyRecoveryStep.Message,
		)
	}
	if err != nil {
		return log.Errore(err)
	}
	if rows, err := sqlResult.RowsAffected(); err == nil && rows == 0 {
		return nil // already there
	}
	topologyRecoveryStep.Id, err = sqlResult.LastInsertId()
	return log.Errore(err)
}

// ReadTopologyRecoverySteps reads recovery steps for a given recovery
func (t *Tenant) ReadTopologyRecoverySteps(recoveryUID string) ([]TopologyRecoveryStep, error) {
	res := []TopologyRecoveryStep{}
	query := `
		select
			recovery_step_id, recovery_uid, audit_at, message
		from
			topology_recovery_steps
		where
			recovery_uid=?
		order by
			recovery_step_id asc
		`
	err := t.Backend.Query(query, sqlutils.Args(recoveryUID), func(m sqlutils.RowMap) error {
		recoveryStep := TopologyRecoveryStep{}
		recoveryStep.RecoveryUID = recoveryUID
		recoveryStep.Id = m.GetInt64("recovery_step_id")
		recoveryStep.AuditAt = m.GetString("audit_at")
		recoveryStep.Message = m.GetString("message")

		res = append(res, recoveryStep)
		return nil
	})
	return res, log.Errore(err)
}

// ExpireFailureDetectionHistory removes old rows from the topology_failure_detection table
func (t *Tenant) ExpireFailureDetectionHistory() error {
	return t.Store.ExpireTableData("topology_failure_detection", "start_active_period")
}

// ExpireTopologyRecoveryHistory removes old rows from the topology_failure_detection table
func (t *Tenant) ExpireTopologyRecoveryHistory() error {
	return t.Store.ExpireTableData("topology_recovery", "start_active_period")
}

// ExpireTopologyRecoveryStepsHistory removes old rows from the topology_failure_detection table
func (t *Tenant) ExpireTopologyRecoveryStepsHistory() error {
	return t.Store.ExpireTableData("topology_recovery_steps", "audit_at")
}
