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

package process

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/proxysql/golib/log"
	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/db"
)

var (
	errHeartbeatNotRegistered = errors.New("heartbeat was not registered")
	errMirrorBusy             = errors.New("previous heartbeat write is still running")
	errMirrorTimeout          = errors.New("heartbeat write timed out")
)

// writeRegisterNode writes the heartbeat of nodeHealth into the backend of t.
func writeRegisterNode(t *target, nodeHealth *NodeHealth) (healthy bool, err error) {
	b := t.backend
	timeNow := time.Now()
	reportedAgo := timeNow.Sub(nodeHealth.LastReported)
	reportedSecondsAgo := int64(reportedAgo.Seconds())
	if reportedSecondsAgo > config.HealthPollSeconds*2 {
		// This entry is too old. No reason to persist it; already expired.
		return false, nil
	}

	if t.firstHistory(nodeHealth) {
		_, _ = b.Exec(`
			insert ignore into node_health_history
				(hostname, token, first_seen_active, extra_info, command, app_version)
			values
				(?, ?, NOW(), ?, ?, ?)
			`,
			nodeHealth.Hostname, nodeHealth.Token, nodeHealth.ExtraInfo, nodeHealth.Command,
			nodeHealth.AppVersion,
		)
	}
	{
		sqlResult, err := b.Exec(`
			update node_health set
				last_seen_active = now() - interval ? second,
				extra_info = case when ? != '' then ? else extra_info end,
				app_version = ?,
				incrementing_indicator = incrementing_indicator + 1
			where
				hostname = ?
				and token = ?
			`,
			reportedSecondsAgo,
			nodeHealth.ExtraInfo, nodeHealth.ExtraInfo,
			nodeHealth.AppVersion,
			nodeHealth.Hostname, nodeHealth.Token,
		)
		if err != nil {
			return false, log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return false, log.Errore(err)
		}
		if rows > 0 {
			return true, nil
		}
	}
	// Got here? The UPDATE didn't work. Row isn't there.
	{
		cfg := b.Config()
		dbBackend := ""
		if cfg.IsSQLite() {
			dbBackend = cfg.SQLite3DataFile
		} else {
			dbBackend = fmt.Sprintf("%s:%d", cfg.MySQLOrchestratorHost,
				cfg.MySQLOrchestratorPort)
		}
		sqlResult, err := b.Exec(`
			insert ignore into node_health
				(hostname, token, first_seen_active, last_seen_active, extra_info, command, app_version, db_backend)
			values (
				?, ?,
				now() - interval ? second, now() - interval ? second,
				?, ?, ?, ?)
			`,
			nodeHealth.Hostname, nodeHealth.Token,
			reportedSecondsAgo, reportedSecondsAgo,
			nodeHealth.ExtraInfo, nodeHealth.Command,
			nodeHealth.AppVersion, dbBackend,
		)
		if err != nil {
			return false, log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return false, log.Errore(err)
		}
		if rows > 0 {
			return true, nil
		}
	}
	return false, nil
}

// Heartbeat writes this node into node_health: first, synchronously, in the
// control backend, whose outcome is the node health; then in every tenant
// mirror, concurrently. A mirror that fails, is still busy with the previous
// heartbeat or does not answer within the mirror timeout is reported in the
// result under its tenant; it never affects the control outcome.
func (c *Control) Heartbeat(nodeHealth *NodeHealth) *HeartbeatResult {
	nodeHealth.Update()
	res := &HeartbeatResult{At: time.Now(), Tenants: make(map[string]BackendHeartbeat)}

	healthy, err := writeRegisterNode(c.control, nodeHealth)
	res.Control = BackendHeartbeat{Healthy: healthy, Err: err}
	c.control.record(res.Control)
	markHealthCheck(healthy)

	type outcome struct {
		id string
		hb BackendHeartbeat
	}
	// A mirror write can outlive this call (timeout) while the next Heartbeat
	// calls nodeHealth.Update(): the goroutines get an immutable copy. The
	// struct cannot be copied as a whole (it holds a sync.Once).
	snapshot := &NodeHealth{
		Hostname:        nodeHealth.Hostname,
		Token:           nodeHealth.Token,
		AppVersion:      nodeHealth.AppVersion,
		FirstSeenActive: nodeHealth.FirstSeenActive,
		LastSeenActive:  nodeHealth.LastSeenActive,
		ExtraInfo:       nodeHealth.ExtraInfo,
		Command:         nodeHealth.Command,
		DBBackend:       nodeHealth.DBBackend,
		LastReported:    nodeHealth.LastReported,
	}
	mirrors := c.mirrorTargets()
	results := make(chan outcome, len(mirrors))
	pending := make(map[string]*target)
	for _, t := range mirrors {
		if t.viaControl {
			res.Tenants[t.id] = res.Control
			t.record(res.Control)
			continue
		}
		if !t.tryStart() {
			hb := BackendHeartbeat{Err: errMirrorBusy}
			t.record(hb)
			res.Tenants[t.id] = hb
			continue
		}
		pending[t.id] = t
		go func(t *target) {
			healthy, err := c.writeMirror(t, snapshot)
			hb := BackendHeartbeat{Healthy: healthy, Err: err}
			t.record(hb)
			t.finish()
			results <- outcome{t.id, hb}
		}(t)
	}
	if len(pending) > 0 {
		timeout := time.NewTimer(c.mirrorTimeout)
		defer timeout.Stop()
		for len(pending) > 0 {
			select {
			case o := <-results:
				res.Tenants[o.id] = o.hb
				delete(pending, o.id)
			case <-timeout.C:
				for id, t := range pending {
					hb := BackendHeartbeat{Err: errMirrorTimeout}
					t.record(hb)
					res.Tenants[id] = hb
					delete(pending, id)
				}
			}
		}
	}
	c.lastHeartbeat.Store(res)
	return res
}

// forEachBackend runs fn on the control backend and on every mirror backend
// (concurrently). The control error is returned as is; the tenant errors are
// wrapped in *TenantHealthError. All of them are joined.
func (c *Control) forEachBackend(fn func(b *db.Backend) error) error {
	mirrors := c.mirrorTargets()
	errs := make([]error, 1+len(mirrors))
	var wg sync.WaitGroup
	wg.Add(1 + len(mirrors))
	go func() {
		defer wg.Done()
		errs[0] = fn(c.control.backend)
	}()
	for i, t := range mirrors {
		go func(i int, t *target) {
			defer wg.Done()
			if t.viaControl {
				return
			}
			if err := fn(t.backend); err != nil {
				errs[i+1] = &TenantHealthError{TenantID: t.id, Err: err}
			}
		}(i, t)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// ExpireAvailableNodes is an aggressive purging method to remove
// node entries who have skipped their keepalive for two times. It purges the
// control backend and every tenant mirror.
func (c *Control) ExpireAvailableNodes() error {
	err := c.forEachBackend(func(b *db.Backend) error {
		_, err := b.Exec(`
			delete
				from node_health
			where
				last_seen_active < now() - interval ? second
			`,
			config.HealthPollSeconds*5,
		)
		return err
	})
	if err != nil {
		_ = log.Errorf("ExpireAvailableNodes: failed to remove old entries: %+v", err)
	}
	return err
}

// ExpireNodesHistory cleans up the nodes history and is run by
// the orchestrator active node. It purges the control backend and every tenant
// mirror, each with the UnseenInstanceForgetHours of its own configuration.
func (c *Control) ExpireNodesHistory() error {
	err := c.forEachBackend(func(b *db.Backend) error {
		_, err := b.Exec(`
			delete
				from node_health_history
			where
				first_seen_active < now() - interval ? hour
			`,
			b.Config().UnseenInstanceForgetHours,
		)
		return err
	})
	return log.Errore(err)
}

// ReadAvailableNodes lists the live nodes, as seen by the control backend.
func (c *Control) ReadAvailableNodes(onlyHttpNodes bool) (nodes [](*NodeHealth), err error) {
	extraInfo := ""
	if onlyHttpNodes {
		extraInfo = string(OrchestratorExecutionHttpMode)
	}
	query := `
		select
			hostname, token, app_version, first_seen_active, last_seen_active, db_backend
		from
			node_health
		where
			last_seen_active > now() - interval ? second
			and ? in (extra_info, '')
		order by
			hostname
		`

	err = c.control.backend.Query(query, sqlutils.Args(config.HealthPollSeconds*2, extraInfo), func(m sqlutils.RowMap) error {
		nodeHealth := &NodeHealth{
			Hostname:        m.GetString("hostname"),
			Token:           m.GetString("token"),
			AppVersion:      m.GetString("app_version"),
			FirstSeenActive: m.GetString("first_seen_active"),
			LastSeenActive:  m.GetString("last_seen_active"),
			DBBackend:       m.GetString("db_backend"),
		}
		nodes = append(nodes, nodeHealth)
		return nil
	})
	return nodes, log.Errore(err)
}

// TokenBelongsToHealthyHttpService reads the control backend.
func (c *Control) TokenBelongsToHealthyHttpService(token string) (result bool, err error) {
	extraInfo := string(OrchestratorExecutionHttpMode)

	query := `
		select
			token
		from
			node_health
		where
			and token = ?
			and extra_info = ?
		`

	err = c.control.backend.Query(query, sqlutils.Args(token, extraInfo), func(m sqlutils.RowMap) error {
		// Row exists? We're happy
		result = true
		return nil
	})
	return result, log.Errore(err)
}

// WriteRegisterNode writes down this node in the node_health table of the
// default control. Legacy wrapper: it only writes the control backend (it
// updates no health timestamps and writes no mirrors); use Heartbeat.
func WriteRegisterNode(nodeHealth *NodeHealth) (healthy bool, err error) {
	return writeRegisterNode(DefaultControl().control, nodeHealth)
}

// ExpireAvailableNodes is the legacy wrapper of (*Control).ExpireAvailableNodes.
func ExpireAvailableNodes() {
	_ = DefaultControl().ExpireAvailableNodes()
}

// ExpireNodesHistory is the legacy wrapper of (*Control).ExpireNodesHistory.
func ExpireNodesHistory() error {
	return DefaultControl().ExpireNodesHistory()
}

// ReadAvailableNodes is the legacy wrapper of (*Control).ReadAvailableNodes.
func ReadAvailableNodes(onlyHttpNodes bool) (nodes [](*NodeHealth), err error) {
	return DefaultControl().ReadAvailableNodes(onlyHttpNodes)
}

// TokenBelongsToHealthyHttpService is the legacy wrapper of
// (*Control).TokenBelongsToHealthyHttpService.
func TokenBelongsToHealthyHttpService(token string) (result bool, err error) {
	return DefaultControl().TokenBelongsToHealthyHttpService(token)
}
