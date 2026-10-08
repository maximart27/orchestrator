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
	"github.com/proxysql/golib/log"
	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/raft"
	"github.com/proxysql/orchestrator/go/util"
)

// The election without Raft lives in the active_node table of the CONTROL
// backend only. With several tenants (several backends) that cannot yield a
// single leader for all of them, so multi-tenant requires Raft: see
// ValidateMultiTenantElection. With Raft none of this is consulted.

// AttemptElection tries to grab leadership (become active node)
func (c *Control) AttemptElection() (bool, error) {
	{
		sqlResult, err := c.control.backend.Exec(`
		insert ignore into active_node (
				anchor, hostname, token, first_seen_active, last_seen_active
			) values (
				1, ?, ?, now(), now()
			)
		`,
			ThisHostname, util.ProcessToken.Hash,
		)
		if err != nil {
			return false, log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return false, log.Errore(err)
		}
		if rows > 0 {
			// We managed to insert a row
			return true, nil
		}
	}
	{
		// takeover from a node that has been inactive
		sqlResult, err := c.control.backend.Exec(`
			update active_node set
				hostname = ?,
				token = ?,
				first_seen_active=now(),
				last_seen_active=now()
			where
				anchor = 1
			  and last_seen_active < (now() - interval ? second)
		`,
			ThisHostname, util.ProcessToken.Hash, config.ActiveNodeExpireSeconds,
		)
		if err != nil {
			return false, log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return false, log.Errore(err)
		}
		if rows > 0 {
			// We managed to update a row: overtaking a previous leader
			return true, nil
		}
	}
	{
		// Update last_seen_active is this very node is already the active node
		sqlResult, err := c.control.backend.Exec(`
			update active_node set
				last_seen_active=now()
			where
				anchor = 1
				and hostname = ?
				and token = ?
		`,
			ThisHostname, util.ProcessToken.Hash,
		)
		if err != nil {
			return false, log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return false, log.Errore(err)
		}
		if rows > 0 {
			// Reaffirmed our own leadership
			return true, nil
		}
	}
	return false, nil
}

// GrabElection forcibly grabs leadership. Use with care!!
func (c *Control) GrabElection() error {
	if orcraft.IsRaftEnabled() {
		return log.Errorf("Cannot GrabElection on raft setup")
	}
	_, err := c.control.backend.Exec(`
			replace into active_node (
					anchor, hostname, token, first_seen_active, last_seen_active
				) values (
					1, ?, ?, now(), now()
				)
			`,
		ThisHostname, util.ProcessToken.Hash,
	)
	return log.Errore(err)
}

// Reelect clears the way for re-elections. Active node is immediately demoted.
func (c *Control) Reelect() error {
	if orcraft.IsRaftEnabled() {
		orcraft.StepDown()
	}
	_, err := c.control.backend.Exec(`delete from active_node where anchor = 1`)
	return log.Errore(err)
}

// ElectedNode returns the details of the elected node, as well as answering the question "is this process the elected one"?
func (c *Control) ElectedNode() (node *NodeHealth, isElected bool, err error) {
	node = &NodeHealth{}
	query := `
		select
			hostname,
			token,
			first_seen_active,
			last_seen_Active
		from
			active_node
		where
			anchor = 1
		`
	err = c.control.backend.QueryRowsMap(query, func(m sqlutils.RowMap) error {
		node.Hostname = m.GetString("hostname")
		node.Token = m.GetString("token")
		node.FirstSeenActive = m.GetString("first_seen_active")
		node.LastSeenActive = m.GetString("last_seen_active")

		return nil
	})

	isElected = (node.Hostname == ThisHostname && node.Token == util.ProcessToken.Hash)
	return node, isElected, log.Errore(err)
}

// AttemptElection is the legacy wrapper of (*Control).AttemptElection.
func AttemptElection() (bool, error) { return DefaultControl().AttemptElection() }

// GrabElection is the legacy wrapper of (*Control).GrabElection.
func GrabElection() error { return DefaultControl().GrabElection() }

// Reelect is the legacy wrapper of (*Control).Reelect.
func Reelect() error { return DefaultControl().Reelect() }

// ElectedNode is the legacy wrapper of (*Control).ElectedNode.
func ElectedNode() (node *NodeHealth, isElected bool, err error) {
	return DefaultControl().ElectedNode()
}
