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

package app

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/proxysql/golib/log"
	"github.com/proxysql/golib/util"
	"github.com/proxysql/orchestrator/go/agent"
	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/inst"
	"github.com/proxysql/orchestrator/go/logic"
	"github.com/proxysql/orchestrator/go/process"
	"github.com/proxysql/orchestrator/go/proxysql"
)

var thisInstanceKey *inst.InstanceKey
var knownCommands []CliCommand

type CliCommand struct {
	Command     string
	Section     string
	Description string
}

type stringSlice []string

func (a stringSlice) Len() int           { return len(a) }
func (a stringSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a stringSlice) Less(i, j int) bool { return a[i] < a[j] }

var commandSynonyms = map[string]string{
	"stop-slave":                  "stop-replica",
	"start-slave":                 "start-replica",
	"restart-slave":               "restart-replica",
	"reset-slave":                 "reset-replica",
	"restart-slave-statements":    "restart-replica-statements",
	"relocate-slaves":             "relocate-replicas",
	"regroup-slaves":              "regroup-replicas",
	"move-up-slaves":              "move-up-replicas",
	"repoint-slaves":              "repoint-replicas",
	"enslave-siblings":            "take-siblings",
	"enslave-master":              "take-master",
	"get-candidate-slave":         "get-candidate-replica",
	"move-slaves-gtid":            "move-replicas-gtid",
	"regroup-slaves-gtid":         "regroup-replicas-gtid",
	"match-slaves":                "match-replicas",
	"match-up-slaves":             "match-up-replicas",
	"regroup-slaves-pgtid":        "regroup-replicas-pgtid",
	"which-cluster-osc-slaves":    "which-cluster-osc-replicas",
	"which-cluster-gh-ost-slaves": "which-cluster-gh-ost-replicas",
	"which-slaves":                "which-replicas",
	"detach-slave":                "detach-replica-master-host",
	"detach-replica":              "detach-replica-master-host",
	"detach-slave-master-host":    "detach-replica-master-host",
	"reattach-slave":              "reattach-replica-master-host",
	"reattach-replica":            "reattach-replica-master-host",
	"reattach-slave-master-host":  "reattach-replica-master-host",
}

func registerCliCommand(command string, section string, description string) string {
	if synonym, ok := commandSynonyms[command]; ok {
		command = synonym
	}
	knownCommands = append(knownCommands, CliCommand{Command: command, Section: section, Description: description})

	return command
}

func commandsListing() string {
	listing := []string{}
	lastSection := ""
	for _, cliCommand := range knownCommands {
		if lastSection != cliCommand.Section {
			lastSection = cliCommand.Section
			listing = append(listing, fmt.Sprintf("%s:", cliCommand.Section))
		}
		commandListing := fmt.Sprintf("\t%-40s%s", cliCommand.Command, cliCommand.Description)
		listing = append(listing, commandListing)
	}
	return strings.Join(listing, "\n")
}

func availableCommandsUsage() string {
	return fmt.Sprintf(`Available commands (-c):
%+v
Run 'orchestrator help <command>' for detailed help on given command, e.g. 'orchestrator help relocate'

Usage for most commands:
	orchestrator -c <command> [-i <instance.fqdn>[,<instance.fqdn>]* ] [-d <destination.fqdn>] [-tenant <id>] [--verbose|--debug]
With several tenants, -tenant <id> (or ORCHESTRATOR_TENANT) selects the one the command acts on; see 'orchestrator help tenant'.
`, commandsListing())
}

// getClusterName will make a best effort to deduce a cluster name using either a given alias
// or an instanceKey. First attempt is at alias, and if that doesn't work, we try instanceKey.
func getClusterName(tenant *logic.Tenant, clusterAlias string, instanceKey *inst.InstanceKey) (clusterName string) {
	clusterName, _ = tenant.Store.FigureClusterName(clusterAlias, instanceKey, thisInstanceKey)
	return clusterName
}

func validateInstanceIsFound(tenant *logic.Tenant, instanceKey *inst.InstanceKey) (instance *inst.Instance) {
	instance, _, err := tenant.Store.ReadInstance(instanceKey)
	if err != nil {
		_ = log.Fatale(err)
	}
	if instance == nil {
		_ = log.Fatalf("Instance not found: %+v", *instanceKey)
	}
	return instance
}

// CliWrapper is called from main and allows for the instance parameter
// to take multiple instance names separated by a comma or whitespace.
// The tenant is the one named by the ORCHESTRATOR_TENANT environment variable,
// or else the default one (see CliWrapperForTenant).
func CliWrapper(command string, strict bool, instances string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {
	CliWrapperForTenant("", command, strict, instances, destination, owner, reason, duration, pattern, clusterAlias, pool, hostnameFlag)
}

// CliWrapperForTenant is CliWrapper for the tenant tenantID (the -tenant flag).
// An empty tenantID defers to ORCHESTRATOR_TENANT and then to the default tenant.
// The tenant is resolved once, whatever the number of instances.
func CliWrapperForTenant(tenantID string, command string, strict bool, instances string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {
	if config.Config.RaftEnabled && !*config.RuntimeCLIFlags.IgnoreRaftSetup {
		_ = log.Fatalf(`Orchestrator configured to run raft ("RaftEnabled": true). All access must go through the web API of the active raft node. You may use the orchestrator-client script which has a similar interface to the command line invocation. You may override this with --ignore-raft-setup`)
	}
	session := mustOpenCliSession(tenantID, command)
	defer session.close()
	r := regexp.MustCompile(`[ ,\r\n\t]+`)
	tokens := r.Split(instances, -1)
	switch command {
	case "submit-pool-instances":
		{
			// These commands unsplit the tokens (they expect a comma delimited list of instances)
			tokens = []string{instances}
		}
	}
	for _, instance := range tokens {
		if instance != "" || len(tokens) == 1 {
			cliRun(session, command, strict, instance, destination, owner, reason, duration, pattern, clusterAlias, pool, hostnameFlag)
		}
	}
}

// Cli initiates a command line interface, executing requested command, on the
// tenant named by ORCHESTRATOR_TENANT or else the default one.
func Cli(command string, strict bool, instance string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {
	CliForTenant("", command, strict, instance, destination, owner, reason, duration, pattern, clusterAlias, pool, hostnameFlag)
}

// CliForTenant is Cli for the tenant tenantID (the -tenant flag). An empty
// tenantID defers to ORCHESTRATOR_TENANT and then to the default tenant: the
// tenant "default" or the only one. If none applies the process exits and says
// which tenants exist.
func CliForTenant(tenantID string, command string, strict bool, instance string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {
	session := mustOpenCliSession(tenantID, command)
	defer session.close()
	cliRun(session, command, strict, instance, destination, owner, reason, duration, pattern, clusterAlias, pool, hostnameFlag)
}

// cliRun executes one command on the tenant of the session.
func cliRun(session *cliSession, command string, strict bool, instance string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {
	if synonym, ok := commandSynonyms[command]; ok {
		command = synonym
	}
	tenant, cfg := session.tenant, session.cfg

	skipDatabaseCommands := false
	switch command {
	case "redeploy-internal-db":
		skipDatabaseCommands = true
	case "help":
		skipDatabaseCommands = true
	case "dump-config":
		skipDatabaseCommands = true
	case "tenants":
		skipDatabaseCommands = true
	}

	// The instances are resolved by the store of the tenant; the commands that
	// need no tenant (help, dump-config, tenants...) have no instance to resolve.
	var err error
	var instanceKey, rawInstanceKey, destinationKey *inst.InstanceKey
	if destination != "" && !strings.Contains(destination, ":") {
		destination = fmt.Sprintf("%s:%d", destination, cfg.DefaultInstancePort)
	}
	if tenant != nil {
		instanceKey, err = tenant.Store.ParseResolveInstanceKey(instance)
		if err != nil {
			instanceKey = nil
		}
		rawInstanceKey, err = tenant.Store.ParseRawInstanceKey(instance)
		if err != nil {
			rawInstanceKey = nil
		}
		destinationKey, err = tenant.Store.ParseResolveInstanceKey(destination)
		if err != nil {
			destinationKey = nil
		}
		if !skipDatabaseCommands {
			destinationKey = tenant.Store.ReadFuzzyInstanceKeyIfPossible(destinationKey)
		}
	}
	if hostname, err := os.Hostname(); err == nil {
		thisInstanceKey = &inst.InstanceKey{Hostname: hostname, Port: int(cfg.DefaultInstancePort)}
	}
	postponedFunctionsContainer := inst.NewPostponedFunctionsContainer()

	if len(owner) == 0 {
		// get os username as owner
		usr, err := user.Current()
		if err != nil {
			_ = log.Fatale(err)
		}
		owner = usr.Username
	}
	inst.SetMaintenanceOwner(owner)

	if !skipDatabaseCommands && !*config.RuntimeCLIFlags.SkipContinuousRegistration {
		session.manager.Control().ContinuousRegistration(string(process.OrchestratorExecutionCliMode), command)
	}

	// begin commands
	switch command {
	// smart mode
	case registerCliCommand("relocate", "Smart relocation", `Relocate a replica beneath another instance`), registerCliCommand("relocate-below", "Smart relocation", `Synonym to 'relocate', will be deprecated`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce destination:", destination)
			}
			_, err := tenant.Store.RelocateBelow(instanceKey, destinationKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), destinationKey.DisplayString())
		}
	case registerCliCommand("relocate-replicas", "Smart relocation", `Relocates all or part of the replicas of a given instance under another instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce destination:", destination)
			}
			replicas, _, errs, err := tenant.Store.RelocateReplicas(instanceKey, destinationKey, pattern)
			if err != nil {
				_ = log.Fatale(err)
			} else {
				for _, e := range errs {
					_ = log.Errore(e)
				}
				for _, replica := range replicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("take-siblings", "Smart relocation", `Turn all siblings of a replica into its sub-replicas.`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}
			_, _, err := tenant.Store.TakeSiblings(instanceKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("regroup-replicas", "Smart relocation", `Given an instance, pick one of its replicas and make it local master of its siblings`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(tenant, instanceKey)

			lostReplicas, equalReplicas, aheadReplicas, cannotReplicateReplicas, promotedReplica, err := tenant.Store.RegroupReplicas(instanceKey, false, func(candidateReplica *inst.Instance) { fmt.Println(candidateReplica.Key.DisplayString()) }, postponedFunctionsContainer)
			lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

			postponedFunctionsContainer.Wait()
			if promotedReplica == nil {
				_ = log.Fatalf("Could not regroup replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Printf("%s lost: %d, trivial: %d, pseudo-gtid: %d\n",
				promotedReplica.Key.DisplayString(), len(lostReplicas), len(equalReplicas), len(aheadReplicas))
			if err != nil {
				_ = log.Fatale(err)
			}
		}
		// General replication commands
		// move, binlog file:pos
	case registerCliCommand("move-up", "Classic file:pos relocation", `Move a replica one level up the topology`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			instance, err := tenant.Store.MoveUp(instanceKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), instance.MasterKey.DisplayString())
		}
	case registerCliCommand("move-up-replicas", "Classic file:pos relocation", `Moves replicas of the given instance one level up the topology`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}

			movedReplicas, _, errs, err := tenant.Store.MoveUpReplicas(instanceKey, pattern)
			if err != nil {
				_ = log.Fatale(err)
			} else {
				for _, e := range errs {
					_ = log.Errore(e)
				}
				for _, replica := range movedReplicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("move-below", "Classic file:pos relocation", `Moves a replica beneath its sibling. Both replicas must be actively replicating from same master.`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce destination/sibling:", destination)
			}
			_, err := tenant.Store.MoveBelow(instanceKey, destinationKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), destinationKey.DisplayString())
		}
	case registerCliCommand("move-equivalent", "Classic file:pos relocation", `Moves a replica beneath another server, based on previously recorded "equivalence coordinates"`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce destination:", destination)
			}
			_, err := tenant.Store.MoveEquivalent(instanceKey, destinationKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), destinationKey.DisplayString())
		}
	case registerCliCommand("repoint", "Classic file:pos relocation", `Make the given instance replicate from another instance without changing the binglog coordinates. Use with care`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			// destinationKey can be null, in which case the instance repoints to its existing master
			instance, err := tenant.Store.Repoint(instanceKey, destinationKey, inst.GTIDHintNeutral)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), instance.MasterKey.DisplayString())
		}
	case registerCliCommand("repoint-replicas", "Classic file:pos relocation", `Repoint all replicas of given instance to replicate back from the instance. Use with care`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			repointedReplicas, errs, err := tenant.Store.RepointReplicasTo(instanceKey, pattern, destinationKey)
			if err != nil {
				_ = log.Fatale(err)
			} else {
				for _, e := range errs {
					_ = log.Errore(e)
				}
				for _, replica := range repointedReplicas {
					fmt.Printf("%s<%s\n", replica.Key.DisplayString(), instanceKey.DisplayString())
				}
			}
		}
	case registerCliCommand("take-master", "Classic file:pos relocation", `Turn an instance into a master of its own master; essentially switch the two.`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}
			_, err := tenant.Store.TakeMaster(instanceKey, false)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("make-co-master", "Classic file:pos relocation", `Create a master-master replication. Given instance is a replica which replicates directly from a master.`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.MakeCoMaster(instanceKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("get-candidate-replica", "Classic file:pos relocation", `Information command suggesting the most up-to-date replica of a given instance that is good for promotion`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}

			instance, _, _, _, _, err := tenant.Store.GetCandidateReplica(instanceKey, false)
			if err != nil {
				_ = log.Fatale(err)
			} else {
				fmt.Println(instance.Key.DisplayString())
			}
		}
	case registerCliCommand("regroup-replicas-bls", "Binlog server relocation", `Regroup Binlog Server replicas of a given instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(tenant, instanceKey)

			_, promotedBinlogServer, err := tenant.Store.RegroupReplicasBinlogServers(instanceKey, false)
			if promotedBinlogServer == nil {
				_ = log.Fatalf("Could not regroup binlog server replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Println(promotedBinlogServer.Key.DisplayString())
			if err != nil {
				_ = log.Fatale(err)
			}
		}
	// move, GTID
	case registerCliCommand("move-gtid", "GTID relocation", `Move a replica beneath another instance.`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce destination:", destination)
			}
			_, err := tenant.Store.MoveBelowGTID(instanceKey, destinationKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), destinationKey.DisplayString())
		}
	case registerCliCommand("move-replicas-gtid", "GTID relocation", `Moves all replicas of a given instance under another (destination) instance using GTID`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce destination:", destination)
			}
			movedReplicas, _, errs, err := tenant.Store.MoveReplicasGTID(instanceKey, destinationKey, pattern)
			if err != nil {
				_ = log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, replica := range movedReplicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("regroup-replicas-gtid", "GTID relocation", `Given an instance, pick one of its replica and make it local master of its siblings, using GTID.`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(tenant, instanceKey)

			lostReplicas, movedReplicas, cannotReplicateReplicas, promotedReplica, err := tenant.Store.RegroupReplicasGTID(instanceKey, false, true, func(candidateReplica *inst.Instance) { fmt.Println(candidateReplica.Key.DisplayString()) }, postponedFunctionsContainer, nil)
			lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

			if promotedReplica == nil {
				_ = log.Fatalf("Could not regroup replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Printf("%s lost: %d, moved: %d\n",
				promotedReplica.Key.DisplayString(), len(lostReplicas), len(movedReplicas))
			if err != nil {
				_ = log.Fatale(err)
			}
		}
		// Pseudo-GTID
	case registerCliCommand("match", "Pseudo-GTID relocation", `Matches a replica beneath another (destination) instance using Pseudo-GTID`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce destination:", destination)
			}
			_, _, err := tenant.Store.MatchBelow(instanceKey, destinationKey, true)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), destinationKey.DisplayString())
		}
	case registerCliCommand("match-up", "Pseudo-GTID relocation", `Transport the replica one level up the hierarchy, making it child of its grandparent, using Pseudo-GTID`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			instance, _, err := tenant.Store.MatchUp(instanceKey, true)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), instance.MasterKey.DisplayString())
		}
	case registerCliCommand("rematch", "Pseudo-GTID relocation", `Reconnect a replica onto its master, via PSeudo-GTID.`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			instance, _, err := tenant.Store.RematchReplica(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), instance.MasterKey.DisplayString())
		}
	case registerCliCommand("match-replicas", "Pseudo-GTID relocation", `Matches all replicas of a given instance under another (destination) instance using Pseudo-GTID`):
		{
			// Move all replicas of "instance" beneath "destination"
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce destination:", destination)
			}

			matchedReplicas, _, errs, err := tenant.Store.MultiMatchReplicas(instanceKey, destinationKey, pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, replica := range matchedReplicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("match-up-replicas", "Pseudo-GTID relocation", `Matches replicas of the given instance one level up the topology, making them siblings of given instance, using Pseudo-GTID`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}

			matchedReplicas, _, errs, err := tenant.Store.MatchUpReplicas(instanceKey, pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, replica := range matchedReplicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("regroup-replicas-pgtid", "Pseudo-GTID relocation", `Given an instance, pick one of its replica and make it local master of its siblings, using Pseudo-GTID.`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(tenant, instanceKey)

			onCandidateReplicaChosen := func(candidateReplica *inst.Instance) { fmt.Println(candidateReplica.Key.DisplayString()) }
			lostReplicas, equalReplicas, aheadReplicas, cannotReplicateReplicas, promotedReplica, err := tenant.Store.RegroupReplicasPseudoGTID(instanceKey, false, onCandidateReplicaChosen, postponedFunctionsContainer, nil)
			lostReplicas = append(lostReplicas, cannotReplicateReplicas...)
			postponedFunctionsContainer.Wait()
			if promotedReplica == nil {
				_ = log.Fatalf("Could not regroup replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Printf("%s lost: %d, trivial: %d, pseudo-gtid: %d\n",
				promotedReplica.Key.DisplayString(), len(lostReplicas), len(equalReplicas), len(aheadReplicas))
			if err != nil {
				log.Fatale(err)
			}
		}
		// General replication commands
	case registerCliCommand("enable-gtid", "Replication, general", `If possible, turn on GTID replication`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.EnableGTID(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("disable-gtid", "Replication, general", `Turn off GTID replication, back to file:pos replication`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.DisableGTID(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("which-gtid-errant", "Replication, general", `Get errant GTID set (empty results if no errant GTID)`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)

			instance, err := tenant.Store.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				_ = log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			fmt.Println(instance.GtidErrant)
		}
	case registerCliCommand("gtid-errant-reset-master", "Replication, general", `Reset master on instance, remove GTID errant transactions`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.ErrantGTIDResetMaster(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("skip-query", "Replication, general", `Skip a single statement on a replica; either when running with GTID or without`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.SkipQuery(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("stop-slave", "Replication, general", `Issue a STOP SLAVE on an instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.StopReplication(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("start-slave", "Replication, general", `Issue a START SLAVE on an instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.StartReplication(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("restart-slave", "Replication, general", `STOP and START SLAVE on an instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.RestartReplication(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("reset-slave", "Replication, general", `Issues a RESET SLAVE command; use with care`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.ResetReplicationOperation(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("detach-replica-master-host", "Replication, general", `Stops replication and modifies Master_Host into an impossible, yet reversible, value.`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}
			_, err := tenant.Store.DetachReplicaMasterHost(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("reattach-replica-master-host", "Replication, general", `Undo a detach-replica-master-host operation`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatal("Cannot deduce instance:", instance)
			}
			_, err := tenant.Store.ReattachReplicaMasterHost(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("master-pos-wait", "Replication, general", `Wait until replica reaches given replication coordinates (--binlog=file:pos)`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatalf("Unresolved instance")
			}
			instance, err := tenant.Store.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				_ = log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			var binlogCoordinates *inst.BinlogCoordinates

			if binlogCoordinates, err = inst.ParseBinlogCoordinates(*config.RuntimeCLIFlags.BinlogFile); err != nil {
				_ = log.Fatalf("Expecing --binlog argument as file:pos")
			}
			_, err = tenant.Store.MasterPosWait(instanceKey, binlogCoordinates)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("enable-semi-sync-master", "Replication, general", `Enable semi-sync replication (master-side)`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.SetSemiSyncMaster(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("disable-semi-sync-master", "Replication, general", `Disable semi-sync replication (master-side)`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.SetSemiSyncMaster(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("enable-semi-sync-replica", "Replication, general", `Enable semi-sync replication (replica-side)`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.SetSemiSyncReplica(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("disable-semi-sync-replica", "Replication, general", `Disable semi-sync replication (replica-side)`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.SetSemiSyncReplica(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("restart-slave-statements", "Replication, general", `Get a list of statements to execute to stop then restore replica to same execution state. Provide --statement for injected statement`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatalf("Unresolved instance")
			}
			statements, err := tenant.Store.GetReplicationRestartPreserveStatements(instanceKey, *config.RuntimeCLIFlags.Statement)
			if err != nil {
				log.Fatale(err)
			}
			for _, statement := range statements {
				fmt.Println(statement)
			}
		}
		// Replication, information
	case registerCliCommand("can-replicate-from", "Replication information", `Can an instance (-i) replicate from another (-d) according to replication rules? Prints 'true|false'`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatalf("Unresolved instance")
			}
			instance := validateInstanceIsFound(tenant, instanceKey)
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce target instance:", destination)
			}
			otherInstance := validateInstanceIsFound(tenant, destinationKey)

			if canReplicate, _ := tenant.Store.InstanceCanReplicateFromEx(instance, otherInstance, "CLI: can-replicate-from"); canReplicate {
				fmt.Println(destinationKey.DisplayString())
			}
		}
	case registerCliCommand("is-replicating", "Replication information", `Is an instance (-i) actively replicating right now`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatalf("Unresolved instance")
			}
			instance := validateInstanceIsFound(tenant, instanceKey)
			if instance.ReplicaRunning() {
				fmt.Println(instance.Key.DisplayString())
			}
		}
	case registerCliCommand("is-replication-stopped", "Replication information", `Is an instance (-i) a replica with both replication threads stopped`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatalf("Unresolved instance")
			}
			instance := validateInstanceIsFound(tenant, instanceKey)
			if instance.ReplicationThreadsStopped() {
				fmt.Println(instance.Key.DisplayString())
			}
		}
		// Instance
	case registerCliCommand("set-read-only", "Instance", `Turn an instance read-only, via SET GLOBAL read_only := 1`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.SetReadOnly(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("set-writeable", "Instance", `Turn an instance writeable, via SET GLOBAL read_only := 0`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.SetReadOnly(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
		// Binary log operations
	case registerCliCommand("flush-binary-logs", "Binary logs", `Flush binary logs on an instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			var err error
			if *config.RuntimeCLIFlags.BinlogFile == "" {
				_, err = tenant.Store.FlushBinaryLogs(instanceKey, 1)
			} else {
				_, err = tenant.Store.FlushBinaryLogsTo(instanceKey, *config.RuntimeCLIFlags.BinlogFile)
			}
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("purge-binary-logs", "Binary logs", `Purge binary logs of an instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			var err error
			if *config.RuntimeCLIFlags.BinlogFile == "" {
				_ = log.Fatal("expecting --binlog value")
			}

			_, err = tenant.Store.PurgeBinaryLogsTo(instanceKey, *config.RuntimeCLIFlags.BinlogFile, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("last-pseudo-gtid", "Binary logs", `Find latest Pseudo-GTID entry in instance's binary logs`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatalf("Unresolved instance")
			}
			instance, err := tenant.Store.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				_ = log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			coordinates, text, err := tenant.Store.FindLastPseudoGTIDEntry(instance, instance.RelaylogCoordinates, nil, strict, nil)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Printf("%+v:%s\n", *coordinates, text)
		}
	case registerCliCommand("locate-gtid-errant", "Binary logs", `List binary logs containing errant GTIDs`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatalf("Unresolved instance")
			}
			errantBinlogs, err := tenant.Store.LocateErrantGTID(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			for _, binlog := range errantBinlogs {
				fmt.Println(binlog)
			}
		}
	case registerCliCommand("last-executed-relay-entry", "Binary logs", `Find coordinates of last executed relay log entry`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatalf("Unresolved instance")
			}
			instance, err := tenant.Store.ReadTopologyInstance(instanceKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			if instance == nil {
				_ = log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			minCoordinates, err := tenant.Store.GetPreviousKnownRelayLogCoordinatesForInstance(instance)
			if err != nil {
				_ = log.Fatalf("Error reading last known coordinates for %+v: %+v", instance.Key, err)
			}
			binlogEvent, err := tenant.Store.GetLastExecutedEntryInRelayLogs(instance, minCoordinates, instance.RelaylogCoordinates)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Printf("%+v:%d\n", *binlogEvent, binlogEvent.NextEventPos)
		}
	case registerCliCommand("correlate-relaylog-pos", "Binary logs", `Given an instance (-i) and relaylog coordinates (--binlog=file:pos), find the correlated coordinates in another instance's relay logs (-d)`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				_ = log.Fatalf("Unresolved instance")
			}
			instance, err := tenant.Store.ReadTopologyInstance(instanceKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			if instance == nil {
				_ = log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			if destinationKey == nil {
				_ = log.Fatal("Cannot deduce target instance:", destination)
			}
			otherInstance, err := tenant.Store.ReadTopologyInstance(destinationKey)
			if err != nil {
				_ = log.Fatale(err)
			}
			if otherInstance == nil {
				_ = log.Fatalf("Instance not found: %+v", *destinationKey)
			}

			var relaylogCoordinates *inst.BinlogCoordinates
			if *config.RuntimeCLIFlags.BinlogFile != "" {
				if relaylogCoordinates, err = inst.ParseBinlogCoordinates(*config.RuntimeCLIFlags.BinlogFile); err != nil {
					_ = log.Fatalf("Expecing --binlog argument as file:pos")
				}
			}
			instanceCoordinates, correlatedCoordinates, nextCoordinates, _, err := tenant.Store.CorrelateRelaylogCoordinates(instance, relaylogCoordinates, otherInstance)
			if err != nil {
				_ = log.Fatale(err)
			}
			fmt.Printf("%+v;%+v;%+v\n", *instanceCoordinates, *correlatedCoordinates, *nextCoordinates)
		}
	case registerCliCommand("find-binlog-entry", "Binary logs", `Get binlog file:pos of entry given by --pattern (exact full match, not a regular expression) in a given instance`):
		{
			if pattern == "" {
				_ = log.Fatal("No pattern given")
			}
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := tenant.Store.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			coordinates, err := tenant.Store.SearchEntryInInstanceBinlogs(instance, pattern, false, nil)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v", *coordinates))
		}
	case registerCliCommand("correlate-binlog-pos", "Binary logs", `Given an instance (-i) and binlog coordinates (--binlog=file:pos), find the correlated coordinates in another instance (-d)`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := tenant.Store.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			if !instance.LogBinEnabled {
				log.Fatalf("Instance does not have binary logs: %+v", *instanceKey)
			}
			if destinationKey == nil {
				log.Fatal("Cannot deduce target instance:", destination)
			}
			otherInstance, err := tenant.Store.ReadTopologyInstance(destinationKey)
			if err != nil {
				log.Fatale(err)
			}
			if otherInstance == nil {
				log.Fatalf("Instance not found: %+v", *destinationKey)
			}
			var binlogCoordinates *inst.BinlogCoordinates
			if *config.RuntimeCLIFlags.BinlogFile == "" {
				binlogCoordinates = &instance.SelfBinlogCoordinates
			} else {
				if binlogCoordinates, err = inst.ParseBinlogCoordinates(*config.RuntimeCLIFlags.BinlogFile); err != nil {
					log.Fatalf("Expecing --binlog argument as file:pos")
				}
			}

			coordinates, _, err := tenant.Store.CorrelateBinlogCoordinates(instance, binlogCoordinates, otherInstance)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v", *coordinates))
		}
		// Pool
	case registerCliCommand("submit-pool-instances", "Pools", `Submit a pool name with a list of instances in that pool`):
		{
			if pool == "" {
				log.Fatal("Please submit --pool")
			}
			err := tenant.Store.ApplyPoolInstances(inst.NewPoolInstancesSubmission(pool, instance))
			if err != nil {
				log.Fatale(err)
			}
		}
	case registerCliCommand("cluster-pool-instances", "Pools", `List all pools and their associated instances`):
		{
			clusterPoolInstances, err := tenant.Store.ReadAllClusterPoolInstances()
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterPoolInstance := range clusterPoolInstances {
				fmt.Println(fmt.Sprintf("%s\t%s\t%s\t%s:%d", clusterPoolInstance.ClusterName, clusterPoolInstance.ClusterAlias, clusterPoolInstance.Pool, clusterPoolInstance.Hostname, clusterPoolInstance.Port))
			}
		}
	case registerCliCommand("which-heuristic-cluster-pool-instances", "Pools", `List instances of a given cluster which are in either any pool or in a specific pool`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)

			instances, err := tenant.Store.GetHeuristicClusterPoolInstances(clusterName, pool)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
		// Information
	case registerCliCommand("find", "Information", `Find instances whose hostname matches given regex pattern`):
		{
			if pattern == "" {
				log.Fatal("No pattern given")
			}
			instances, err := tenant.Store.FindInstances(pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("search", "Information", `Search instances by name, version, version comment, port`):
		{
			if pattern == "" {
				log.Fatal("No pattern given")
			}
			instances, err := tenant.Store.SearchInstances(pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("clusters", "Information", `List all clusters known to orchestrator`):
		{
			clusters, err := tenant.Store.ReadClusters()
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(strings.Join(clusters, "\n"))
		}
	case registerCliCommand("clusters-alias", "Information", `List all clusters known to orchestrator`):
		{
			clusters, err := tenant.Store.ReadClustersInfo("")
			if err != nil {
				log.Fatale(err)
			}
			for _, cluster := range clusters {
				fmt.Println(fmt.Sprintf("%s\t%s", cluster.ClusterName, cluster.ClusterAlias))
			}
		}
	case registerCliCommand("all-clusters-masters", "Information", `List of writeable masters, one per cluster`):
		{
			instances, err := tenant.Store.ReadWriteableClustersMasters()
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("topology", "Information", `Show an ascii-graph of a replication topology, given a member of that topology`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			output, err := tenant.Store.ASCIITopology(clusterName, pattern, false, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(output)
		}
	case registerCliCommand("topology-tabulated", "Information", `Show an ascii-graph of a replication topology, given a member of that topology`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			output, err := tenant.Store.ASCIITopology(clusterName, pattern, true, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(output)
		}
	case registerCliCommand("topology-tags", "Information", `Show an ascii-graph of a replication topology and instance tags, given a member of that topology`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			output, err := tenant.Store.ASCIITopology(clusterName, pattern, false, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(output)
		}
	case registerCliCommand("all-instances", "Information", `The complete list of known instances`):
		{
			instances, err := tenant.Store.SearchInstances("")
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("which-instance", "Information", `Output the fully-qualified hostname:port representation of the given instance, or error if unknown`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get master: unresolved instance")
			}
			instance := validateInstanceIsFound(tenant, instanceKey)
			fmt.Println(instance.Key.DisplayString())
		}
	case registerCliCommand("which-cluster", "Information", `Output the name of the cluster an instance belongs to, or error if unknown to orchestrator`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			fmt.Println(clusterName)
		}
	case registerCliCommand("which-cluster-alias", "Information", `Output the alias of the cluster an instance belongs to, or error if unknown to orchestrator`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			clusterInfo, err := tenant.Store.ReadClusterInfo(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(clusterInfo.ClusterAlias)
		}
	case registerCliCommand("which-cluster-domain", "Information", `Output the domain name of the cluster an instance belongs to, or error if unknown to orchestrator`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			clusterInfo, err := tenant.Store.ReadClusterInfo(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(clusterInfo.ClusterDomain)
		}
	case registerCliCommand("which-heuristic-domain-instance", "Information", `Returns the instance associated as the cluster's writer with a cluster's domain name.`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			instanceKey, err := tenant.Store.GetHeuristicClusterDomainInstanceAttribute(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("which-cluster-master", "Information", `Output the name of the master in a given cluster`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			masters, err := tenant.Store.ReadClusterMaster(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			if len(masters) == 0 {
				log.Fatalf("No writeable masters found for cluster %+v", clusterName)
			}
			fmt.Println(masters[0].Key.DisplayString())
		}
	case registerCliCommand("which-cluster-instances", "Information", `Output the list of instances participating in same cluster as given instance`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			instances, err := tenant.Store.ReadClusterInstances(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-cluster-osc-replicas", "Information", `Output a list of replicas in a cluster, that could serve as a pt-online-schema-change operation control replicas`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			instances, err := tenant.Store.GetClusterOSCReplicas(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-cluster-gh-ost-replicas", "Information", `Output a list of replicas in a cluster, that could serve as a gh-ost working server`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			instances, err := tenant.Store.GetClusterGhostReplicas(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-master", "Information", `Output the fully-qualified hostname:port representation of a given instance's master`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get master: unresolved instance")
			}
			instance := validateInstanceIsFound(tenant, instanceKey)
			if instance.MasterKey.IsValid() {
				fmt.Println(instance.MasterKey.DisplayString())
			}
		}
	case registerCliCommand("which-downtimed-instances", "Information", `List instances currently downtimed, potentially filtered by cluster`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			instances, err := tenant.Store.ReadDowntimedInstances(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-replicas", "Information", `Output the fully-qualified hostname:port list of replicas of a given instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get replicas: unresolved instance")
			}
			replicas, err := tenant.Store.ReadReplicaInstances(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			for _, replica := range replicas {
				fmt.Println(replica.Key.DisplayString())
			}
		}
	case registerCliCommand("which-lost-in-recovery", "Information", `List instances marked as downtimed for being lost in a recovery process`):
		{
			instances, err := tenant.Store.ReadLostInRecoveryInstances("")
			if err != nil {
				log.Fatale(err)
			}
			for _, instance := range instances {
				fmt.Println(instance.Key.DisplayString())
			}
		}
	case registerCliCommand("instance-status", "Information", `Output short status on a given instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get status: unresolved instance")
			}
			instance := validateInstanceIsFound(tenant, instanceKey)
			fmt.Println(tenant.Store.InstanceHumanReadableDescription(instance))
		}
	case registerCliCommand("get-cluster-heuristic-lag", "Information", `For a given cluster (indicated by an instance or alias), output a heuristic "representative" lag of that cluster`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			lag, err := tenant.Store.GetClusterHeuristicLag(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(lag)
		}
	case registerCliCommand("submit-masters-to-kv-stores", "Key-value", `Submit master of a specific cluster, or all masters of all clusters to key-value stores`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			log.Debugf("cluster name is <%s>", clusterName)

			kvPairs, _, err := tenant.SubmitMastersToKvStores(clusterName, true)
			if err != nil {
				log.Fatale(err)
			}
			for _, kvPair := range kvPairs {
				fmt.Println(fmt.Sprintf("%s:%s", kvPair.Key, kvPair.Value))
			}
		}

	case registerCliCommand("tags", "tags", `List tags for a given instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			tags, err := tenant.Store.ReadInstanceTags(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			for _, tag := range tags {
				fmt.Println(tag.String())
			}
		}
	case registerCliCommand("tag-value", "tags", `Get tag value for a specific instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			tag, err := inst.ParseTag(*config.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatale(err)
			}

			tagExists, err := tenant.Store.ReadInstanceTag(instanceKey, tag)
			if err != nil {
				log.Fatale(err)
			}
			if tagExists {
				fmt.Println(tag.TagValue)
			}
		}
	case registerCliCommand("tagged", "tags", `List instances tagged by tag-string. Format: "tagname" or "tagname=tagvalue" or comma separated "tag0,tag1=val1,tag2" for intersection of all.`):
		{
			tagsString := *config.RuntimeCLIFlags.Tag
			instanceKeyMap, err := tenant.Store.GetInstanceKeysByTags(tagsString)
			if err != nil {
				log.Fatale(err)
			}
			keysDisplayStrings := []string{}
			for _, key := range instanceKeyMap.GetInstanceKeys() {
				keysDisplayStrings = append(keysDisplayStrings, key.DisplayString())
			}
			sort.Strings(keysDisplayStrings)
			for _, s := range keysDisplayStrings {
				fmt.Println(s)
			}
		}
	case registerCliCommand("tag", "tags", `Add a tag to a given instance. Tag in "tagname" or "tagname=tagvalue" format`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			tag, err := inst.ParseTag(*config.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatale(err)
			}
			_ = tenant.Store.PutInstanceTag(instanceKey, tag)
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("untag", "tags", `Remove a tag from an instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			tag, err := inst.ParseTag(*config.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatale(err)
			}
			untagged, err := tenant.Store.Untag(instanceKey, tag)
			if err != nil {
				log.Fatale(err)
			}
			for _, key := range untagged.GetInstanceKeys() {
				fmt.Println(key.DisplayString())
			}
		}
	case registerCliCommand("untag-all", "tags", `Remove a tag from all matching instances`):
		{
			tag, err := inst.ParseTag(*config.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatale(err)
			}
			untagged, err := tenant.Store.Untag(nil, tag)
			if err != nil {
				log.Fatale(err)
			}
			for _, key := range untagged.GetInstanceKeys() {
				fmt.Println(key.DisplayString())
			}
		}

		// Instance management
	case registerCliCommand("discover", "Instance management", `Lookup an instance, investigate it`):
		{
			if instanceKey == nil {
				instanceKey = thisInstanceKey
			}
			if instanceKey == nil {
				log.Fatalf("Cannot figure instance key")
			}
			instance, err := tenant.Store.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instance.Key.DisplayString())
		}
	case registerCliCommand("forget", "Instance management", `Forget about an instance's existence`):
		{
			if rawInstanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			instanceKey, _ = tenant.Store.FigureInstanceKey(rawInstanceKey, nil)
			err := tenant.Store.ForgetInstance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("begin-maintenance", "Instance management", `Request a maintenance lock on an instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if reason == "" {
				log.Fatal("--reason option required")
			}
			var durationSeconds = 0
			if duration != "" {
				durationSeconds, err = util.SimpleTimeToSeconds(duration)
				if err != nil {
					log.Fatale(err)
				}
				if durationSeconds < 0 {
					log.Fatalf("Duration value must be non-negative. Given value: %d", durationSeconds)
				}
			}
			maintenanceKey, err := tenant.Store.BeginBoundedMaintenance(instanceKey, inst.GetMaintenanceOwner(), reason, uint(durationSeconds), true)
			if err == nil {
				log.Infof("Maintenance key: %+v", maintenanceKey)
				log.Infof("Maintenance duration: %d seconds", durationSeconds)
			}
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("end-maintenance", "Instance management", `Remove maintenance lock from an instance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.EndMaintenanceByInstanceKey(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("in-maintenance", "Instance management", `Check whether instance is under maintenance`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			inMaintenance, err := tenant.Store.InMaintenance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if inMaintenance {
				fmt.Println(instanceKey.DisplayString())
			}
		}
	case registerCliCommand("begin-downtime", "Instance management", `Mark an instance as downtimed`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if reason == "" {
				log.Fatal("--reason option required")
			}
			var durationSeconds = 0
			if duration != "" {
				durationSeconds, err = util.SimpleTimeToSeconds(duration)
				if err != nil {
					log.Fatale(err)
				}
				if durationSeconds < 0 {
					log.Fatalf("Duration value must be non-negative. Given value: %d", durationSeconds)
				}
			}
			duration := time.Duration(durationSeconds) * time.Second
			err := tenant.Store.BeginDowntime(inst.NewDowntime(instanceKey, inst.GetMaintenanceOwner(), reason, duration))
			if err == nil {
				log.Infof("Downtime duration: %d seconds", durationSeconds)
			} else {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("end-downtime", "Instance management", `Indicate an instance is no longer downtimed`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := tenant.Store.EndDowntime(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
		// Recovery & analysis
	case registerCliCommand("recover", "Recovery", `Do auto-recovery given a dead instance`), registerCliCommand("recover-lite", "Recovery", `Do auto-recovery given a dead instance. Orchestrator chooses the best course of actionwithout executing external processes`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}

			recoveryAttempted, promotedInstanceKey, err := tenant.CheckAndRecover(instanceKey, destinationKey, (command == "recover-lite"))
			if err != nil {
				log.Fatale(err)
			}
			if recoveryAttempted {
				if promotedInstanceKey == nil {
					log.Fatalf("Recovery attempted yet no replica promoted")
				}
				fmt.Println(promotedInstanceKey.DisplayString())
			}
		}
	case registerCliCommand("force-master-failover", "Recovery", `Forcibly discard master and initiate a failover, even if orchestrator doesn't see a problem. This command lets orchestrator choose the replacement master`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			topologyRecovery, err := tenant.ForceMasterFailover(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
		}
	case registerCliCommand("force-master-takeover", "Recovery", `Forcibly discard master and promote another (direct child) instance instead, even if everything is running well`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination, the instance to promote in place of the master. Please provide with -d")
			}
			destination := validateInstanceIsFound(tenant, destinationKey)
			topologyRecovery, err := tenant.ForceMasterTakeover(clusterName, destination)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
		}
	case registerCliCommand("graceful-master-takeover", "Recovery", `Gracefully promote a new master. Either indicate identity of new master via '-d designated.instance.com' or setup replication tree to have a single direct replica to the master.`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			if destinationKey != nil {
				validateInstanceIsFound(tenant, destinationKey)
			}
			topologyRecovery, promotedMasterCoordinates, err := tenant.GracefulMasterTakeover(clusterName, destinationKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
			fmt.Println(*promotedMasterCoordinates)
			log.Debugf("Promoted %+v as new master. Binlog coordinates at time of promotion: %+v", topologyRecovery.SuccessorKey, *promotedMasterCoordinates)
		}
	case registerCliCommand("graceful-master-takeover-auto", "Recovery", `Gracefully promote a new master. orchestrator will attempt to pick the promoted replica automatically`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			// destinationKey doesn't _have_ to be specified: if unspecified, orchestrator will auto-deduce a replica.
			// but if specified, then that's the replica to promote, and it must be valid.
			if destinationKey != nil {
				validateInstanceIsFound(tenant, destinationKey)
			}
			topologyRecovery, promotedMasterCoordinates, err := tenant.GracefulMasterTakeover(clusterName, destinationKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
			fmt.Println(*promotedMasterCoordinates)
			log.Debugf("Promoted %+v as new master. Binlog coordinates at time of promotion: %+v", topologyRecovery.SuccessorKey, *promotedMasterCoordinates)
		}
	case registerCliCommand("replication-analysis", "Recovery", `Request an analysis of potential crash incidents in all known topologies`):
		{
			analysis, err := tenant.Store.GetReplicationAnalysis("", &inst.ReplicationAnalysisHints{})
			if err != nil {
				log.Fatale(err)
			}
			for _, entry := range analysis {
				fmt.Println(fmt.Sprintf("%s (cluster %s): %s", entry.AnalyzedInstanceKey.DisplayString(), entry.ClusterDetails.ClusterName, entry.AnalysisString()))
			}
		}
	case registerCliCommand("ack-all-recoveries", "Recovery", `Acknowledge all recoveries; this unblocks pending future recoveries`):
		{
			if reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			countRecoveries, err := tenant.AcknowledgeAllRecoveries(inst.GetMaintenanceOwner(), reason)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%d recoveries acknowledged", countRecoveries))
		}
	case registerCliCommand("ack-cluster-recoveries", "Recovery", `Acknowledge recoveries for a given cluster; this unblocks pending future recoveries`):
		{
			if reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			countRecoveries, err := tenant.AcknowledgeClusterRecoveries(clusterName, inst.GetMaintenanceOwner(), reason)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%d recoveries acknowledged", countRecoveries))
		}
	case registerCliCommand("ack-instance-recoveries", "Recovery", `Acknowledge recoveries for a given instance; this unblocks pending future recoveries`):
		{
			if reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)

			countRecoveries, err := tenant.AcknowledgeInstanceRecoveries(instanceKey, inst.GetMaintenanceOwner(), reason)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%d recoveries acknowledged", countRecoveries))
		}
	// Instance meta
	case registerCliCommand("register-candidate", "Instance, meta", `Indicate that a specific instance is a preferred candidate for master promotion`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			promotionRule, err := inst.ParseCandidatePromotionRule(*config.RuntimeCLIFlags.PromotionRule)
			if err != nil {
				log.Fatale(err)
			}
			err = tenant.Store.RegisterCandidateInstance(tenant.Store.CandidateDatabaseInstanceWithCurrentTime(inst.NewCandidateDatabaseInstance(instanceKey, promotionRule)))
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("register-hostname-unresolve", "Instance, meta", `Assigns the given instance a virtual (aka "unresolved") name`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			err := tenant.Store.RegisterHostnameUnresolve(inst.NewHostnameRegistration(instanceKey, hostnameFlag))
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("deregister-hostname-unresolve", "Instance, meta", `Explicitly deregister/dosassociate a hostname with an "unresolved" name`):
		{
			instanceKey, _ = tenant.Store.FigureInstanceKey(instanceKey, thisInstanceKey)
			err := tenant.Store.RegisterHostnameUnresolve(inst.NewHostnameDeregistration(instanceKey))
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("set-heuristic-domain-instance", "Instance, meta", `Associate domain name of given cluster with what seems to be the writer master for that cluster`):
		{
			clusterName := getClusterName(tenant, clusterAlias, instanceKey)
			instanceKey, err := tenant.Store.HeuristicallyApplyClusterDomainInstanceAttribute(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}

		// meta
	case registerCliCommand("snapshot-topologies", "Meta", `Take a snapshot of existing topologies.`):
		{
			err := tenant.Store.SnapshotTopologies()
			if err != nil {
				log.Fatale(err)
			}
		}
	case registerCliCommand("continuous", "Meta", `Enter continuous mode, and actively poll for instances, diagnose problems, do maintenance`):
		{
			runContinuous(session)
		}
	case registerCliCommand("active-nodes", "Meta", `List currently active orchestrator nodes`):
		{
			nodes, err := session.manager.Control().ReadAvailableNodes(false)
			if err != nil {
				log.Fatale(err)
			}
			for _, node := range nodes {
				fmt.Println(node)
			}
		}
	case registerCliCommand("access-token", "Meta", `Get a HTTP access token`):
		{
			publicToken, err := session.manager.Control().GenerateAccessToken(owner)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(publicToken)
		}
	case registerCliCommand("resolve", "Meta", `Resolve given hostname`):
		{
			if rawInstanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			if conn, err := net.Dial("tcp", rawInstanceKey.DisplayString()); err == nil {
				log.Debugf("tcp test is good; got connection %+v", conn)
				_ = conn.Close()
			} else {
				log.Fatale(err)
			}
			if cname, err := inst.GetCNAME(rawInstanceKey.Hostname); err == nil {
				log.Debugf("GetCNAME() %+v, %+v", cname, err)
				rawInstanceKey.Hostname = cname
				fmt.Println(rawInstanceKey.DisplayString())
			} else {
				log.Fatale(err)
			}
		}
	case registerCliCommand("reset-hostname-resolve-cache", "Meta", `Clear the hostname resolve cache`):
		{
			err := tenant.Store.ResetHostnameResolveCache()
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println("hostname resolve cache cleared")
		}
	case registerCliCommand("dump-config", "Meta", `Print out configuration in JSON format`):
		{
			jsonString := cfg.ToJSONString()
			fmt.Println(jsonString)
		}
	case registerCliCommand("tenants", "Meta", `List the tenants of this orchestrator: ID, provider and backend`):
		{
			if err := printTenants(os.Stdout, config.Config); err != nil {
				log.Fatale(err)
			}
		}
	case registerCliCommand("show-resolve-hosts", "Meta", `Show the content of the hostname_resolve table. Generally used for debugging`):
		{
			resolves, err := tenant.Store.ReadAllHostnameResolves()
			if err != nil {
				log.Fatale(err)
			}
			for _, r := range resolves {
				fmt.Println(r)
			}
		}
	case registerCliCommand("show-unresolve-hosts", "Meta", `Show the content of the hostname_unresolve table. Generally used for debugging`):
		{
			unresolves, err := tenant.Store.ReadAllHostnameUnresolves()
			if err != nil {
				log.Fatale(err)
			}
			for _, r := range unresolves {
				fmt.Println(r)
			}
		}
	case registerCliCommand("redeploy-internal-db", "Meta, internal", `Force internal schema migration to current backend structure`):
		{
			config.RuntimeCLIFlags.ConfiguredVersion = ""
			_, err := tenant.Store.ReadClusters()
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println("Redeployed internal db")
		}
	case registerCliCommand("internal-suggest-promoted-replacement", "Internal", `Internal only, used to test promotion logic in CI`):
		{
			destination := validateInstanceIsFound(tenant, destinationKey)
			replacement, _, err := tenant.SuggestReplacementForPromotedReplica(&logic.TopologyRecovery{}, instanceKey, destination, nil)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(replacement.Key.DisplayString())
		}
	case registerCliCommand("custom-command", "Agent", "Execute a custom command on the agent as defined in the agent conf"):
		{
			output, err := agent.CustomCommand(hostnameFlag, pattern)
			if err != nil {
				log.Fatale(err)
			}

			fmt.Printf("%v\n", output)
		}
	case registerCliCommand("disable-global-recoveries", "", `Disallow orchestrator from performing recoveries globally`):
		{
			if err := tenant.DisableRecovery(); err != nil {
				log.Fatalf("ERROR: Failed to disable recoveries globally: %v\n", err)
			}
			fmt.Println("OK: Orchestrator recoveries DISABLED globally")
		}
	case registerCliCommand("enable-global-recoveries", "", `Allow orchestrator to perform recoveries globally`):
		{
			if err := tenant.EnableRecovery(); err != nil {
				log.Fatalf("ERROR: Failed to enable recoveries globally: %v\n", err)
			}
			fmt.Println("OK: Orchestrator recoveries ENABLED globally")
		}
	case registerCliCommand("check-global-recoveries", "", `Show the global recovery configuration`):
		{
			isDisabled, err := tenant.IsRecoveryDisabled()
			if err != nil {
				log.Fatalf("ERROR: Failed to determine if recoveries are disabled globally: %v\n", err)
			}
			fmt.Printf("OK: Global recoveries disabled: %v\n", isDisabled)
		}
	case registerCliCommand("bulk-instances", "", `Return a list of sorted instance names known to orchestrator`):
		{
			instances, err := tenant.Store.BulkReadInstance()
			if err != nil {
				log.Fatalf("Error: Failed to retrieve instances: %v\n", err)
				return
			}
			var asciiInstances stringSlice
			for _, v := range instances {
				asciiInstances = append(asciiInstances, v.String())
			}
			sort.Sort(asciiInstances)
			fmt.Printf("%s\n", strings.Join(asciiInstances, "\n"))
		}
	case registerCliCommand("bulk-promotion-rules", "", `Return a list of promotion rules known to orchestrator`):
		{
			promotionRules, err := tenant.Store.BulkReadCandidateDatabaseInstance()
			if err != nil {
				log.Fatalf("Error: Failed to retrieve promotion rules: %v\n", err)
			}
			var asciiPromotionRules stringSlice
			for _, v := range promotionRules {
				asciiPromotionRules = append(asciiPromotionRules, v.String())
			}
			sort.Sort(asciiPromotionRules)

			fmt.Printf("%s\n", strings.Join(asciiPromotionRules, "\n"))
		}
	case registerCliCommand("proxysql-test", "ProxySQL", `Test connectivity to ProxySQL Admin interface`):
		{
			hook := tenant.ProxySQL
			if !hook.IsConfigured() {
				log.Fatal("ProxySQL is not configured. Set ProxySQLAdminAddress and ProxySQLWriterHostgroup in config.")
			}
			client := proxysql.NewClient(
				cfg.ProxySQLAdminAddress,
				cfg.ProxySQLAdminPort,
				cfg.ProxySQLAdminUser,
				cfg.ProxySQLAdminPassword,
				cfg.ProxySQLAdminUseTLS,
			)
			if client == nil {
				log.Fatal("ProxySQL client creation failed.")
			}
			if err := client.Ping(); err != nil {
				log.Fatale(err)
			}
			fmt.Println("ProxySQL Admin connection: OK")
			fmt.Printf("Writer hostgroup: %d\n", cfg.ProxySQLWriterHostgroup)
			fmt.Printf("Reader hostgroup: %d\n", cfg.ProxySQLReaderHostgroup)
		}
	case registerCliCommand("proxysql-servers", "ProxySQL", `Show mysql_servers from ProxySQL`):
		{
			client := proxysql.NewClient(
				cfg.ProxySQLAdminAddress,
				cfg.ProxySQLAdminPort,
				cfg.ProxySQLAdminUser,
				cfg.ProxySQLAdminPassword,
				cfg.ProxySQLAdminUseTLS,
			)
			if client == nil {
				log.Fatal("ProxySQL is not configured.")
			}
			rows, db, err := client.Query("SELECT hostgroup_id, hostname, port, status, weight FROM runtime_mysql_servers ORDER BY hostgroup_id, hostname, port")
			if err != nil {
				log.Fatale(err)
			}
			defer func() { _ = db.Close() }()
			defer func() { _ = rows.Close() }()
			fmt.Printf("%-12s %-30s %-6s %-15s %-6s\n", "HOSTGROUP", "HOSTNAME", "PORT", "STATUS", "WEIGHT")
			for rows.Next() {
				var hg, port, weight int
				var hostname, status string
				if err := rows.Scan(&hg, &hostname, &port, &status, &weight); err != nil {
					_ = log.Errorf("Error scanning row: %v", err)
					continue
				}
				fmt.Printf("%-12d %-30s %-6d %-15s %-6d\n", hg, hostname, port, status, weight)
			}
		}
		// Help
	case "help":
		{
			fmt.Fprintf(os.Stderr, "%s", availableCommandsUsage())
		}
	default:
		log.Fatalf("Unknown command: \"%s\". %s", command, availableCommandsUsage())
	}
}
