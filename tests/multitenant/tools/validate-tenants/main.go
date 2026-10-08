// Command validate-tenants loads an orchestrator configuration file and the tenants manifest it points to
// (TenantsConfigFile) with the same code the orchestrator uses, and prints the effective per-tenant settings
// that matter for the end-to-end tests. It exits non-zero when the manifest is rejected.
//
//	validate-tenants /path/to/orchestrator.conf.json
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/proxysql/orchestrator/go/config"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: validate-tenants <orchestrator.conf.json>")
		os.Exit(2)
	}
	base := config.ForceRead(os.Args[1])
	tenants, err := config.LoadTenantConfigs(base)
	if err != nil {
		fmt.Fprintln(os.Stderr, "INVALID:", err)
		os.Exit(1)
	}
	for _, t := range tenants {
		c := t.Cfg
		user := c.MySQLTopologyUser
		if t.Provider == config.ProviderPostgreSQL {
			user = c.PostgreSQLTopologyUser
		}
		fmt.Printf("tenant=%s provider=%s backend=%s topologyUser=%s poll=%ds seeds=%s kvPrefix=%s masterFilters=%s\n",
			t.ID, t.Provider, c.SQLite3DataFile, user, c.InstancePollSeconds,
			strings.Join(c.DiscoverySeeds, ","), c.KVClusterMasterPrefix, strings.Join(c.RecoverMasterClusterFilters, ","))
	}
}
