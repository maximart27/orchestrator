# Upgrading to multi-tenant

This page covers the three moves between a single-tenant orchestrator and a multi-tenant one, and how to undo them:

1. [Migrating a single-tenant installation](#1-migrating-a-single-tenant-installation-to-multi-tenant): your current
   configuration becomes the tenant `default`.
2. [Rolling upgrade of an upstream Raft cluster](#2-rolling-upgrade-of-an-upstream-raft-cluster-to-the-new-binary):
   replacing the binary of a running cluster, node by node, without stopping it.
3. [Activating the second tenant](#3-activating-the-second-tenant): the step that makes the cluster really
   multi-tenant, and the only one that cannot be mixed with old nodes.

Then [rollback](#4-rollback-and-what-cannot-be-undone) and the [table of known risks](#5-known-risks). For a fresh
install read [Installing orchestrator in multi-tenant mode](install-multi-tenant.md); what a tenant is and what it
shares is in [Multi-tenant orchestrator](multi-tenant.md).

## The path, in stages

```text
 stage 0                stage 1                    stage 2                     stage 3
 upstream, 1 tenant --> new binary, NO tenants --> tenants file with only  --> tenants file with
 (old nodes)            file (legacy config)       the tenant "default"        "default" + 1 or more tenants
 |<-- mixed old/new nodes allowed -->|             (all nodes new)               (all nodes new, same file)
```

* Stage 0 to 1 is the binary swap. With the **legacy configuration (no tenants file)** old and new nodes
  interoperate, so it is a rolling upgrade (section 2).
* Stage 1 to 2 only reorganises the configuration (section 1): the existing data becomes the tenant `default`.
  Stage 3 always includes it (the tenants file must declare `default`, see section 3), so stages 2 and 3 can share a
  restart round; keeping them apart makes a problem easier to attribute.
* Stages 2 and 3 need **every** node on the new binary.

What is tested, not just designed: `tests/multitenant/run-mixed.sh` runs old (upstream `master`) and new nodes in one
Raft group and checks that they interoperate through the rolling upgrade, that the legacy commands behave the same
in both directions, that snapshots cross versions (new to old and old to new), that a node can be downgraded, and
that a second tenant breaks old nodes (the hazard of stage 3). See
[`tests/multitenant/README-mixed.md`](../tests/multitenant/README-mixed.md).

## 1. Migrating a single-tenant installation to multi-tenant

Without a tenants file the process already runs **one implicit tenant called `default`**, driven by the regular
configuration, and it is already multi-tenant-aware: `GET /api/tenants` lists it and the metrics carry
`tenant="default"`. Migrating means moving that configuration into a tenants file, where `default` is declared like
any other tenant and others can later be added next to it. Nothing else has to change, which is the point: the
`default` tenant keeps its data, its hooks and every route that already existed.

### What changes and what does not

| Area | After the migration |
|---|---|
| **Data** | The backend of `default` is the one you have today (same SQLite file or MySQL database); nothing is copied or converted. It is also the **control backend** of the node (node heartbeat, access tokens). |
| **API** | Every legacy route (`/api/...`, `/api/v2/...`, `/web/...`) keeps working and resolves to `default`; they never become ambiguous while a tenant called `default` exists. New: `GET /api/tenants` and the per-tenant form `/api/tenants/default/...`. API responses carry a `TenantId` field (`"TenantId":"default"`); ignore it if you do not need it. |
| **`orchestrator-client` and CLI** | Unchanged. `-T <tenant>` / `ORCHESTRATOR_TENANT` (client) and `-tenant <id>` / `ORCHESTRATOR_TENANT` (CLI) select a tenant; without them the tenant `default` is used. New: `orchestrator -c tenants`. |
| **Hooks** | Unchanged, and they gain `ORC_TENANT_ID` in their environment and the `{tenantId}` placeholder in the command line (`default` for this tenant). They still inherit the environment of the process. |
| **Metrics** | Same names; the tenant-scoped series (discoveries, instances, clusters, recoveries, queue lengths...) carry the label `tenant="default"`, process-wide ones (health, Raft) have none. The label is already there in the legacy setup, so a dashboard that already works keeps working; if you came from an older build without it, review PromQL that matches or aggregates on exact label sets (`on()`, `ignoring()`, `without()`). |
| **Health** | `/api/health` gains `Details.Tenants` with the health of each tenant. |
| **`SIGHUP` / `reload-configuration`** | Re-reads only the **global** configuration file. Once `default` is declared in a tenants file it keeps the configuration it started with: to change it, edit the tenants file and **restart** the node. This is the one behaviour that gets worse for day-2 changes; plan for it. |
| **Raft** | The log entries and snapshots of `default` are exactly the ones of a single-tenant orchestrator (see section 2): old and new nodes still understand each other. |

### Step by step

Take a copy of the backend first: for SQLite, stop the node and copy the `.db` file (and its `-wal`/`-shm` if
present), or `sqlite3 <file> ".backup '<copy>'"` while it runs; for a MySQL backend, a normal dump.

Start from your existing configuration. This is a typical single-tenant node of a 3-node Raft cluster (SQLite
backend):

<!-- validate:upgrade-before:legacy -->
```json
{
  "ListenAddress": ":3000",
  "HTTPAdvertise": "http://orchestrator1.example.com:3000",
  "BackendDB": "sqlite3",
  "SQLite3DataFile": "/var/lib/orchestrator/orchestrator.db",
  "RaftEnabled": true,
  "RaftDataDir": "/var/lib/orchestrator/raft",
  "RaftBind": "10.0.0.11",
  "DefaultRaftPort": 10008,
  "RaftNodes": ["10.0.0.11", "10.0.0.12", "10.0.0.13"],
  "MySQLTopologyCredentialsConfigFile": "/etc/orchestrator/credentials/prod.cnf",
  "InstancePollSeconds": 5,
  "RecoverMasterClusterFilters": ["*"],
  "RecoveryPeriodBlockSeconds": 600,
  "ConsulAddress": "consul.example.com:8500",
  "KVClusterMasterPrefix": "prod/master",
  "PostMasterFailoverProcesses": ["/usr/local/bin/post-failover {successorHost}:{successorPort}"]
}
```

Split it in two. The **node configuration** keeps what is global or node-local (listen address, Raft, HTTP,
authentication, logging...) and gains `TenantsConfigFile`. The **backend settings leave it**: they go to the `Backend`
section of `default`. The **tenant-scoped settings** (topology credentials, polling, filters, recovery policy, hooks,
KV store, ProxySQL) go to the `Config` section of `default`.

<!-- validate:upgrade-after:node -->
```json
{
  "ListenAddress": ":3000",
  "HTTPAdvertise": "http://orchestrator1.example.com:3000",
  "RaftEnabled": true,
  "RaftDataDir": "/var/lib/orchestrator/raft",
  "RaftBind": "10.0.0.11",
  "DefaultRaftPort": 10008,
  "RaftNodes": ["10.0.0.11", "10.0.0.12", "10.0.0.13"],
  "TenantsConfigFile": "/etc/orchestrator/tenants.json"
}
```

<!-- validate:upgrade-after:tenants -->
```json
{
  "Tenants": [
    {
      "ID": "default",
      "Provider": "mysql",
      "Backend": {
        "BackendDB": "sqlite3",
        "SQLite3DataFile": "/var/lib/orchestrator/orchestrator.db"
      },
      "Config": {
        "MySQLTopologyCredentialsConfigFile": "/etc/orchestrator/credentials/prod.cnf",
        "InstancePollSeconds": 5,
        "RecoverMasterClusterFilters": ["*"],
        "RecoveryPeriodBlockSeconds": 600,
        "ConsulAddress": "consul.example.com:8500",
        "KVClusterMasterPrefix": "prod/master",
        "PostMasterFailoverProcesses": ["/usr/local/bin/post-failover {successorHost}:{successorPort}"]
      }
    }
  ]
}
```

Rules behind that split:

* **The tenant called `default` may keep the node's backend.** The tenant `default` is the control backend of the node
  (the manager uses its backend as the control one), so it can inherit the global
  `BackendDB`/`SQLite3DataFile`/`MySQLOrchestrator*` settings: the data of a single-tenant installation stays where it
  is. Any other tenant must have its own `Backend`: a tenant other than `default` that uses the node's backend, or two
  tenants that share one backend, is rejected
  (`tenant "x" uses the SQLite3DataFile ... of the node's own (control) backend: give the tenant its own Backend.SQLite3DataFile`).
  Without a tenant called `default`, the global backend settings describe a separate control backend.
* **`Provider` is the engine of the tenant**: `mysql` (also MariaDB) or `postgresql`. It replaces `ProviderType`,
  which is not accepted inside a tenant. A single-tenant PostgreSQL installation becomes `"Provider": "postgresql"`.
* **Why move the tenant-scoped settings and not leave them in the node file.** A tenant inherits *everything* it
  does not set, including topology credentials, hooks, ProxySQL and KV settings. If you leave them global, `default`
  works, but every tenant you add later silently inherits them too (the hooks of `default` would run for the new
  tenant). Moving them makes `default` explicit and the node file purely node-level. Settings that are global
  (Raft, HTTP, authentication, `ReadOnly`, `PowerAuthUsers`...) are rejected inside a tenant: they stay in the node file.
* **MySQL backend.** Move `BackendDB` (`mysql`) and every `MySQLOrchestrator*` setting the same way:

<!-- validate:upgrade-mysql:node -->
```json
{
  "ListenAddress": ":3000",
  "HTTPAdvertise": "http://orchestrator1.example.com:3000",
  "RaftEnabled": true,
  "RaftDataDir": "/var/lib/orchestrator/raft",
  "RaftBind": "10.0.0.11",
  "DefaultRaftPort": 10008,
  "RaftNodes": ["10.0.0.11", "10.0.0.12", "10.0.0.13"],
  "TenantsConfigFile": "/etc/orchestrator/tenants.json"
}
```

<!-- validate:upgrade-mysql:tenants -->
```json
{
  "Tenants": [
    {
      "ID": "default",
      "Provider": "mysql",
      "Backend": {
        "BackendDB": "mysql",
        "MySQLOrchestratorHost": "backend1.example.com",
        "MySQLOrchestratorPort": 3306,
        "MySQLOrchestratorDatabase": "orchestrator",
        "MySQLOrchestratorCredentialsConfigFile": "/etc/orchestrator/credentials/backend.cnf"
      },
      "Config": {
        "MySQLTopologyCredentialsConfigFile": "/etc/orchestrator/credentials/prod.cnf"
      }
    }
  ]
}
```

1. Create `/etc/orchestrator/tenants.json` and edit the node configuration as above, on **every** node of the
   cluster (the backend path may legitimately differ per node; the tenant IDs and providers may not). Owner
   `orchestrator`, mode `0600` (see [Installation](install-multi-tenant.md#service-user-directories-and-permissions)).
2. Validate on each node, before touching the service. This loads the node configuration and the tenants file with
   the same code the service uses and opens no backend:

   ```shell
   sudo -u orchestrator /usr/local/orchestrator/orchestrator \
     --config /etc/orchestrator/orchestrator.conf.json --ignore-raft-setup -c tenants
   # default	mysql	sqlite3:/var/lib/orchestrator/orchestrator.db
   ```

3. Restart the node (one at a time in a Raft cluster; followers first, the leader last, as in
   [section 2](#2-rolling-upgrade-of-an-upstream-raft-cluster-to-the-new-binary)).
4. Verify that it is the same orchestrator with the same data:

   ```shell
   curl -s localhost:3000/api/tenants | jq '.Details[] | {ID, Provider, Default, Healthy}'   # default, Default: true, Healthy: true
   curl -s localhost:3000/api/clusters                                                         # the clusters you had
   curl -s localhost:3000/api/tenants/default/clusters                                         # same answer, per-tenant form
   curl -s localhost:3000/metrics | grep -E '^orchestrator_instances_total'                    # {tenant="default"}
   ```

If the node is **not** running Raft you can stop here: a single tenant needs no Raft. Adding a second tenant does,
and enabling Raft on a node that did not use it is a change of its own (see [orchestrator/raft](raft.md) and
[Configuration: raft](configuration-raft.md)): do it as a separate step, before section 3, never together with it.
A shared-backend (MySQL) high-availability setup has the same constraint: multi-tenant needs one backend per tenant
and Raft between the nodes.

## 2. Rolling upgrade of an upstream Raft cluster to the new binary

This is stage 0 to 1: the same cluster, the same configuration, a new binary, one node at a time, with the cluster
serving throughout. It rests on what the new binary guarantees while there is **no tenants file**:

* The Raft log entries of the legacy tenant are byte for byte the ones an old node writes and reads (no `Tenant`
  field). The tenant `default` is encoded the same way.
* A legacy single-tenant deployment keeps writing the **snapshot format that old nodes read** (version 1). The
  multi-tenant snapshot (version 2) is only written once a second tenant exists.
* An old node that receives a command it does not know (the leader's periodic `tenant-set` announcement) answers
  "Unknown command op" and keeps working; it simply cannot report a tenant mismatch.

Hard rules:

1. **Followers first, the leader last**, one node at a time, waiting for the cluster to be healthy in between.
2. **No tenants file and no second tenant anywhere** until every node runs the new binary (section 3). Leave the
   configuration exactly as it is.
3. **Never run a build older than the fix of the snapshot format** (commit `283f0ce0`, "logic: a legacy
   single-tenant snapshot keeps the format that pre-multi-tenancy nodes read", merged with `2716106f`). Earlier
   builds of the multi-tenant work write version 2 snapshots even for one tenant: an old node that restores one finds
   no table in it, comes up with an **empty state** and **re-enables recoveries** that the cluster had disabled.
   Any release or build that contains that commit is fine.
4. Do not restart two nodes at once: a 3-node group needs 2 for quorum.

### Before starting

* [ ] The new binary is built from a tree that contains `283f0ce0` and is installed next to the old one
      (`/usr/local/orchestrator/orchestrator.new`) on every node, or ready as a package.
* [ ] Every node has the same configuration and no `TenantsConfigFile`.
* [ ] Backup of the backend of every node (SQLite: copy the file; keep the Raft data dir too).
* [ ] Decide whether to pause automatic recoveries during the window: `orchestrator-client -c disable-global-recoveries`
      before and `-c enable-global-recoveries` after. The flag is replicated through Raft and survives snapshots in
      both directions (with several tenants it is per tenant: add `-T <tenant>`). A failure during the window is then
      not handled automatically: a policy decision, not a requirement.
* [ ] The cluster is healthy and quiet. On every node:

```shell
curl -s localhost:3000/api/raft-state; echo        # Leader on exactly one node, Follower on the others
curl -s localhost:3000/api/raft-peers | jq .       # all peers present
curl -s localhost:3000/api/health | jq '.Details | {Healthy, IsRaftLeader, versions: [.AvailableNodes[] | {Hostname, AppVersion}]}'
curl -s localhost:3000/metrics | grep -E '^orchestrator_active_recoveries'    # 0: no recovery in progress
```

### Each node, followers first

1. Find the leader: `curl -s localhost:3000/api/raft-leader`. Start with a follower.
2. Stop the service and wait for the orderly stop. The remaining nodes keep quorum and the leader stays.
3. Swap the binary and start it. Keep the configuration, the backend and `RaftDataDir`:

   ```shell
   sudo systemctl stop orchestrator
   sudo install -m 0755 orchestrator.new /usr/local/orchestrator/orchestrator
   sudo systemctl start orchestrator
   ```

4. Verify before the next node:

   ```shell
   /usr/local/orchestrator/orchestrator --version                      # the new version
   curl -s localhost:3000/api/raft-state; echo                         # Follower
   curl -s localhost:3000/api/raft-health; echo                        # part of a healthy group
   curl -s localhost:3000/api/health | jq '.Details.AvailableNodes[] | {Hostname, AppVersion}'   # mixed versions visible here
   curl -s localhost:3000/api/tenants | jq '.Message'                  # "1 tenants": the implicit tenant "default"
   ```

   The node has to catch up (log replay, or a snapshot from the leader if it was down long enough). Wait until
   `/api/raft-state` answers `Follower` and the leader's `/api/raft-peers` lists it before moving on.

### The leader, last

Same procedure on the leader. Stopping it makes Raft elect a new leader among the nodes already upgraded (a few
seconds, expected); the old leader comes back as a follower. Optionally move the leadership beforehand
(`orchestrator-client -c raft-elect-leader --hostname <upgraded node>`).

### After

* All nodes: same `AppVersion` in `/api/health`, one leader, `orchestrator_tenant_set_mismatch 0`,
  `orchestrator_raft_unknown_tenant_commands_total 0`, no `FATAL` or panic in the logs.
* Exercise a Raft write and see it on every node (`begin-downtime`/`end-downtime` of a test instance, or
  `set-cluster-alias`), then end it. Re-enable the recoveries if you paused them.

What the mixed-version suite shows during the window (all phases): one leader, one converged state on every node
(tags, downtimes, candidates, aliases, hostname unresolve, recoveries, the disabled-recoveries flag); a failover
hook that runs exactly **once, on the leader**, with old leader and new followers, and with all-new; and no
`tenant` field in any Raft entry.

## 3. Activating the second tenant

After this step the cluster is multi-tenant for real, and it is **one way** for old binaries. Preconditions, all of
which must be true:

* [ ] **Every node runs the new binary.** A node of an older version ignores the `Tenant` field of a Raft command and
      would apply the commands of the new tenant to its only backend; it also cannot read a multi-tenant snapshot.
      This hazard is reproduced by scenario 4 of the mixed-version suite.
* [ ] Raft is enabled on every node (more than one tenant refuses to start without it).
* [ ] The tenants file **declares `default`** and `default` is the tenant that holds the existing data (section 1).
      The Raft entries and snapshots written so far belong to the tenant `default`; a file without it would drop them.
* [ ] The cluster is healthy, quiet (no recovery in progress) and backed up.
* [ ] The tenants file is **identical in tenant IDs and providers on every node**. The backend path and the
      connection details may differ per node; the set of tenants may not. A node without a tenant drops that
      tenant's Raft commands **for good**, and adding the tenant later does not recover them.
* [ ] The new tenant has its **own backend** (own SQLite file or MySQL database), its own credentials, and, if it
      shares ProxySQL or Consul/ZooKeeper with another tenant, disjoint hostgroups and a different
      `KVClusterMasterPrefix`. All of it is validated when the file loads.

The tenants file, now with `default` (already declared as in section 1, so it does not inherit anything by accident)
and the new tenant:

<!-- validate:upgrade-second:node -->
```json
{
  "ListenAddress": ":3000",
  "HTTPAdvertise": "http://orchestrator1.example.com:3000",
  "RaftEnabled": true,
  "RaftDataDir": "/var/lib/orchestrator/raft",
  "RaftBind": "10.0.0.11",
  "DefaultRaftPort": 10008,
  "RaftNodes": ["10.0.0.11", "10.0.0.12", "10.0.0.13"],
  "TenantsConfigFile": "/etc/orchestrator/tenants.json"
}
```

<!-- validate:upgrade-second:tenants -->
```json
{
  "Tenants": [
    {
      "ID": "default",
      "Provider": "mysql",
      "Backend": {
        "BackendDB": "sqlite3",
        "SQLite3DataFile": "/var/lib/orchestrator/orchestrator.db"
      },
      "Config": {
        "MySQLTopologyCredentialsConfigFile": "/etc/orchestrator/credentials/prod.cnf",
        "InstancePollSeconds": 5,
        "RecoverMasterClusterFilters": ["*"],
        "RecoveryPeriodBlockSeconds": 600,
        "ConsulAddress": "consul.example.com:8500",
        "KVClusterMasterPrefix": "prod/master",
        "PostMasterFailoverProcesses": ["/usr/local/bin/post-failover {successorHost}:{successorPort}"]
      }
    },
    {
      "ID": "reporting",
      "Provider": "postgresql",
      "Backend": {
        "BackendDB": "sqlite3",
        "SQLite3DataFile": "/var/lib/orchestrator/reporting.db"
      },
      "Config": {
        "PostgreSQLTopologyUser": "orchestrator_reporting",
        "PostgreSQLTopologyPassword": "change-me",
        "PostgreSQLSSLMode": "verify-full",
        "InstancePollSeconds": 10,
        "RecoveryPeriodBlockSeconds": 600,
        "DiscoverySeeds": ["pg-reporting1.example.com:5432"],
        "KVClusterMasterPrefix": "reporting/master",
        "ConsulAddress": "consul.example.com:8500"
      }
    }
  ]
}
```

New tenant, safe defaults: it has **no `RecoverMasterClusterFilters`**, so it is discovered and shown but never
failed over automatically. Turn recoveries on later, in a restart of its own, once you have seen it behave.

### Procedure

1. Put the same tenants file on every node, then validate on each (nothing is opened or changed):

   ```shell
   sudo -u orchestrator /usr/local/orchestrator/orchestrator \
     --config /etc/orchestrator/orchestrator.conf.json --ignore-raft-setup -c tenants
   # default	mysql	sqlite3:/var/lib/orchestrator/orchestrator.db
   # reporting	postgresql	sqlite3:/var/lib/orchestrator/reporting.db
   ```

   Do **not** restart anything yet.

2. Restart the nodes **one at a time, followers first, the leader last**, waiting for `Follower`/healthy between
   them. While the leader does not yet have the tenant, nothing for it is replicated (no command of the new tenant
   exists until its leader runs it), so the followers that already have it are safe.

3. What to expect during the window, and that it is **normal**: the leader announces the tenants it runs (global op
   `tenant-set`: tenant IDs and a digest of ID and provider, never credentials) when it is elected and every 5
   minutes. Each node compares it with its own. While the old leader still lacks `reporting`, the upgraded followers
   log

   ```text
   ERROR orchestrator/raft: the tenants of this node (default,reporting) differ from the ones of the leader (...): ...
   ```

   and set `orchestrator_tenant_set_mismatch 1`; that is correct and clears (a few seconds after an election, at most 5
   minutes) once the leader runs the same set. Leave `RaftTenantSetMismatchPolicy` at its default `"warn"` for this
   step; `"yield"` (a leader that runs different tenants than the previous one gives up the leadership) is for
   steady state.

4. Verify on **every** node:

   ```shell
   curl -s localhost:3000/api/tenants | jq '[.Details[] | {ID, Provider, Healthy}]'      # default + reporting, identical on all nodes
   curl -s localhost:3000/metrics | grep -E '^orchestrator_(tenant_set_mismatch|raft_unknown_tenant_commands_total)'   # both 0
   curl -s localhost:3000/api/tenants/reporting/clusters                                  # through any node (followers forward to the leader)
   curl -s localhost:3000/api/clusters                                                    # still the tenant "default"
   ```

   and in the logs one `tenant reporting: continuous discovery: starting` per node and no
   `command NOT applied on this node, tenant unknown here`.

5. Only now enable what you held back for the new tenant (`RecoverMasterClusterFilters`, hooks, ProxySQL), again with
   a restart of the nodes one at a time, and watch `/api/tenants/reporting/...` and the recoveries of that tenant
   only.

From here Raft writes the multi-tenant snapshot (version 2): the cluster cannot go back to an old binary (see the next
section).

## 4. Rollback and what cannot be undone

| From | To | How | Status |
|---|---|---|---|
| Stage 1 (new binary, no tenants file) | old binary | Stop a node, put the old binary back, start it with the same configuration, **same backend and same `RaftDataDir`**; one node at a time. The old binary reads the Raft log and the snapshots the new one wrote (version 1). Followers first. | Tested (mixed suite scenario 5: downgrade of one node, converges). |
| Stage 2 (`default` in a tenants file) | Stage 1 | Reverse the conversion of section 1 on every node: put `BackendDB`, `SQLite3DataFile`/`MySQLOrchestrator*` and the tenant settings back in the node file, drop `TenantsConfigFile`, restart one node at a time. Same data. Needs the new binary: to reach the old one continue as in the first row. | The same data files and Raft entries are used both ways; verified on one node. |
| Stage 3 (second tenant active) | Stage 2 | Remove the tenant from the tenants file of **every** node and restart them one at a time. Its Raft entries are then replayed as unknown-tenant: each is logged (`ERROR ... command NOT applied on this node, tenant unknown here`) and counted (`orchestrator_raft_unknown_tenant_commands_total`), and dropped. The tenant's SQLite file or MySQL database is **left untouched** on disk. | Verified on one node. |
| Stage 3 | old binary | **Not supported and not tested.** Old nodes cannot read version 2 snapshots or the `Tenant` field. If you must, first go back to stage 2 on all nodes; then rebuild each node on its own, one at a time: stop it, delete its `RaftDataDir`, start it with the **old** binary so it re-joins and is sent a version 1 snapshot by the (new, single-tenant, hence version 1 writing) leader. Rehearse it on a copy of the cluster first. | Outline only. |

What cannot be undone:

* **Commands dropped on a node** (unknown tenant, full or broken tenant backend) are never retried there. A node that
  missed them is rebuilt by deleting its Raft data and re-joining it.
* **A command applied by an old node to its only backend** when a second tenant was activated too early: that
  backend now holds another tenant's changes. Restore the node from backup.
* **Retried client calls**: a client that retries after an ambiguous error (timeout, lost leadership) may apply a
  command twice, there is no deduplication.
* **The audit trail and recoveries** of a tenant you remove stay in its own backend; they are not merged into another.
* A tenant's ID is its identity in Raft and in the URLs and metrics: "renaming" it is a new, empty tenant; the old
  one's data stays where it was.

## 5. Known risks

| Risk | Consequence | Mitigation |
|---|---|---|
| A tenants file without a tenant `default` over an existing cluster | The Raft entries and snapshots written so far are those of `default` (legacy entries carry no tenant); they would be dropped as unknown-tenant and the existing data left out of Raft. | Always declare `default` and give it the existing backend. |
| A second tenant activated while an old node is still in the group | The old node applies the new tenant's commands to its only backend and cannot read a multi-tenant snapshot. Reproduced by the mixed-version suite. | Upgrade **all** nodes to the legacy configuration first; verify `AppVersion` on every node; only then section 3. |
| Different tenants (IDs or providers) on different nodes | A node without a tenant drops its commands for good; adding it later recovers nothing. | Same tenants file everywhere; alert on `orchestrator_tenant_set_mismatch == 1` and `orchestrator_raft_unknown_tenant_commands_total` increasing; optionally `RaftTenantSetMismatchPolicy: "yield"`. |
| An older build of the multi-tenant work (before `283f0ce0`) in a mixed group | Version 2 snapshots even for one tenant: an old node restoring one starts empty and re-enables recoveries. | Never go below that commit. |
| Two nodes restarted at once | Quorum lost in a 3-node group. | One node at a time, wait for `Follower`/healthy. |
| Tenants inheriting from the node file | A new tenant runs the first tenant's hooks, credentials, ProxySQL or Consul settings. | Move tenant-scoped settings into `default` (section 1). Settings that clash (same `KVClusterMasterPrefix` on a shared Consul, overlapping ProxySQL hostgroups) are rejected at load. |
| `SIGHUP` no longer applies tenant changes | A change in the tenants file is ignored until a restart. | Restart the node (one at a time in a Raft group) and plan changes accordingly. |
| `RecoveryPeriodBlockSeconds` shorter than a failover under Raft (40-60 s) | The same recovery repeated, hooks run several times. | Keep it at 120 or more. |
| `${ENV}` credentials | Visible to the hooks of every tenant (they inherit the process environment). | `MySQLTopologyCredentialsConfigFile` per tenant, mode `0600`. |
| A tenant's backend failing | The node yields the Raft leadership so another node runs all tenants; the process does not exit. | Monitor `Healthy` in `/api/tenants`; fix the backend. |
| Raft snapshot with large backends | The backends of every tenant are read while Raft command application is blocked, delaying replication. | Size the backends; watch snapshot duration after adding tenants. |
| Enabling Raft late or changing the backend path while migrating | Several changes at once make a failure impossible to attribute. | One change per restart: Raft, then the tenants file, then the second tenant. |
