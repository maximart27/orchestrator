# Multi-tenant orchestrator

One orchestrator process can manage several independent *tenants*: several unrelated MySQL/MariaDB and PostgreSQL
topologies, each with its own configuration, backend, discovery, analysis and recovery, behind a single Raft group,
HTTP server and dashboard. This page is the operational overview; the format of the tenants file and the
classification of every setting are in [Multi-tenant configuration](multi-tenant-config.md).

Without a tenants file nothing changes: the process runs one implicit tenant called `default`, driven by the regular
configuration. Everything on this page applies only when `TenantsConfigFile` is set.

## Model

```
 one process ──┬── tenant "mariadb-a"  (engine: mysql)       backend: its own SQLite file
               ├── tenant "mariadb-b"  (engine: mysql)       backend: its own SQLite file
               └── tenant "postgres-c" (engine: postgresql)  backend: its own SQLite file
 one Raft group, one leader (for ALL the tenants), one HTTP listener, one dashboard
```

* A **tenant** is a logical orchestrator: its own configuration, **one** database engine (`mysql`, which includes
  MariaDB, or `postgresql`), its own backend, discovery queues, caches, recovery state, key-value stores and
  ProxySQL hook. Two tenants share no mutable state: the same `host:port`, or the same cluster name, can exist in
  two tenants without interfering.
* **Raft is global.** There is one Raft group and one leader for the whole process; the leader runs the
  leader-only work (recoveries, caretaking, publishing to the KV stores) of *every* tenant. Raft commands and
  snapshots carry the tenant they belong to.
* The control of the node (heartbeat in `node_health`, access tokens) is global. The heartbeat of the node is
  written to the backend of every tenant.

## Requirements

1. **Raft is mandatory with more than one tenant.** Without Raft the election lives in the `active_node` row of one
   backend, and several backends would give several leaders. The process refuses to start with more than one
   tenant and `RaftEnabled: false`:
   `process: 2 tenants configured but Raft is disabled: ...`.
2. **The same tenants file on every Raft node.** A Raft command names its tenant; a node that does not know it
   cannot apply it (it logs and counts `unknown tenant` commands and does not apply the command to another
   tenant). The tenants file is node-local, so keeping the files equivalent is up to the deployment
   (Ansible, config management...). Tenant IDs, providers and each tenant's configuration should be identical;
   the backend (`Backend` section) is the part that may legitimately differ per node.
3. **Upgrade every node before activating a second tenant (mixed-version risk).** A node of a version without
   multi-tenancy ignores the `Tenant` field of a Raft command and would apply it to its single backend. Roll the
   new version out to *all* nodes first, with no tenants file or only the `default` tenant, and only then add the
   second tenant. Raft logs and snapshots written by a single-tenant node are read as the tenant `default`
   (`LegacyTenantID`); name the tenant that holds the existing data `default`.

   Rolling upgrade of an existing single-tenant cluster (tested by `tests/multitenant/run-mixed.sh`): with the
   legacy configuration (no tenants file) old and new nodes interoperate -- the Raft log entries of the legacy
   tenant are byte for byte the old ones, and a legacy deployment keeps writing the snapshot format that old
   nodes read -- so upgrade the followers first, the leader last, one node at a time. A node can also be
   downgraded while the cluster is still single-tenant. What does **not** interoperate is a second tenant: while
   one node is old, a tenant other than `default` must not exist anywhere (old nodes would apply its commands to
   their only backend, and cannot read a multi-tenant snapshot). Ship the tenants file with the second tenant only
   after every node runs the new version.
4. **Distinct `KVClusterMasterPrefix` for tenants that share a Consul or ZooKeeper**, otherwise the masters of one
   tenant would overwrite the other's. Loading the tenants file fails if two tenants sharing a KV store use the
   same prefix.
5. **Disjoint ProxySQL hostgroups** for tenants that share a ProxySQL admin endpoint. Loading the tenants file
   fails on overlap.
6. **One backend per tenant**, never shared. Loading fails if two tenants resolve to the same SQLite file or the
   same MySQL backend. In this version the backend of a tenant is SQLite (or MySQL); PostgreSQL is supported as a
   *topology* engine of a tenant, **not** as an orchestrator backend.

## Starting the process

`orchestrator http` (and `orchestrator -c continuous`) builds all the tenants **before** anything else starts, and
fails fast: if one of them cannot be initialised (unusable backend, invalid configuration) the process does not
start, and the error names the tenant (`cannot initialise the tenants: tenant "mariadb-b": ...`). An inconsistent
set of tenants is never run.

Startup order, in `orchestrator http`: load the configuration (including `TenantsConfigFile`) → build the tenants
and the control of the node → register the node (`node_health`) → start Raft, the node loop and every tenant
(`TenantManager.Start`) → register the HTTP routes and listen. The global initialisations (Prometheus, metrics
exporters, signal handlers, Raft) happen exactly once, whatever the number of tenants. By default the
tenants run; with `--discovery=false` they are built (the API serves them) but nothing is discovered.

Shutdown: `SIGINT` stops the tenants in order (waiting up to 30 seconds), then exits. `SIGTERM` is handled by the
process-wide signal handler, which audits the shutdown of every tenant and exits. `SIGHUP` reloads the **global**
configuration only (see below).

### Health of a tenant

The health of the node is that of its control backend and of Raft. If the backend of **one tenant** keeps failing,
the node yields the Raft leadership so that another node takes over all the tenants; a broken tenant never makes
the process exit. This is deliberate: there is no "degraded but still leader" state.

### Reload (SIGHUP)

`SIGHUP` (and `reload-configuration`) re-reads the regular configuration file: the global configuration, which is
also the configuration of the legacy `default` tenant. The tenants of a tenants file **keep the configuration
they started with**: restart the process (one Raft node at a time) to change a tenant, or the tenants file itself.

## Command line

A command-line invocation acts on **one** tenant, chosen once at the beginning:

```
orchestrator -c clusters -tenant mariadb-a
ORCHESTRATOR_TENANT=postgres-c orchestrator -c which-cluster -i db1.example.com
orchestrator -c tenants
```

Resolution order: `-tenant <id>` > the `ORCHESTRATOR_TENANT` environment variable > the tenant named `default` >
the only tenant. An unknown tenant, or several tenants with none named `default` and no selection, makes the command
fail and list the available tenants; nothing runs by accident on the wrong one. `orchestrator -c tenants` lists them
(ID, provider and backend, tab separated) without contacting any backend.

The commands about the node, not about a tenant (`active-nodes`, `access-token`), do not need a tenant;
`continuous` runs every tenant. `dump-config` prints the effective configuration of the selected tenant (the global
one if the choice is ambiguous). With Raft enabled the CLI keeps refusing to run, as before: use the API of the
active node (or `orchestrator-client`), or `--ignore-raft-setup` knowing the consequences.

## Hooks

Every recovery hook (`PreFailoverProcesses`, `PostMasterFailoverProcesses`, `PostFailoverProcesses`...) of a tenant
knows which tenant it runs for:

* the environment variable `ORC_TENANT_ID`;
* the placeholder `{tenantId}` in the command line.

Hooks are part of the tenant configuration, so different tenants can run different scripts; when they share one,
use these to tell them apart (`/usr/local/bin/post-failover {tenantId} {successorHost}:{successorPort}`).

## Security between tenants

Tenants are isolated inside the process (configuration, backend, caches, queues), but a hook is a child process of
the orchestrator and **inherits the whole environment of the process**, as it always did. Anything referenced as
`${ENV}` in the configuration of *any* tenant (for instance `"MySQLTopologyPassword": "${ORC_A_TOPOLOGY_PASSWORD}"`)
lives in that environment, so the hooks of tenant B can read the credentials of tenant A. The environment is not
filtered because hooks that rely on it would break.

When the tenants do not trust each other (different teams or customers sharing the node), do not use `${ENV}` for
their credentials: give each tenant its own credentials file (`MySQLTopologyCredentialsConfigFile`, readable only
by the orchestrator user) so the secrets never reach the process environment. Likewise keep the tenants file and
those credentials files at mode `0600`.

## API

All the existing endpoints are available per tenant under a tenant prefix:

| Legacy | Per tenant |
|--------|------------|
| `/api/<endpoint>` | `/api/tenants/{tenantId}/<endpoint>` |
| `/api/v2/<endpoint>` | `/api/v2/tenants/{tenantId}/<endpoint>` |
| `/web/<page>` | `/web/tenants/{tenantId}/<page>` |

The routes without prefix keep working and resolve to the tenant `default` (or to the only tenant); with several
tenants and none named `default` they answer `400` and ask for the tenant. The tenant may also be given in the
`X-Orchestrator-Tenant` header or the `tenant` query parameter; the path wins over the header, and the header over
the query. An unknown tenant is a `404`. `GET /api/tenants` lists them. The objects returned carry a `tenantId`.

Identifiers are only unique **within** a tenant (`host:port`, the cluster name, recovery and maintenance IDs):
always send the tenant along with them. Requests that reach a follower are forwarded to the leader unchanged, tenant
included. Raft and node endpoints (`raft-*`, `leader-check`, `health`, `grab-election`, `reelect`...) are global and
take no tenant.

Authentication and authorisation (`AuthenticationMethod`, `ReadOnly`, `PowerAuthUsers`) are **global**: the
same users can act on every tenant.

This section describes the contract implemented by the HTTP layer; see [the API documentation](using-the-web-api.md) for the
endpoints themselves.

## Metrics

The tenant-scoped Prometheus metrics (discoveries, instances, recoveries, queue lengths...) carry a `tenant` label,
registered once; process-wide metrics (health, Raft) have none. In the legacy setup the label is `default`.

## Known limitations

* **SIGHUP only reloads the global configuration.** Neither `SIGHUP` nor `reload-configuration` re-reads the tenants
  file or changes a running tenant; adding, removing or changing a tenant means restarting the node (one Raft
  node at a time).
* **Agents and seeds are outside this version.** The agents listener (`ServeAgentsHttp`) and the agents poll work
  on the legacy single-tenant backend only; the process refuses to start with `ServeAgentsHttp` and a tenants
  file. Seeds are not multi-tenant either.
* **The Raft snapshot reads the backends in the apply goroutine.** While a snapshot is being taken, the reading of
  the backend of every tenant stops the application of Raft commands (`Apply` is blocked) for as long as it lasts.
  With big or slow backends this delays the replication of every tenant on that node.
* **`RecoveryPeriodBlockSeconds` must be longer than a failover with Raft.** A failover under Raft takes 40-60
  seconds; if the block period is shorter than that, the recovery is considered expired before it finishes and is
  repeated. The default (3600) is fine; if a tenant lowers it, keep it at **120 seconds or more** in multi-tenant
  setups.
* **`${ENV}` credentials are visible to every hook.** Hooks inherit the environment of the process, so anything a
  tenant references as `${ENV}` can be read by the hooks of all the tenants (see
  [Security between tenants](#security-between-tenants)).
* **One authentication realm** for all tenants (no per-tenant users or roles); `ReadOnly` and `PowerAuthUsers`
  are global.
* **No PostgreSQL as orchestrator backend**: SQLite or MySQL. PostgreSQL is only a topology engine.
* **`disable-global-recoveries` is per tenant**, not per process.
* **No hot upgrade** from a version without multi-tenancy while a second tenant is active (see Requirements).

## Verifying

```
orchestrator -c tenants                                   # the tenants this node will run
orchestrator -c clusters -tenant mariadb-a                # reads the backend of that tenant only
curl -s localhost:3000/api/tenants/mariadb-a/clusters     # same, through the API
curl -s localhost:3000/api/raft-state                     # one Raft group
```

and, in the logs of the node, one `cannot initialise the tenants` error if something is wrong, or none and the
usual discovery lines per tenant.
