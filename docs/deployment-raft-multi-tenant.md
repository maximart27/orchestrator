# Orchestrator deployment: raft, multi-tenant

This text describes how to deploy a [multi-tenant](multi-tenant.md) orchestrator on a [raft](raft.md) group: three or
five nodes, each one running the same N tenants. It complements [deployment: raft](deployment-raft.md) (which still
applies: one backend per node, a proxy in front, the same node scenarios) and does not repeat it; it covers what is
different when one Raft group carries several tenants.

Related pages: [Multi-tenant orchestrator](multi-tenant.md) (the model and its known limitations),
[Multi-tenant configuration](multi-tenant-config.md) (format of the tenants file),
[MySQL/MariaDB backend](multi-tenant-mysql-backend.md), [Operations runbook](multi-tenant-operations.md),
[Installing the multi-tenant orchestrator](install-multi-tenant.md), [Upgrading to multi-tenant](upgrade-multi-tenant.md)
and [Docker and Compose](docker-multi-tenant.md).

Raft is **mandatory** as soon as there is more than one tenant: the process refuses to start with several tenants and
`RaftEnabled: false`.

## Topology

```
              HAProxy / load balancer  (health check: /api/leader-check)
                          |
        +-----------------+-----------------+
        |                 |                 |
   orchestrator n1   orchestrator n2   orchestrator n3        raft: n1 <-> n2 <-> n3  (DefaultRaftPort)
   tenants: a b c    tenants: a b c    tenants: a b c
   backends of n1    backends of n2    backends of n3         (never shared between nodes)
        \                 |                 /
         +---- every node probes the topologies of ALL tenants ----+
```

* **Three nodes** tolerate the loss of one; **five** tolerate two (quorum 2 of 3, 3 of 5). Use an odd number, spread
  over failure domains (hosts, racks, data centers) as for any Raft deployment: see [raft](raft.md).
* **One leader for all the tenants.** There is no per-tenant leader: the same node runs the recoveries of every
  tenant, and when it loses the leadership it loses it for all of them.
* **Every node runs every tenant.** Each node has, per tenant, its own backend and its own discovery, and probes every
  topology server of every tenant: with three nodes each MySQL/PostgreSQL server is visited by three orchestrators, as
  in single-tenant Raft. Size the probing load as `nodes x (sum of the instances of all tenants)`.
* **Each node has its own backends, never shared with another node** (SQLite files, or one MySQL database per node
  and tenant, see [MySQL/MariaDB backend](multi-tenant-mysql-backend.md)). Raft replicates *commands* and every node
  applies them to its own backend; two nodes sharing one backend would apply every command twice to the same rows.
* The Raft port (`DefaultRaftPort`) must be open between the nodes and to nobody else; the HTTP port to the load
  balancer and to the operators.

## The same tenants file on every node

The tenants file (`TenantsConfigFile`) is **local to each node**: nothing distributes it. A Raft command names its
tenant; a node that does not know that tenant cannot apply the command, logs it, and never applies it later (it is not
retried). So **every node must run the same set of tenants**:

| Must be identical on every node | May legitimately differ per node |
|---|---|
| tenant `ID`s and `Provider`s | the `Backend` section (SQLite file, MySQL host/database/user/password) |
| the tenant's `Config` (credentials towards the topology, hooks, filters, recovery policy, KV prefix, ProxySQL...) | |

Generate the file with configuration management (Ansible, Puppet...) from one template and substitute only the
backend of each node. Keep it, and the credentials files it refers to, at mode `0600`.

**Order of startup of a node** (the same for every node): load the configuration and the tenants file -> build every
tenant, deploying each backend (`cannot initialise the tenants: tenant "<id>": ...` and the process exits if one fails)
-> register the node -> start Raft, the node loop and the tenants -> serve HTTP. A node whose backends are broken
never reaches Raft: it does not join the group half-initialised.

**What happens if the files differ.** The leader announces the tenants it runs (Raft op `tenant-set`: sorted IDs and a
digest of IDs and providers, never credentials) when it is elected and every 5 minutes. Each node compares it with its
own set; on a difference it logs an `ERROR` that names the tenants missing here, only here, or with another provider,
and raises the gauge `orchestrator_tenant_set_mismatch` (Prometheus) / `raft.tenant_set_mismatch` (go-metrics) to 1. The
node-local `RaftTenantSetMismatchPolicy` decides what else happens:

* `"warn"` (default): nothing else. The node keeps running and keeps applying the commands of the tenants it knows.
* `"yield"`: a node that is the **leader** and runs different tenants than the previous leader announced gives up the
  leadership (a follower only warns). It stops a newly elected node with a stale tenants file from running recoveries
  for a subset of the tenants.

Neither policy recovers the commands already dropped: a node that did not know tenant `b` while commands of `b` were
replicated never gets them by adding `b` afterwards. The remedy is to [rebuild the node](multi-tenant-operations.md#rebuilding-a-node).
Nodes of a version older than the multi-tenant one answer `Unknown command op` to the announcement and keep working;
they cannot report a mismatch (see [Upgrading](upgrade-multi-tenant.md)).

Compare the tenants of the nodes without waiting for the announcement: `GET /api/tenants` on each node (ID and
provider must match), or `orchestrator -c tenants --ignore-raft-setup` (reads the configuration only, opens no backend).

## Raft configuration of a node

The Raft settings are the usual ones ([configuration: raft](configuration-raft.md)); what changes is only that the
node configuration also names the tenants file, and that the control backend of the node (the plain `BackendDB`
settings) is separate from the backends of the tenants. The node configuration of `n1` of a three-node group:

```json
{
  "ListenAddress": ":3000",
  "HTTPAdvertise": "http://10.0.0.1:3000",
  "BackendDB": "sqlite",
  "SQLite3DataFile": "/var/lib/orchestrator/control.db",
  "RaftEnabled": true,
  "RaftDataDir": "/var/lib/orchestrator/raft",
  "RaftBind": "10.0.0.1",
  "RaftAdvertise": "10.0.0.1",
  "DefaultRaftPort": 10008,
  "RaftNodes": ["10.0.0.1", "10.0.0.2", "10.0.0.3"],
  "RaftTenantSetMismatchPolicy": "warn",
  "TenantsConfigFile": "/etc/orchestrator/tenants.json"
}
```

`n2` and `n3` differ only in `HTTPAdvertise`, `RaftBind`, `RaftAdvertise`, and (with MySQL backends) the backend
settings. For five nodes list the five addresses in `RaftNodes` on all of them. `RaftAdvertise` and `HTTPAdvertise`
are needed behind NAT or port mapping; the nodes reverse-proxy to the leader through `HTTPAdvertise`.

`SQLite3DataFile` here is the **control** backend of the node (`node_health`, access tokens): it must not be the file of
any tenant nor `<RaftDataDir>/raft_store.db`; loading the tenants file fails otherwise. With MySQL backends the
equivalent is the database of the control backend, which no tenant may use either.

The tenants file, identical on the three nodes except for the backends:

```json
{
  "Tenants": [
    {
      "ID": "mariadb-a",
      "Provider": "mysql",
      "Backend": { "BackendDB": "sqlite", "SQLite3DataFile": "/var/lib/orchestrator/mariadb-a.db" },
      "Config": {
        "MySQLTopologyUser": "orchestrator-a",
        "MySQLTopologyPassword": "${ORC_A_TOPOLOGY_PASSWORD}",
        "InstancePollSeconds": 5,
        "RecoverMasterClusterFilters": ["*"],
        "RecoveryPeriodBlockSeconds": 300,
        "KVClusterMasterPrefix": "mariadb-a/master",
        "PostMasterFailoverProcesses": ["/usr/local/bin/post-failover {tenantId} {successorHost}:{successorPort}"]
      }
    },
    {
      "ID": "mariadb-b",
      "Provider": "mariadb",
      "Backend": { "BackendDB": "sqlite", "SQLite3DataFile": "/var/lib/orchestrator/mariadb-b.db" },
      "Config": {
        "MySQLTopologyUser": "orchestrator-b",
        "MySQLTopologyPassword": "${ORC_B_TOPOLOGY_PASSWORD}",
        "InstancePollSeconds": 10,
        "RecoverMasterClusterFilters": [".*"],
        "RecoveryPeriodBlockSeconds": 300,
        "KVClusterMasterPrefix": "mariadb-b/master"
      }
    },
    {
      "ID": "postgres-c",
      "Provider": "postgresql",
      "Backend": { "BackendDB": "sqlite", "SQLite3DataFile": "/var/lib/orchestrator/postgres-c.db" },
      "Config": {
        "PostgreSQLTopologyUser": "orchestrator-c",
        "PostgreSQLTopologyPassword": "secret-c",
        "PostgreSQLSSLMode": "verify-full",
        "InstancePollSeconds": 5,
        "RecoverMasterClusterFilters": [".*"],
        "RecoveryPeriodBlockSeconds": 300
      }
    }
  ]
}
```

`${ORC_A_TOPOLOGY_PASSWORD}` is read from the environment of the process (see "Security between tenants" in
[multi-tenant](multi-tenant.md#security-between-tenants) before using it for tenants that do not trust each other).
The format, the allowed settings and the cross-tenant validation (distinct backends, KV prefixes, ProxySQL hostgroups)
are in [Multi-tenant configuration](multi-tenant-config.md).

### Anti-flapping: `RecoveryPeriodBlockSeconds`

`RecoveryPeriodBlockSeconds` (a tenant-scoped setting) must be **longer than the duration of a failover under Raft**,
which takes 40-60 seconds in the tests; **120 seconds or more** is the recommendation (the examples above use 300). If
the block period is shorter than the failover, the recovery is considered expired before it finishes and the same
recovery is started again: the chaos suite measured four recovery rows and four hook runs for one failover with a block
of 5 seconds. The default (3600) is safe.

It is also what bounds the outage when a leader is killed in the middle of a failover (see below): the new leader waits
the block period before repeating the recovery from scratch.

## Load balancer in front

Put a proxy in front exactly as for single-tenant Raft ([raft: proxy, leader](raft.md#proxy-leader)):

* **Health check `/api/leader-check`**: at most one node answers `200`; the others answer `404`
  (`/api/leader-check/503` to get another code). Send traffic only to the node that passes it.
* Or, the relaxed variant, check **`/api/raft-health`** (`200` healthy, `500` not part of a healthy group) and talk to
  any healthy node: the followers reverse-proxy to the leader, tenant included (path `/api/tenants/<id>/...`, header
  `X-Orchestrator-Tenant` or query parameter `tenant` travel untouched and the leader resolves the tenant again).

`leader-check`, `raft-health`, `health`, `raft-state` and the other node endpoints are global: they take no tenant.

**The v2 API does not go through the proxy to the leader.** `/api/v2/...` and `/api/v2/tenants/{tenantId}/...` are
answered by the node that receives the request from *its own* backend (all v2 endpoints only read); a follower does not
forward them. Behind a leader-only load balancer this is invisible (the node that receives them is the leader), but if you
talk to the followers directly, or use the relaxed `raft-health` variant, the answers of two nodes can differ slightly
because each node has its own discovery state.

Clients that must act on the nodes themselves (`orchestrator-client`) can be given the list of all node APIs and will
find the leader, as in [deployment: raft](deployment-raft.md#what-to-deploy-client). The `orchestrator` command line
refuses to run with Raft enabled (`--ignore-raft-setup` at your own risk): use the API, always naming the tenant.

## What runs only on the leader

One leader for all the tenants means the leader-only work of **every** tenant runs on the same node:

* the **recoveries** of all tenants (failure detection runs on every node that is part of the quorum, but only the leader
  starts a recovery) and their **hooks** (`PreFailoverProcesses`, `PostMasterFailoverProcesses`, ...: they run only on the
  leader, so only its disk shows their effects; each hook gets `ORC_TENANT_ID` and the `{tenantId}` placeholder);
* publishing the masters to the **key-value stores** (Consul, ZooKeeper, internal), the **ProxySQL** hook, the **Pseudo-GTID**
  injection, and the replication of the commands (`begin-downtime`, `put-instance-tag`, `ack-recovery`, ...) as Raft entries;
* user changes through the API: the followers forward them to the leader.

All the nodes that are part of the quorum, in contrast, discover (probe) the topologies of all tenants, run failure
detection and register their own health in every backend. A node that loses the quorum stops acting as leader at once and
no longer runs recoveries or hooks.

## Behaviour under failures

Everything in this section was provoked with real containers by the chaos suite ([README-chaos](../tests/multitenant/README-chaos.md)),
which runs three nodes and three tenants. Times are those measured there, on a small host; treat them as orders of magnitude.

| Event | What happens | What you do |
|---|---|---|
| **A follower dies or is partitioned** | The leader is unchanged. Commands issued meanwhile (tags, downtimes, acknowledgements) reach the other follower at once and the isolated one after healing, only in their own tenant. | Nothing. Start the node; it rejoins and catches up from the log or a snapshot. |
| **The leader dies or is cut off** (also when it still reaches every database) | The majority elects a new leader; the isolated old one stops being leader within about 1 second and starts no recovery or hook; after healing it comes back as a follower and the tenants converge with one recovery and one hook per failed master. | Nothing; check `raft-state` afterwards. |
| **The leader is paused** (frozen process, `SIGSTOP`, a stalled VM) | On waking up it learns it is no longer leader and runs nothing. A recovery that was in flight stops when the leader loses the term (a few seconds of staleness before it notices are not covered). A pause longer than 30 s is not mistaken for a broken control backend (the count of unhealthy time restarts at the wake-up). | Nothing; look at the log if a recovery was in flight (see the runbook). |
| **`kill -9` of the leader in the middle of a failover** | The new leader repairs the topology, but only after `RecoveryPeriodBlockSeconds`: with a 60 s block and a 20 s pre-failover hook the topology was repaired 86 s after the kill; **the pre-failover hook ran twice** (once per leader), the post hook once. Make hooks idempotent. | Pick the block period for the outage you can tolerate (>= 120 s), and write idempotent hooks. |
| **The backend of ONE tenant breaks** (disk full, server down, wrong grants) | The leader keeps failing to write that tenant, **yields the leadership after about 5 seconds** (`orchestrator/raft: yielding leadership: backend of tenant <id> unhealthy`) and another node takes over **all** the tenants; the broken process stays alive, never exits because of a tenant, and heals when its backend does. There is no "degraded but still leader" state, by design. | Repair that tenant's backend on that node. Commands it could not apply meanwhile are lost for that node: [rebuild it](multi-tenant-operations.md#rebuilding-a-node). |
| **The control backend of the node breaks** | Same yield after about 5 seconds; after about 30 seconds of real unhealthiness (not a pause) the process gives up (`node is unable to register health ...`). | Repair it; restart the process if it exited. |
| **Rolling restart under traffic** | One node at a time: no accepted command was lost in the tests (2944 of 2944 on three tenants). | Wait for each node to be healthy (`raft-health` = `healthy`, `raft-state` = `Follower` or `Leader`) before the next; restart the leader last. |
| **A node restarts, or is rebuilt from nothing** | It restores the latest Raft snapshot into its backends (or the leader installs one if the log is already compacted). | Nothing for a restart; see below for the cost. |
| **The tenants of a node differ** | `tenant unknown here` per dropped command, `orchestrator_tenant_set_mismatch` = 1, the node otherwise keeps working. | Fix the tenants file and [rebuild](multi-tenant-operations.md#rebuilding-a-node) the node. |

### Snapshots and their cost

Raft compacts its log with snapshots; here a snapshot is the state of **every tenant**, taken by reading the backends in
the same goroutine that applies the commands. The relevant fixed values (`go/raft/store.go`, vendored hashicorp/raft
defaults; none is configurable):

* `TrailingLogs` 10240: a follower that is behind more than that many entries cannot be fed from the log, the leader installs a
  snapshot instead;
* Raft takes a snapshot when something has been committed since the last one (`SnapshotThreshold` 1), checked about every 30
  minutes (`SnapshotInterval`); besides, after a recovery has written its key-value pairs the leader publishes
  `async-snapshot` and every node takes a snapshot at a random moment within the next minute, and `/api/raft-snapshot` takes
  one on the node that receives the call; the last 10 snapshots are kept.

**While a snapshot is being taken, the application of Raft commands is blocked** (`Apply`) for the whole duration: with
big or slow backends it delays the replication of *every* tenant on that node. Measured with three tenants on SQLite
backends in one process (`TestSnapshotCost` in `go/logic/snapshot_cost_test.go`, one run on a development host): about
**0.65 s** with 20000 instances per tenant and about **1.7 s** with 50000 per tenant. Those numbers are for local SQLite
files; a MySQL backend adds network and server time to every table read, so measure yours (`/api/raft-snapshot` while
watching the log) before sizing the number of tenants per group.

On restore, the tables written only by Raft are replaced by the snapshot's content, atomically per tenant (a row deleted
while the node was down does not survive); the tables the node also writes itself are only added to.

### Cascading restarts

Restarting several nodes at once (a power cut, a bad rollout) is safe for data but not free: each node rebuilds its tenants
(on MySQL backends the first start of a new database deploys the schema, about 5 seconds per database), restores its latest
snapshot and re-joins. After the group is up again, check that every tenant answers on every node and that no recovery stays
in its active period beyond `RecoveryPeriodBlockSeconds` (see the symptom table of the
[runbook](multi-tenant-operations.md#symptoms-causes-actions)).

### A client retry can apply a command twice

There is no deduplication of Raft commands. A client that retries after an *ambiguous* error (a timeout, or the leader
losing the term in the middle of the call) may apply the same command twice (a downtime, a tag, an acknowledgement).
Most commands are idempotent in effect; do not blindly retry the ones that are not, and look at the state before retrying.

## Checklist before going live

* The same `tenants.json` (ID, provider, `Config`) on every node; only `Backend` differs. `GET /api/tenants` agrees on all.
* `RaftTenantSetMismatchPolicy` decided (`warn`, or `yield` if a newly elected node with a wrong file is worse than a
  leadership change), and an alert on `orchestrator_tenant_set_mismatch` (see [operations](multi-tenant-operations.md#monitoring)).
* `RecoveryPeriodBlockSeconds` >= 120 in every tenant; hooks idempotent.
* Load balancer checks `/api/leader-check`; clients name the tenant in every call.
* Every node can reach every topology server of every tenant (a node that cannot probe a master sees it dead; test the
  network from each node, not only from the current leader).
* Distinct `KVClusterMasterPrefix` and ProxySQL hostgroups per tenant when sharing a Consul/ZooKeeper/ProxySQL.
