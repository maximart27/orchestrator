# Multi-tenant orchestrator: operations runbook

Day-two operations of a [multi-tenant](multi-tenant.md) orchestrator on a Raft group
([deployment](deployment-raft-multi-tenant.md)): what to watch, how to add, remove or change a tenant, how to rebuild a node,
how to handle recoveries per tenant, and what the symptoms mean. Everything here comes from the code and from the Raft
chaos suite (`tests/multitenant/README-chaos.md`); where a statement is a reading of the code and was not provoked in a
test, it says so.

Conventions: `$NODE` is the HTTP address of a node (`http://10.0.0.1:3000`), `$T` a tenant ID. Tenant-scoped calls are
`$NODE/api/tenants/$T/<endpoint>`; the global ones (`health`, `raft-*`, `leader-check`, `tenants`) take no tenant.
Authentication and `ReadOnly` are global, and endpoints that act (`ack-recovery`, `disable-global-recoveries`, `raft-yield`...)
need the same authorisation as in single-tenant mode.

## Monitoring

### Endpoints

| Endpoint | Scope | What it tells you |
|---|---|---|
| `GET /api/health` | node | The health of the node (control backend, Raft). `Details.Tenants[]` lists every tenant with `ID`, `Provider`, `Default`, `Healthy`, `Message` (the last heartbeat error of its backend, empty when healthy) and `LastGood` (last good heartbeat into its backend). A tenant that fails does **not** change the health of the node. |
| `GET /api/tenants` | node | The same per-tenant status; `Details` is the list. Compare it across the nodes: IDs and providers must be equal. |
| `GET /api/raft-state` | node | `Leader`, `Follower`, `Candidate`... of this node. |
| `GET /api/raft-health` | node | `200` `"healthy"` if the node is part of a healthy group, `500` otherwise (also right after start and during a re-election). |
| `GET /api/leader-check` | node | `200` only on the leader, `404` elsewhere (`/api/leader-check/503` for another code). The load-balancer check. |
| `GET /api/raft-status`, `/api/raft-peers`, `/api/raft-leader` | node | Bind/advertise address, state, quorum membership, leader URI, peers. |
| `GET /api/raft-snapshot` | node | Takes a snapshot **on the node that receives the call**. Useful to time it and to compact the log. |
| `GET /api/tenants/$T/blocked-recoveries`, `/audit-recovery`, `/replication-analysis`, `/check-global-recoveries` | tenant | Blocked and past recoveries, current analysis, whether recoveries are enabled for the tenant. |
| `GET /metrics` | node | Prometheus (`PrometheusEnabled` is on by default; the path honours `URLPrefix`). |

`GET /api/health` is the one to poll per node: it also answers whether the backend of each tenant on that node is
being written.

### Prometheus metrics

Process-wide metrics have **no** `tenant` label; the tenant-scoped ones carry `tenant` (in a legacy setup the value is `default`).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `orchestrator_tenant_set_mismatch` | gauge | | 1 if this node's tenants differ from the ones the leader announced (see [deployment](deployment-raft-multi-tenant.md#the-same-tenants-file-on-every-node)). |
| `orchestrator_raft_unknown_tenant_commands_total` | counter | | Raft commands this node could not apply since it started because their tenant is unknown here. Any increase means commands were lost on this node. |
| `orchestrator_raft_is_leader` | gauge | | 1 on the leader. Exactly one node of the group must have it. |
| `orchestrator_raft_is_healthy` | gauge | | 1 if Raft is enabled and this node is healthy. |
| `orchestrator_raft_is_part_of_quorum` | gauge | | 1 if the node is part of the quorum. |
| `orchestrator_raft_peers_total` | gauge | | Number of known Raft peers. |
| `orchestrator_health_is_ready`, `orchestrator_health_is_live` | gauge | | Readiness (last node-health registration succeeded) and liveness. |
| `orchestrator_discoveries_total`, `orchestrator_discovery_errors_total` | counter | `tenant` | Discovery attempts and failures. |
| `orchestrator_instances_total`, `orchestrator_clusters_total` | gauge | `tenant` | Known instances and clusters. |
| `orchestrator_discovery_queue_length`, `orchestrator_dead_instances_discovery_queue_length` | gauge | `tenant` | Discovery queues. |
| `orchestrator_recoveries_total` | counter | `tenant`, `type`, `result` | Recoveries by analysis type; `result` is `success` or `failed`. Emitted by the node that ran the recovery (the leader). |
| `orchestrator_recovery_duration_seconds` | histogram | `tenant` | Duration of recoveries. |
| `orchestrator_active_recoveries` | gauge | `tenant` | Recoveries in their active period. |
| `orchestrator_active_topology_issues` | gauge | `tenant`, `issue_type` | Current analysis findings by type. |
| `orchestrator_downtimed_instances_total` | gauge | `tenant` | Instances in downtime. |

With go-metrics/Graphite the node-level names are `raft.tenant_set_mismatch`, `raft.unknown_tenant_commands`,
`raft.is_leader`, `raft.is_healthy`, `elect.is_elected` and `health.is_healthy`; the tenant-scoped ones keep their
historical name for the tenant `default` and are prefixed `tenants.<id>.` for every other tenant (for example
`tenants.mariadb-a.discoveries.attempt`, `...discoveries.fail`, `...recover.dead_master.start|success|fail`,
`...recover.pending`).

### Suggested alerts

| Alert | Expression (Prometheus) | Why |
|---|---|---|
| Tenant sets differ | `max_over_time(orchestrator_tenant_set_mismatch[10m]) == 1` | A node that does not run the same tenants drops commands for good. The announcement is every 5 minutes, hence the window. |
| Commands dropped | `increase(orchestrator_raft_unknown_tenant_commands_total[1h]) > 0` | The node needs to be [rebuilt](#rebuilding-a-node) once the cause is fixed. |
| No or several leaders | `sum(orchestrator_raft_is_leader) != 1` for 2m | Group without leader, or a split-brain symptom. |
| Leader flapping | `changes(orchestrator_raft_is_leader[30m]) > 3` | A node that keeps yielding (a tenant backend flapping, a mismatch with policy `yield`, an unstable network). |
| Node out of the group | `orchestrator_raft_is_part_of_quorum == 0` or `orchestrator_raft_is_healthy == 0` for 2m | |
| Tenant backend failing | `/api/health` has a tenant with `Healthy: false` for more than 1 minute (probe it, or alert on the log line `yielding leadership: backend of tenant`) | The leader yields after about 5 s; a tenant that keeps failing on every node is a backend outage. |
| Tenant discovery degraded | `rate(orchestrator_discovery_errors_total[5m]) / rate(orchestrator_discoveries_total[5m]) > 0.2` per tenant | |
| Tenant lost its instances | `orchestrator_instances_total == 0` for 5m (tenant label) | Discovery of that tenant is dead (credentials, network, backend). |
| Recovery stuck | `orchestrator_active_recoveries > 0` for longer than `RecoveryPeriodBlockSeconds` + 5m | See the symptom table. |
| Failed recoveries | `increase(orchestrator_recoveries_total{result="failed"}[15m]) > 0` | |
| Backend connections | `Threads_connected` / `max_connections` on the MySQL server > 0.8 | See [MySQL backend: sizing](multi-tenant-mysql-backend.md#connections). |

Also watch the log of every node for `ERROR` lines starting `orchestrator/raft:` (the tenant-set and command-drop
messages are `ERROR`).

## Tasks

Common rules for every task that changes the tenants or their configuration:

* **`SIGHUP` does not reload tenants.** `SIGHUP` and `reload-configuration` re-read the *global* configuration only; the
  tenants keep the configuration they started with. Any change to the tenants file means **restarting the nodes, one at
  a time**.
* **Raft must be healthy before each restart**: all the nodes up, `raft-health` = `healthy` on each, one leader
  (`sum(orchestrator_raft_is_leader) == 1`). In a three-node group a second node restarted while the first is down loses
  the quorum. Do not start if a recovery is in progress (`orchestrator_active_recoveries` and `audit-recovery`).
* **Order: followers first, the leader last**, one node at a time; wait for the restarted node to be `Follower` and healthy
  (`raft-state`, `raft-health`, `/api/tenants`) before the next. To move the leadership before touching the leader, use
  `GET $LEADER/api/raft-yield/<address of the node to hand it to>` or just restart it.
* The tenants file must end up **identical** (ID, provider, `Config`) on all nodes.

### Add a tenant

1. Prepare what the tenant needs: its topology credentials on the monitored servers (see
   [multi-tenant configuration](multi-tenant-config.md)), its backend on **every** node (the database and grants, or the
   SQLite directory; see [MySQL backend](multi-tenant-mysql-backend.md)), its KV prefix and ProxySQL hostgroups if the
   tenant shares Consul/ZooKeeper/ProxySQL with others, and its hook scripts **on every node** (any node can become leader).
2. Write the new `tenants.json` and check it offline on each node, without touching a backend:
   `orchestrator -config /etc/orchestrator/orchestrator.conf.json -c tenants --ignore-raft-setup`
   (prints `ID`, provider and backend per tenant; a duplicated ID, a shared backend or an overlapping hostgroup is an error).
3. Set `"RaftTenantSetMismatchPolicy": "warn"` on all the nodes for the duration of the rollout (the default). *From reading
   the code, not provoked in a test:* with `"yield"`, a newly elected leader that already runs the new tenant while the
   announcement of the previous leader (valid for 10 minutes) did not include it gives up the leadership again, so the
   leadership can bounce during the rollout.
4. Restart the nodes **followers first, the leader last**. While the leader still runs the old file nothing is published for
   the new tenant (only the leader publishes commands), so followers that already run it have nothing to drop. When the
   leader is restarted, a node that already has the tenant takes over. Do not restart nodes in another order and do not
   use the new tenant (no API calls on it) until **all** the nodes run it: a command published while a node lacks the
   tenant is lost on that node for good.
5. Verify on every node: `GET /api/tenants` lists it with `Healthy: true`; `orchestrator_tenant_set_mismatch` is 0;
   `orchestrator_raft_unknown_tenant_commands_total` did not grow; the log has `tenant $T: continuous discovery: starting`;
   after a few poll intervals `GET /api/tenants/$T/clusters` returns the topology on each node.
6. Set the mismatch policy back if you changed it.

If a node did miss commands of the new tenant, [rebuild it](#rebuilding-a-node).

### Remove a tenant

1. Stop its activity: `GET $NODE/api/tenants/$T/disable-global-recoveries` (per tenant, so no recovery runs for it).
2. Remove it from the tenants file and restart the nodes, followers first, the leader last. Until the last node is done,
   the nodes that still have it keep applying its commands and the others log `tenant unknown here` for each one and count
   them in `orchestrator_raft_unknown_tenant_commands_total` (harmless for a tenant that is being retired, but expect the
   mismatch gauge to be 1 and ignore those alerts for the window).
3. After the last node: take a final backup if you need one, drop the tenant's backends (SQLite files, MySQL databases)
   and its grants, and clean what orchestrator wrote outside itself and does not delete: KV entries under its
   `KVClusterMasterPrefix`, ProxySQL hostgroup rows, its hook scripts.
4. Do not reuse the tenant ID with the old backend in place: it would resume with the old state.

A snapshot taken before the removal may still contain a section for the tenant; a node without it logs
`raft snapshot restore: tenant <id> is in the snapshot but has no runtime on this node; skipping it`, which is expected.

### Rotate credentials of a tenant

The configuration of a tenant is read at start, so a credential change is a rolling restart. To avoid an outage while the
nodes run a mix of old and new credentials, make **both valid during the rollout** and use a *new user* rather than changing
the password of the old one in place:

1. Create the new user on the monitored servers (topology user, `MySQLTopologyUser`/`PostgreSQLTopologyUser`) with the same
   grants as the old one, or the new backend user (backend, `Backend.MySQLOrchestratorUser`).
2. Change `Config`/`Backend` in the tenants file (or the credentials file it points to, or the environment variable it
   references with `${ENV}`; prefer a `0600` credentials file, see [security between tenants](multi-tenant.md#security-between-tenants)) and restart
   the nodes, followers first, the leader last. A rollout in which nodes hold different credentials is not a mismatch (the
   tenant-set digest covers IDs and providers only) and has no effect on Raft.
3. Check discovery on every node (`orchestrator_discovery_errors_total`, `Healthy` in `/api/health`; `Access denied` in the log means a node
   still has the wrong credential).
4. Drop the old user.

A shared backend user (the pattern grant of the MySQL layout) is used by every node and tenant: rotate it with a new user as
above, never with `ALTER USER` followed by a rolling restart. HTTP authentication (`AuthenticationMethod`, `PowerAuthUsers`)
is global and not per tenant.

### Change a hook

Hooks (`PreFailoverProcesses`, `PostMasterFailoverProcesses`, `PostFailoverProcesses`, ...) are part of the tenant
configuration. Hooks run **only on the leader**, but any node can become leader, so:

1. Deploy the new script (if it is a file) **to every node first**; a changed script on disk needs no restart, a changed
   command line in the tenants file does.
2. Change the command line in the tenants file on every node and restart them, followers first, the leader last, when no
   recovery is running. A recovery that starts meanwhile uses the command line of the node that is leader at that moment.
3. Hooks know their tenant (`ORC_TENANT_ID`, `{tenantId}`) and are not deduplicated: if the leader dies in the middle of a
   failover, the new leader repeats the recovery after `RecoveryPeriodBlockSeconds` and the pre-failover hook ran twice in
   the test. Keep hooks **idempotent**.

## Rebuilding a node

**When.** A node applied commands incompletely and nothing will ever retry them:

* the log says `command NOT applied on this node, tenant unknown here` / `orchestrator_raft_unknown_tenant_commands_total`
  grew (the node did not run a tenant of the group), or
* a tenant backend of the node was unusable while commands arrived (disk full, server down, missing grants): the node
  answered an error to Raft and moved on, and later snapshots are built from the backend that had missed them; or
* the tenants of the node were different and `orchestrator_tenant_set_mismatch` was 1 for a while; or
* you are moving the node to another kind of backend (see [MySQL backend](multi-tenant-mysql-backend.md#moving-a-tenant-from-sqlite-to-mysql)), or
  it must be re-provisioned.

Adding the missing tenant afterwards, or fixing the backend, does **not** recover what was dropped (verified: after the
tenant was added back and the node restarted, the commands lost meanwhile were still missing; wiping and re-joining the node
fixed it). A restart alone does not help either: the node restores a snapshot made from its own incomplete state.

**How** (one node at a time; the group must keep its quorum without this node):

1. Fix the cause first: the tenants file identical to the others, the backend working and with space, the grants right.
2. Check the rest of the group is healthy (`raft-health`, one leader, no recovery in progress). If the node is the leader,
   hand the leadership over first (`GET $NODE/api/raft-yield/<other node address>`) or just stop it.
3. Stop the orchestrator on the node (`SIGINT` stops the tenants in order, waiting up to 30 seconds; `SIGTERM`, what a
   service manager sends, audits the shutdown and exits).
4. Delete its **Raft data**: the contents of `RaftDataDir` (`raft_store.db` and `snapshots/`). With SQLite backends also
   delete the node's database files (control and one per tenant); with MySQL backends drop the node's databases
   (`DROP DATABASE IF EXISTS` for each of its `orc_<node>_...` databases; the orchestrator recreates them, which needs the
   `CREATE` privilege of the grants). This is what `chaos-06` (replace a follower by an empty node) and `chaos-08` (wipe and
   re-join) do.
5. Start it. It deploys the schemas (about 5 s per MySQL database), joins the group (the others already list it in
   `RaftNodes`; nothing to reconfigure) and becomes a `Follower`: the leader sends a snapshot if its log is already
   compacted past the beginning (`installing snapshot on <address>` in the *leader's* log) or replays the log.
6. Verify: `raft-state` is `Follower`, `/api/tenants` equals the other nodes', `orchestrator_tenant_set_mismatch` is 0 and
   `orchestrator_raft_unknown_tenant_commands_total` stays 0; compare something per tenant that Raft replicates, for example
   the number of tags of each tenant (`GET $NODE/api/tenants/$T/...`) or the audit of recoveries on this node and on the
   leader; after a few poll intervals the node discovers the same topology.

Wiping **only** the Raft directory and keeping the backends (scenario 6B of the chaos suite) also converges the Raft-replicated
tables, but it is not enough when the backend itself missed commands; wiping both is the verified full rebuild. What is lost
in a rebuild is what is node-local: its audit trail and rows only that node held (see
[MySQL backend](multi-tenant-mysql-backend.md#what-is-not-rediscovered-the-tables-to-protect)). Never rebuild two nodes at the
same time.

## Recoveries per tenant

* **Disable and enable recoveries for ONE tenant:** `GET $NODE/api/tenants/$T/disable-global-recoveries`,
  `.../enable-global-recoveries`, `.../check-global-recoveries` (`"enabled"` or `"disabled"`). The name is historical: the
  flag is **per tenant**, replicated through Raft, and the other tenants are unaffected. It is part of the snapshot, so a
  restarted or rebuilt node keeps it. Disable before planned work on a tenant's master; failure *detection* keeps running.
* **See what is blocked:** `GET $NODE/api/tenants/$T/blocked-recoveries` (and `/cluster/<name>`), `.../audit-recovery`,
  `.../replication-analysis`. A cluster that was recovered recently is blocked for `RecoveryPeriodBlockSeconds`; the log of
  the leader says `instance ... has recently been promoted ... and is in active period. It will not be failed over` or
  `cluster ... has recently experienced a failover ... and is in active period`.
* **Acknowledge** (a comment is required, and the tenant must be in the path, because recovery IDs and cluster names are
  only unique within a tenant):
  * `.../ack-recovery/instance/<host>/<port>?comment=...`
  * `.../ack-recovery/<recoveryId>?comment=...` (or `.../ack-recovery/uid/<uid>`)
  * `.../ack-recovery/cluster/<name>?comment=...` (or `/cluster/alias/<alias>`)
  * `.../ack-all-recoveries?comment=...` (every recovery of that tenant)

  Prefer acknowledging **by instance, by ID or all**: by cluster name also matches the cluster alias that each node derives
  from its own discovery state, so nodes can disagree (seen in the mixed-version tests). With Raft the acknowledgement is
  published as a command, so every node applies it.
* Acknowledging a recovery ends its active period and **unblocks** a new recovery of that cluster. Do it only when the master
  really needs to be failed over again.

## Symptoms, causes, actions

| Symptom | Cause | Action |
|---|---|---|
| A master is dead and **no failover happens**; `blocked-recoveries` or the leader log says `... is in active period. It will not be failed over` | The cluster is inside `RecoveryPeriodBlockSeconds` of a previous recovery (anti-flapping), or an orphan recovery is still open (see the `kill -9` row). Earlier versions also left a recovery restored from a snapshot active forever; that is fixed in this tree (a failover stuck 150-240 s in the soak, unblocked by an acknowledgement). | Wait out the period, or acknowledge by instance (`ack-recovery/instance/<host>/<port>?comment=...`) if the previous recovery is known to be over. Check `RecoveryPeriodBlockSeconds` is not larger than you can tolerate. |
| A recovery repeated: several `topology_recovery` rows and hook runs for one failover | `RecoveryPeriodBlockSeconds` shorter than a failover under Raft (40-60 s). | Raise it to **120 s or more** in the tenant (default 3600). |
| **A hook ran twice after a `kill -9` of the leader** in the middle of a failover (`topology_recovery` shows the recovery repeated) | The new leader waits the block period and repeats the recovery from scratch: the pre-failover hook ran twice, the post hook once, and the topology was repaired after the block period (86 s with a 60 s block and a 20 s hook). | Expected. Make hooks idempotent; shorten the block period within the >= 120 s rule if the outage is too long. |
| **The node gives up the leadership**: log `orchestrator/raft: yielding leadership: backend of tenant <id> unhealthy: ...`; `orchestrator_raft_is_leader` flips | The backend of one tenant on that node kept failing for about 5 seconds (disk full, server down, lost grants, too many connections). By design another node takes **all** the tenants; the broken node stays alive and heals. | Read `Details.Tenants[]` of `/api/health` on that node (`Message`). Repair the backend. If commands arrived while it was broken, [rebuild the node](#rebuilding-a-node). |
| `Health test is failing for over 5 seconds. raft yielding`, then `node is unable to register health` and the process exits | The **control** backend of the node cannot register the heartbeat (30 s of real unhealthiness; a frozen process is not counted). | Repair the control backend or the clock; restart the process. |
| Log `command NOT applied on this node, tenant unknown here`; `orchestrator_raft_unknown_tenant_commands_total` grows | This node does not run a tenant the leader's commands name: its tenants file differs. | Fix the file so it matches the other nodes, restart, then [rebuild the node](#rebuilding-a-node) (the dropped commands are never retried). |
| `orchestrator_tenant_set_mismatch` = 1; `ERROR ... the tenants of this node (...) differ from the ones of the leader (...)` | The same as above, detected by the announcement (every 5 minutes, and at each election). | Same. With policy `yield` a leader that differs also hands the leadership over. |
| Log `raft snapshot restore: tenant <id> is in the snapshot but has no runtime on this node; skipping it` | The snapshot holds a tenant this node does not run (mismatch, or a tenant that was removed). | Expected after a removal; otherwise a mismatch. |
| `Unknown command op: tenant-set` (or another op) in the log of a node | A node of an **older version** received an operation it does not know. For `tenant-set` it is harmless: the node keeps working and cannot report a mismatch. For a tenant-scoped op during a mixed-version rollout it means that node does not apply what newer nodes publish. | Finish the upgrade ([upgrade](upgrade-multi-tenant.md)); do not add a second tenant while any node is old. |
| API `400` on `/api/<endpoint>` | The route has no tenant prefix and the process has several tenants and none named `default`. | Use `/api/tenants/$T/<endpoint>` (or the `X-Orchestrator-Tenant` header / `tenant` query parameter). `404` = the tenant does not exist on the node that answered. |
| The process does not start: `process: N tenants configured but Raft is disabled` | More than one tenant requires Raft. | Enable Raft ([deployment](deployment-raft-multi-tenant.md)). |
| The process does not start: `cannot initialise the tenants: tenant "<id>": ...` | The backend of that tenant is unusable, or two tenants share a backend, or the tenants file is invalid. | Read the message after the tenant name ([fail-fast table](multi-tenant-mysql-backend.md#fail-fast-what-an-unusable-backend-looks-like)). No half-started node ever joins Raft. |
| Followers answer v2 API calls with data that differs from the leader's | `/api/v2/...` is not proxied to the leader; each node answers from its own backend. | Talk to the leader (the load balancer does), or accept that each node has its own discovery state. |
| `topology_recovery` rows differ slightly between nodes (end of the active period of a row that simply expired) | `ClearActiveRecoveries` expires active periods locally, on each node, by design. | None. Everything else converges (the chaos suite compares a hash of the other columns). |
| Snapshots every 30 minutes and after recoveries; short stalls of replication while one is taken | Raft takes a snapshot reading every tenant's backend in the apply path (`SnapshotInterval` 30 min, `async-snapshot` after a recovery; neither is configurable). Measured ~0.65 s (3 tenants x 20000 instances) to ~1.7 s (x 50000) on SQLite. | Normal. If it is long on your data, reduce the instances per group or the backend latency (MySQL on the node's own host). Time it with `/api/raft-snapshot`. |
| After a long pause or freeze of the node (VM stall, `SIGSTOP`) | A paused leader wakes up as follower and runs nothing; a recovery in flight stops when it loses the term. A pause longer than 30 s no longer kills the process (the unhealthy-time count restarts at wake-up). | Check `audit-recovery` of the tenants that were mid-recovery: a recovery that was cut may need an acknowledgement. A few seconds between losing the term and noticing it are not covered. |
| A tenant's data differs between nodes after a backend incident (tags or downtimes missing on one node) | Commands the node failed to apply are never retried; snapshots built from its backend repeat the gap. | [Rebuild the node](#rebuilding-a-node). |
| Client got a timeout or "not leader" and retried; the command appears twice | No deduplication of Raft commands: a retry after an ambiguous error can apply twice. | Read the state before retrying a command that is not idempotent. |
| `Error 1062` or `Error 1213` in the log of a MySQL backend, occasionally | Duplicate on the `cluster_alias` unique key while a cluster is renamed; a deadlock. Both are upstream behaviour. | None unless they are constant. Any other `Error 1064/1054/1146/...` is a bug or version problem: report it. |

## Limits and known limitations

These restate [Multi-tenant orchestrator: known limitations](multi-tenant.md#known-limitations) and the *Remaining limitations*
of the chaos suite; if they ever disagree, `multi-tenant.md` wins.

* **SIGHUP only reloads the global configuration**; adding, removing or changing a tenant means restarting the nodes one at a
  time.
* **Raft is mandatory** with more than one tenant, and every node must run **the same tenants**; there is no upgrade path with
  a second tenant while a node is of a version without multi-tenancy (upgrade every node first).
* **A command a node could not apply is never retried** (unknown tenant, full or broken backend): rebuild the node by
  deleting its Raft data and backends and re-joining it.
* **A client retry after an ambiguous error may apply a command twice**: no deduplication.
* **The leadership check during a recovery only helps once Raft has demoted the node**: the few seconds before a leader
  notices it has lost the term are not covered.
* **`lost-in-recovery` downtime is written locally by the recovering leader**, not through Raft, and
  `topology_failure_detection` keeps the sealing/unique-index pattern that `topology_recovery` no longer has.
* **The Raft snapshot reads the backends in the apply goroutine**: while it is taken (`~0.65 s` to `~1.7 s` in the
  measurements above), `Apply` is blocked for every tenant of that node.
* **`RecoveryPeriodBlockSeconds` must be longer than a failover with Raft**: 120 s or more.
* **`${ENV}` credentials are visible to every hook**; one authentication realm for all tenants (`ReadOnly`,
  `PowerAuthUsers` are global); `disable-global-recoveries` is per tenant.
* **Agents and seeds** are outside this version (`ServeAgentsHttp` with a tenants file is refused).
* **PostgreSQL is a topology engine only**, never an orchestrator backend: tenant backends are SQLite or MySQL/MariaDB
  (MariaDB 11.4 tested; MySQL 8 and TLS to the backend are not).
* **The v2 API is not proxied to the leader** and is read-only.
* **Not tested**: more than three nodes, a leader pause of 60 s (informational in the suite), per-tenant backend users,
  mixed SQLite/MySQL backends in one group, and backends on a different host than the node.
