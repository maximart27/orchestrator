# Multi-tenant orchestrator: MySQL/MariaDB as the backend

A [multi-tenant](multi-tenant.md) orchestrator keeps the state of every tenant in its own backend. The backend of a
tenant is a SQLite file or **a database on a MySQL/MariaDB server**; this page is about the second option: how the
databases are laid out, how to configure and grant them, how many connections and how much memory to plan for, what the
process does when the backend is unusable, and what to do about backups and about leaving SQLite. The format of the tenants
file is in [Multi-tenant configuration](multi-tenant-config.md); the Raft side is in
[Deployment: raft, multi-tenant](deployment-raft-multi-tenant.md); day-two procedures are in the
[Operations runbook](multi-tenant-operations.md). The single-tenant MySQL backend is described in
[configuration: backend](configuration-backend.md); the schema is the same.

PostgreSQL is a *topology* engine of a tenant, never an orchestrator backend.

## Design

```
 MySQL/MariaDB server of node n1
   orc_n1_control      control backend of the node (node_health, access tokens)
   orc_n1_mariadb_a    backend of tenant mariadb-a
   orc_n1_mariadb_b    backend of tenant mariadb-b
   orc_n1_postgres_c   backend of tenant postgres-c
```

* **One database per tenant** on a server, with the tenant's whole state in it (instances, audit, recoveries, tags,
  downtimes, KV store...). A tenant never reads or writes a table of another tenant's database; the end-to-end suite checks
  that no row about another tenant's instances lands in a tenant database and that every database holds the same set of tables.
* **One more database for the control of the node**: the plain `BackendDB`/`MySQLOrchestrator*` settings of the node
  configuration describe the *control* backend (`node_health`, the election without Raft, access tokens). No tenant may use it
  (see "Fail-fast"). If one of the tenants is called `default`, the control of the node lives in **that tenant's** backend and no
  separate control database is needed; otherwise the global backend settings must describe a usable backend.
* **Every Raft node needs its own backend: its own databases.** Raft replicates commands and each node applies them to
  *its* databases. If two nodes shared a database, every command would be applied once per node to the same rows. This is
  true whether the nodes have a server each or share a server.
* The schema is deployed by the orchestrator itself the first time it finds a database empty (it runs
  `create database if not exists` first), and upgraded when the binary is upgraded.

### One server per node, or one shared server

* **One MySQL/MariaDB server per orchestrator node** (the layout of [deployment: raft](deployment-raft.md): a 1:1
  `orchestrator:MySQL`, possibly on the same box) is the recommended production layout: the loss of a backend server takes
  down one node, not the group. The databases are then `orc_control` and `orc_<tenant>` on each server.
* **One shared server with one set of databases per node** (`orc_<node>_control`, `orc_<node>_<tenant>`) is what the MySQL
  variant of the end-to-end suite uses (`tests/multitenant/README-mysql.md`). It is simple, but the server is a single
  point of failure for **all** the Raft nodes, which gives up part of what Raft is for; use it for small or non-critical
  groups, or tests.

The rest of this page works for both; where the numbers differ, both are given.

Database names: MySQL identifiers are at most 64 characters, tenant IDs may be up to 63; with a prefix such as
`orc_<node>_` keep tenant IDs short. The orchestrator quotes the name, so `-` is accepted, but the suite and the examples use `_`.

## Configuration

The **global** (node) configuration says where the control backend is; each tenant inherits host, port, user and password
from it and overrides only what differs, usually just the database:

```json
{
  "ListenAddress": ":3000",
  "HTTPAdvertise": "http://10.0.0.1:3000",
  "RaftEnabled": true,
  "RaftDataDir": "/var/lib/orchestrator/raft",
  "RaftBind": "10.0.0.1",
  "RaftAdvertise": "10.0.0.1",
  "DefaultRaftPort": 10008,
  "RaftNodes": ["10.0.0.1", "10.0.0.2", "10.0.0.3"],
  "TenantsConfigFile": "/etc/orchestrator/tenants.json",
  "BackendDB": "mysql",
  "MySQLOrchestratorHost": "127.0.0.1",
  "MySQLOrchestratorPort": 3306,
  "MySQLOrchestratorDatabase": "orc_n1_control",
  "MySQLOrchestratorUser": "orcbk",
  "MySQLOrchestratorPassword": "${ORC_BACKEND_PASSWORD}",
  "MySQLOrchestratorMaxPoolConnections": 32
}
```

and the tenants file (the `Backend` of each tenant is the only part that may differ between nodes):

```json
{
  "Tenants": [
    {
      "ID": "mariadb-a",
      "Provider": "mysql",
      "Backend": {
        "BackendDB": "mysql",
        "MySQLOrchestratorHost": "127.0.0.1",
        "MySQLOrchestratorPort": 3306,
        "MySQLOrchestratorDatabase": "orc_n1_mariadb_a",
        "MySQLOrchestratorUser": "orcbk",
        "MySQLOrchestratorPassword": "${ORC_BACKEND_PASSWORD}",
        "MySQLOrchestratorMaxPoolConnections": 64
      },
      "Config": {
        "MySQLTopologyUser": "orchestrator-a",
        "MySQLTopologyPassword": "${ORC_A_TOPOLOGY_PASSWORD}",
        "InstancePollSeconds": 5,
        "RecoverMasterClusterFilters": ["*"],
        "RecoveryPeriodBlockSeconds": 300,
        "KVClusterMasterPrefix": "mariadb-a/master"
      }
    },
    {
      "ID": "mariadb-b",
      "Provider": "mariadb",
      "Backend": {
        "MySQLOrchestratorDatabase": "orc_n1_mariadb_b",
        "MySQLOrchestratorMaxPoolConnections": 16
      },
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
      "Backend": { "MySQLOrchestratorDatabase": "orc_n1_postgres_c" },
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

Here `mariadb-a` spells its whole `Backend` section out, `mariadb-b` overrides the database and the pool, and `postgres-c`
gives only its database and inherits everything else (the three forms are exercised by the suite). The `Backend` section
accepts exactly the backend settings (`BackendDB`, `SQLite3DataFile`, `MySQLOrchestrator*`); any other setting in it, or a
backend setting in `Config`, is rejected naming the tenant and the field.

Credentials can also come from a `[client]` option file, per tenant or globally (`MySQLOrchestratorCredentialsConfigFile`,
as in [configuration: backend](configuration-backend.md#mysql-backend)); the tenant reads its own file only when it names
one. Keep the file at mode `0600`. Do not rely on `${ENV}` for tenants that do not trust each other (hooks inherit the
environment, see [multi-tenant](multi-tenant.md#security-between-tenants)):

```json
{
  "ID": "mariadb-b",
  "Provider": "mysql",
  "Backend": {
    "MySQLOrchestratorHost": "db-b.example.com",
    "MySQLOrchestratorDatabase": "orc_n1_mariadb_b",
    "MySQLOrchestratorCredentialsConfigFile": "/etc/orchestrator/backend-b.cnf"
  },
  "Config": {
    "MySQLTopologyUser": "orchestrator-b",
    "MySQLTopologyPassword": "${ORC_B_TOPOLOGY_PASSWORD}"
  }
}
```

Every tenant must resolve to its **own** `host:port/database`; see "Fail-fast".

## The backend user and its grants

The orchestrator creates its databases itself, so its backend user needs the privileges to create and own them. The suite
uses **one user with a pattern grant** on a naming convention (every database called `orc_...`):

```sql
CREATE USER IF NOT EXISTS 'orcbk'@'10.0.0.%' IDENTIFIED BY '<backend password>';
GRANT ALL PRIVILEGES ON `orc\_%`.* TO 'orcbk'@'10.0.0.%';
```

(`\_` is a literal underscore in the grant pattern.) This is simple and is what was tested, but it is isolation **by
name**: the user can reach every `orc_...` database, so a tenant whose `Backend` points at another tenant's database is
stopped only by the orchestrator's own check (it refuses to start, see below), not by the server.

A **stricter** layout is one user per tenant (and one for the control backend), each with `ALL` on its own database only:

```sql
CREATE DATABASE IF NOT EXISTS orc_n1_mariadb_a;
CREATE USER IF NOT EXISTS 'orc_n1_a'@'10.0.0.%' IDENTIFIED BY '<password of tenant a>';
GRANT ALL PRIVILEGES ON `orc_n1_mariadb_a`.* TO 'orc_n1_a'@'10.0.0.%';
```

Then the isolation is by **privileges**, enforced by the server: a misconfigured tenant cannot open another's database,
and a compromised credential exposes one tenant. Each tenant's `Backend` must then spell out its own user and password (or
credentials file) instead of inheriting them. The suite does not run this variant, but the code paths it needs (per-tenant
backend credentials and the "refused database" fail-fast) are exercised. Whichever you choose, the backend user needs no
privilege outside the orchestrator databases; do not reuse the topology user, and never `GRANT` on `*.*`.

The **topology** grants (the user the tenant uses on the monitored servers) are unrelated; see
[configuration: discovery basic](configuration-discovery-basic.md) (MariaDB 11 needs the extra privileges listed there).

## Versions tested

* **MariaDB 11.4** as the backend server, with the server's default `sql_mode` (the suite sets none), in the `M1` (one
  node) and `M2` (three nodes, Raft) scenarios of `tests/multitenant/run-m1-mysql.sh` / `run-m2-mysql.sh`, and in the `go/logic`
  tests (`ORCH_TEST_MYSQL_DSN`). The M2 run asserts that no MySQL dialect/schema error (1064, 1054, 1146, 1136, 1048, 1364,
  1366, 1292, 1265, 1305) appears in any node log.
* **Not tested:** MySQL 8.x as a backend, an explicitly strict `sql_mode` set by the operator, TLS to the backend (the
  settings exist: `MySQLOrchestratorUseMutualTLS`, `MySQLOrchestratorSSLCAFile`, `MySQLOrchestratorSSLCertFile`,
  `MySQLOrchestratorSSLPrivateKeyFile`, `MySQLOrchestratorSSLSkipVerify`; the TLS configuration of the driver is registered
  with a name unique to each backend, so tenants do not overwrite each other's), a backend server on another host than
  the node, and Galera/replicated backends. Test them in a staging group before relying on them.

## Sizing

### Connections

Every backend (the control one and one per tenant) has its **own connection pool**, of up to
`MySQLOrchestratorMaxPoolConnections` connections (default **128**), and keeps up to `max(10, 25% of the pool)` idle
connections open. With N tenants a node can therefore hold up to `(N + 1) x pool` connections to its server, and the
server needs at least

```
max_connections  >=  nodes_on_that_server x (tenants + 1) x MySQLOrchestratorMaxPoolConnections   (+ headroom for you)
```

* One server per node: `nodes_on_that_server` is 1: three tenants with the default pool is `4 x 128 = 512`.
* One shared server for three nodes: `3 x 4 x 128 = 1536` with the defaults.

That is the worst case (every pool saturated at once), but a default left alone makes the worst case a large number. A tenant
**never uses more than its pool**: beyond it, requests wait in the driver (discovery gets slower, nothing fails). So size
the pools, not only the server:

* Lower `MySQLOrchestratorMaxPoolConnections` in the **global** configuration (it is inherited by every tenant and by the
  control backend) and raise it only in the `Backend` of the tenants that need it (it is a backend setting, allowed per
  tenant, as in the examples above); the control backend needs very little.
* The suite runs its three tenants with the default pool against a server with `max_connections=400` (`BK_MAX_CONN`) and
  asserts that no `Too many connections` error appears in the node log; `BK_MAX_POOL` sets the pool of every backend to test
  smaller values. Watch `Threads_connected` and `Max_used_connections` on your server and `Too many connections` in the
  orchestrator log.

### Memory (measured with the suite)

The memory of the backend server grows with the number of databases and with the Raft commands applied to them. In the
three-node suite (one MariaDB 11.4 server, `innodb_buffer_pool_size=64M`, `performance_schema=OFF`, 3 nodes x 4 databases)
a container limit of **256 MB was killed by the OOM killer** while 10600 commands were applied (the commands published to
compact the Raft log before the new-node test), and **384 MB held**. The defaults of `tests/multitenant/lib-mysql.sh`
(`MEM_BK=256m`) predate this finding; use `MEM_BK=384m` or more when you run that scenario. Those numbers are a floor for a
test-size buffer pool, not a recommendation: size `innodb_buffer_pool_size` for the data of the tenants (instances x polls
retained in the history tables, audit retention `AuditPurgeDays`) and leave memory for the connections above.

### Startup time

The first start of an empty database deploys the whole schema, about **5 seconds per database** on MariaDB (about 300 DDL
statements), cold, and the tenants are built one after the other. A node that starts with N new tenants therefore needs
about `5 x (N + 1)` seconds before it serves, plus the restore of its snapshot. Later starts find the schema in place and
only apply pending upgrades. Plan health-check grace periods and rolling restarts for the cold case.

## Fail-fast: what an unusable backend looks like

The process builds every backend **before** it starts anything, and does not start if one is unusable. The error names the
tenant; the backend password never appears in the log (checked by the suite). The two duplicate-backend messages were
produced by the real validator (`tests/multitenant/tools/validate-tenants`) on the configuration above; the others are
read from the code:

| Cause | Message (shape) |
|---|---|
| The user has no privilege on the database (outside the grant pattern, or no `CREATE`) | `cannot initialise the tenants: tenant "<id>": deploying the backend schema: Error 1044 (42000): Access denied for user '<user>'@'<host>' to database '<db>'` |
| Wrong password or user unknown from that host | `cannot initialise the tenants: tenant "<id>": deploying the backend schema: Error 1045 (28000): Access denied for user '<user>'@'<host>' (using password: YES)` |
| Two tenants resolve to the same `host:port/database` (also when both inherit the node's) | `cannot initialise the tenants: tenants file <path>: tenants "<a>" and "<b>" share the same MySQL backend <host>:<port>/<db>` |
| A tenant uses the **control** database of the node | `cannot initialise the tenants: tenants file <path>: tenant "<id>" uses the MySQL backend <host>:<port>/<db> of the node's own (control) backend: give the tenant its own Backend.MySQLOrchestratorDatabase` |
| No database name | `cannot initialise the tenants: tenant "<id>": deploying the backend schema: MySQLOrchestratorDatabase is not set` |
| The control backend (no tenant named `default`) is unusable | `control backend: no tenant "default" to host it and the global backend settings are unusable: ...` or `control backend: deploying the schema: ...` |

(The server side of the first two is the server's own text; the suite checks that the tenant is named and that the process
exits, not the exact MySQL wording, which differs between servers and versions.) A server that is down at start gives the
driver's connection error in the same place. If the backend of a *running* tenant fails, the node [yields the Raft
leadership](deployment-raft-multi-tenant.md#behaviour-under-failures) instead of exiting.

### Transient errors that are not failures

On a MySQL backend the log may show, occasionally, `Error 1062` (duplicate key on the `cluster_alias` unique key while a
cluster is renamed) and `Error 1213` (deadlock between concurrent backend writers). They are upstream behaviour of the
orchestrator's MySQL backend, not specific to multi-tenancy; the suite reports them and does not fail on them. Any
*other* class of MySQL error in the log, above all syntax or unknown-column/table errors, is a bug or a version
incompatibility: collect the log line and the server version.

## Backups and restoring the database of one tenant

Most of a backend is a **cache of what the orchestrator discovers live** (instances, replication, analysis, histories): it is
rebuilt by discovery within a few poll intervals. What must be preserved is the small set of tables that hold operator
decisions and recovery state, listed in the next section. Because every Raft node holds them all, **the group itself is
the backup**: the safest restore of a damaged node is to [rebuild the node](multi-tenant-operations.md#rebuilding-a-node)
from the leader, not to load a dump.

If you still take logical backups (for forensic history, or the audit trail, which is local to each node and not replicated):

```sh
mysqldump --single-transaction orc_n1_mariadb_a > mariadb-a-n1.sql     # one tenant, one node
```

To restore one tenant on one node: stop the orchestrator on that node, load the dump into the (dropped and recreated)
database, **delete that node's `RaftDataDir`** and start it. With an empty Raft directory the node asks the leader for a
snapshot and re-applies the Raft-only tables to the database (the chaos suite, scenario 6B, wipes only the Raft directory of a node and it converges to data identical to the leader's,
with SQLite backends; the MySQL suite restarts a node that restores a snapshot into populated databases and starts a brand
new node with empty databases, and both converge). The Raft-only tables of an older dump are thereby replaced by the
leader's, atomically per tenant. Do not copy a database from one node to another and keep the old Raft directory: the node
would resume from a log position that does not match its data.

## Moving a tenant from SQLite to MySQL

**There is no migration tool, and none is needed in a Raft group.** SQLite and MySQL backends can be mixed across the nodes
of a group (the `Backend` section is the part of the tenants file that may differ per node; a mixed group is not exercised
by the suite, so test it first), so move **one node at a time**:

1. Create the databases and the user on the MySQL server (grants above).
2. Stop the orchestrator on the node; keep its old SQLite files until the end.
3. Change the `Backend` of the tenants (and the control backend settings) to MySQL, keeping the tenants file otherwise
   identical to the other nodes.
4. Delete the node's `RaftDataDir`, so that it starts as a new node, and start it: it deploys the schema (about 5 s per
   database), joins, and the leader installs a snapshot that fills the Raft-only tables. Discovery refills the rest.
5. Wait until it is healthy and its tenants have converged ([runbook](multi-tenant-operations.md#rebuilding-a-node)),
   then do the next node, **the leader last**.

Reconstructing a backend is safe because it is a cache of what is discovered, *except* for the tables below.
**What is lost** by moving a node this way: the node-local audit trail (`audit`), maintenance windows and histories of
that node (not carried by snapshots), and `host_attributes` and `access_token` rows that exist only on that node. A SQLite
file cannot be loaded into MySQL with `.dump` (different dialect); if you need those node-local rows, export them
beforehand from the old file with `sqlite3` and re-create them through the API.

## What is not rediscovered: the tables to protect

These are the tables of a tenant backend that a Raft snapshot carries (`TenantSnapshot.tables()`,
`go/logic/snapshot_data.go`). The first group is written **only by Raft commands**, so the snapshot is its whole content
(a restore replaces the table, atomically per tenant): it holds operator decisions and recovery history that discovery
cannot recreate.

| Table | Written by | What it holds | Rediscovered? |
|---|---|---|---|
| `database_instance_tags` | `put-instance-tag`, `delete-instance-tag` | instance tags | **no** |
| `database_instance_downtime` | `begin-downtime`, `end-downtime` (+ expiry) | downtimes | **no** (they expire by time) |
| `candidate_database_instance` | `register-candidate` (+ expiry) | promotion rules / candidates | **no** (expire by time) |
| `database_instance_pool` | `submit-pool-instances` (+ expiry) | instance pools | **no** |
| `cluster_alias_override` | `set-cluster-alias-manual-override` | manual cluster-alias overrides | **no** |
| `hostname_unresolve` | `register-hostname-unresolve` (+ expiry) | unresolve overrides | **no** |
| `topology_recovery`, `topology_recovery_steps` | `write-recovery`, `resolve-recovery`, `write-recovery-step`, `ack-recovery` | recovery history, acknowledgements, and the *active period* that blocks a repeated recovery | **no** |
| `topology_failure_detection` | `register-failure-detection`, `ack-recovery` | failure detection records | re-detected, history is not |
| `cluster_injected_pseudo_gtid` | `injected-pseudo-gtid` (+ expiry) | clusters where Pseudo-GTID was injected | by `AutoPseudoGTID`; manual injections are not |
| `kv_store` | `put-key-value` | contents of the internal key-value store | republished on the next recovery / `submit-masters-to-kv-stores` |
| (flag) `global_recovery_disable` | `disable-global-recoveries`, `enable-global-recoveries` | whether recoveries are disabled for the tenant | **no** |

The second group is also written by the node itself and is only **added to** by a restore, never emptied:
`cluster_alias`, `cluster_domain_name` and `hostname_resolve` (derived from discovery, rebuilt), and `host_attributes` and
`access_token` (set through the API of the node that receives the call, **with no Raft command of their own**, and not
rediscoverable: a node that is rebuilt keeps only what the leader's snapshot adds, and loses the rows only it held). `database_instance` itself (the instances) is written by discovery and
reconciled separately. Everything else in the schema (the audit tables, the history of coordinates and analysis,
maintenance) is local to the node.
