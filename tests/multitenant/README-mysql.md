# Multi-tenant end-to-end tests with a MySQL/MariaDB backend

`run-m1.sh` and `run-m2.sh` run the orchestrator with SQLite backends (one file per tenant). These variants run
the same scenarios with the **orchestrator backend on a MariaDB server**:

| Script | What it does |
|---|---|
| `run-m1-mysql.sh` | M1 (1 process, 3 tenants, Raft of one) with ONE backend server and one database per tenant (`orc_mariadb_a`, `orc_mariadb_b`, `orc_postgres_c`) plus the control database of the node (`orc_control`). Same assertions as `run-m1.sh` plus: database purity, schema completeness, `node_health` mirrors, recovery data confined to the failed tenant's database, fail-fast of unusable backend settings. |
| `run-m2-mysql.sh` | M2 (3 nodes, Raft of three, same tenants) with ONE backend server and one database **per node and tenant** (`orc_n1_mariadb_a` ... `orc_n3_postgres_c`, `orc_n1_control` ...). Same flow as `run-m2.sh` plus the MySQL checks and the snapshot v2 restore of a restarted node and of a brand new node, with per-tenant table hashes compared across the nodes' databases. |
| `lib-mysql.sh` | `source`s `lib.sh` and overrides `sql` / `tenant_db` / `reintegrated` so the helpers read MySQL databases; backend server helpers (`bk_up`, `bk_root`). |
| `gen-config-mysql.sh m1\|m2` | Runs `gen-config.sh` and rewrites the backend sections (single source of truth for the tenant settings). |

Nothing of `lib.sh`, `gen-config.sh`, `run-m1.sh`, `run-m2.sh` is modified.

## Layout

```
orcmtm-bk (MariaDB 11.4, 256 MB, max_connections=400)   172.31.90.200
   user orcbk : GRANT ALL ON `orc\_%`.*     (it creates the databases itself: "create database if not exists")
   M1:  orc_control  orc_mariadb_a  orc_mariadb_b  orc_postgres_c
   M2:  orc_<n1|n2|n3>_control  orc_<node>_<tenant>
```

* The **global** configuration of every node carries `BackendDB=mysql` and `MySQLOrchestrator{Host,Port,User,Password}`;
  its `MySQLOrchestratorDatabase` is the *control* database of the node (`node_health`, election, access tokens).
* **Tenants** inherit host/port/user/password and set their own `Backend.MySQLOrchestratorDatabase`. `mariadb-a` and
  `mariadb-b` spell out the whole `Backend` section, `postgres-c` gives only its database (exercises the inheritance).
* **Every Raft node needs its own backend** (Raft replicates commands and each node applies them to *its* database:
  sharing one database between nodes would apply every command three times to the same rows), hence per-node
  databases in M2.
* The backend server is not one of the monitored topologies (`a1/a2/b1/b2/c1/c2`).

## Running

```sh
flock /tmp/orcmt-docker.lock tests/multitenant/run-m1-mysql.sh      # ~7 min
flock /tmp/orcmt-docker.lock tests/multitenant/run-m2-mysql.sh      # ~20 min (M2_NEWNODE_OPS=0 skips the compacted-log part)
ORCMT_PREFIX=orcmtm tests/multitenant/cleanup.sh                     # only removes orcmtm-* resources
```

Defaults: prefix `orcmtm`, subnet `172.31.90.0/24`, `ORCMT_WORK=/tmp/orcmtm-work`. Knobs: `M1_FAILOVER=0`,
`M2_TENANTS="mariadb-a postgres-c"` (less memory), `M2_NEWNODE_OPS=N` (tag commands published to compact the Raft
log before the new-node test; the log is truncated beyond `TrailingLogs=10240`), `BK_MAX_POOL=N` (sets
`MySQLOrchestratorMaxPoolConnections` of every backend), `ORCMT_KEEP=1`.

## Go tests against MySQL

The tenant helpers of `go/logic` tests (`testBase`, `testTenantConfigs`, `newTestManager`) switch to MySQL when
`ORCH_TEST_MYSQL_DSN` is set; every test that builds a manager then runs on a real MariaDB (one database per tenant,
dropped at cleanup):

```sh
flock /tmp/orcmt-docker.lock bash -c 'source tests/multitenant/lib-mysql.sh; bk_up'   # backend server at 172.31.90.200
docker run --rm --network orcmtm-net -e ORCH_TEST_MYSQL_DSN='orcbk:orcbk-secret@tcp(172.31.90.200:3306)/' \
  -e GOFLAGS=-mod=vendor -v "$PWD":/src -w /src golang:1.25 go test -timeout 60m ./go/logic/
```

Without the variable nothing changes (SQLite, no server). Deploying the schema takes ~5 s per database on MariaDB
(about 300 DDL statements), so a manager with N tenants needs ~5 s x (N+1) at test time; the suite is slow on MySQL.
