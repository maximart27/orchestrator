# Multi-tenant end-to-end tests

Real engines, real binary, no mocks: MariaDB 11.4 and PostgreSQL 16 topologies in Docker, and the
`orchestrator` binary of this tree running in containers. They check that **one process hosts several
tenants** (Milestone 1) and that **one Raft leader runs the leader-only work of all tenants together**
(Milestone 2). Tenant semantics: `docs/multi-tenant-config.md`.

| Script | What it does | Runnable today? |
|---|---|---|
| `validate-legacy.sh` | Single-tenant legacy mode (no tenants file): discovery on MariaDB and PostgreSQL, Raft of one node, real failover of both engines. Validates the container infrastructure, the cabling and the assertion helpers. Keep it as the regression of the legacy (`default` tenant) path. | yes |
| `run-m1.sh` | **M1**: 1 process, 3 tenants (`mariadb-a`, `mariadb-b`, `postgres-c`), 1 Raft of one node. | yes (`ORCMT_M1_EMULATE=1`: self-test of the script against 3 legacy processes, runtime-independent) |
| `run-m2.sh` | **M2**: 3 nodes `orcmt-n1..n3`, 1 Raft of three, same 3 tenants on each node. | yes (`M2_PROFILE=legacy-raft3 M2_TENANTS=mariadb-a`: self-test of the script with one legacy tenant) |
| `topology.sh up [a] [b] [c]` / `down` | Brings up the database topologies (parallel, health-checked, replication probed). | yes |
| `gen-config.sh <profile>` | Generates `orchestrator.conf.json` + `tenants.json` per node. | yes |
| `validate-config.sh <node>` | Loads the generated config with the real `config.LoadTenantConfigs`. | yes |
| `cleanup.sh` | Removes every `orcmt-*` container, volume and network. Nothing else. | yes |
| `lib.sh` | Parameters (images, memory, timeouts, credentials, IPs) and helpers. | |
| `tools/validate-tenants/` | Tiny Go program (`config.ForceRead` + `config.LoadTenantConfigs`). | |

## Requirements

* Docker (no published ports, no network pulls). Images that must already exist locally:
  `mariadb:11.4`, `postgres:16-alpine`, `golang:1.25` (builds **and** runs the binary; it has `curl`, `bash`,
  `python3` with `sqlite3`). Override with `IMG_MARIADB`, `IMG_POSTGRES`, `IMG_GO`.
* `bash`, `python3` on the host (only to parse API output).
* About 3 GB of RAM at peak (6 databases at ~85 MB, nodes capped at 512 MB each; `MEM_*` to tune).
* Subnet `172.31.77.0/24` free (`ORCMT_SUBNET`).

The binary is compiled offline in `golang:1.25` into `$ORCMT_WORK/bin` (default `/tmp/orcmt-work`) the first
time (about 1-2 minutes with a cold Go cache; the cache lives in `$ORCMT_WORK/gocache`). `ORCMT_REBUILD=1`
forces a rebuild; **rebuild after every change of the Go code**.

## Running

```sh
tests/multitenant/validate-legacy.sh              # ~2 min: mysql pg raft failover (or pick stages)
tests/multitenant/run-m1.sh                       # ~3 min
tests/multitenant/run-m2.sh                       # ~6 min
```

Every script cleans up after itself (trap on EXIT/INT/TERM) and saves the logs of all containers to
`$ORCMT_WORK/logs/<container>.log` before removing them. `ORCMT_KEEP=1` leaves the containers running so you can
inspect them (`docker exec orcmt-n1 ...`), then run `cleanup.sh`. Only resources named `orcmt-*` are ever
touched (other containers/networks of the host are left alone). The exit code is non-zero when any assertion fails.

Useful knobs (all in `lib.sh`): `T_DISCOVERY`, `T_FAILOVER`, `T_RAFT_ELECTION`, `T_SETTLE`,
`RECOVERY_BLOCK_SECONDS`, `M1_FAILOVER=0`, `CADENCE_WINDOW`, `ORCMT_WORK`, `ORCMT_PREFIX`.

## Topologies (fixed IPs on `orcmt-net`)

| Tenant | Containers | Notes |
|---|---|---|
| `mariadb-a` | `orcmt-a1` (master) -> `orcmt-a2` | server_id 1001/1002, GTID domain 11, topology user `orcmt_a`, `InstancePollSeconds` 3, filter `*`, KV prefix `mariadb-a/master`, alias `alias-a`, post-failover hook -> `/data/hook-mariadb-a.log`, password via `${ORC_A_TOPOLOGY_PASSWORD}` |
| `mariadb-b` | `orcmt-b1` -> `orcmt-b2` | server_id 2001/2002, GTID domain 22, user `orcmt_b`, poll 5, `ReasonableReplicationLagSeconds` 60, filter `.*`, `@@report_host` resolution, pre- and post-failover hooks, alias `alias-b` |
| `postgres-c` | `orcmt-c1` (primary) -> `orcmt-c2` (streaming standby) | user `orcmt_c`, poll 4, `sslmode=disable` |
| nodes | `orcmt-n1..n3` (`.101-.103`) | `golang:1.25` running the compiled binary, `/data` = named volume `orcmt-nX-data` with the control SQLite and one `<tenant>.db` per tenant |

Tenant-scoped values differ on purpose so that a cross-tenant leak shows up as a failed assertion.

## What each script asserts

**validate-legacy.sh** (19 assertions): legacy MariaDB discovery (`database_instance` has exactly master+replica,
`DetectClusterAliasQuery` applied, `/api/clusters`, `/api/cluster/<name>`), legacy PostgreSQL discovery
(`provider_type=postgresql`), `RaftEnabled` with one node becomes Leader and still discovers, and for both engines
stop-the-master -> `topology_recovery` row -> replica promoted. No panic/fatal in the log.

**run-m1.sh**
1. All three tenants discover *their own* master+replica (exact rows in each tenant's SQLite, right
   `provider_type`, one cluster name, replica pointing at the master) and do it concurrently.
2. Isolation: one backend file per tenant; no `database_instance` / `audit` / `topology_recovery` row about
   another tenant's instance, no instance of another provider; per-tenant `DetectClusterAliasQuery`; the control
   backend holds no tenant instances.
3. Per-tenant config: distinct topology credentials work (users only exist on their own topology) and
   `InstancePollSeconds` 3 vs 5 shows up as a different polling cadence.
4. API: `GET /api/tenants`, `GET /api/health` names every tenant, `/api/tenants/<id>/clusters` lists only that tenant, legacy `/api/clusters` is 400 (ambiguous).
5. Process alive, never restarted, no panic / `unknown tenant`, no authentication errors, one `tenant <id>: continuous discovery: starting` line per tenant.
6. (`M1_FAILOVER=1`) stop a1 and c1: recoveries appear only in the tenants `mariadb-a` / `postgres-c`, about
   their own master only; `mariadb-b` has none; a's hook ran (and knows its tenant through `{tenantId}` / `ORC_TENANT_ID`), b's did not; replicas promoted.

**run-m2.sh**
1. Exactly one Leader, followers agree on it; the leader discovered all tenants; `/api/tenants/<id>/clusters` works on every node (followers forward to the leader).
2. Stop the masters of all three tenants at once: the **leader** registers a recovery for each, runs a's and b's
   hooks (hook file exists **only** on the leader's `/data`), runs each recovery once; the followers have no hook
   file and never log a recovery.
3. Old masters rejoin as GTID replicas of the promoted ones; the leader sees the new topology.
4. Stop the leader's orchestrator: a *different* node becomes Leader, survivors agree.
5. Stop the new masters of a and b at once: the **new** leader recovers **both** tenants (row processed by its
   hostname, hook on its `/data`, once each); the other follower and the dead ex-leader do nothing.
6. The Raft snapshot is format v2 (`gzip(JSON{FormatVersion:2, Tenants:{...}})`) with a section per tenant; no panic anywhere.

Evidence is node-local on purpose: Raft replicates `topology_recovery` rows to the followers, and each follower
stamps the replicated row with *its own* hostname, so a row on a follower proves nothing (verified on the
legacy binary).

## Findings while building this (current binary)

1. **MariaDB >= 11 needs more than the documented grants.** With only `SUPER, PROCESS, REPLICATION SLAVE,
   REPLICATION CLIENT, RELOAD` (docs/configuration-discovery-basic.md) `SHOW SLAVE STATUS` fails with
   `Error 1227 ... SLAVE MONITOR` and nothing is discovered; `SHOW SLAVE HOSTS` needs `REPLICATION MASTER ADMIN`.
   `lib.sh` also grants `SLAVE MONITOR, REPLICATION SLAVE ADMIN, REPLICATION MASTER ADMIN, READ_ONLY ADMIN,
   BINLOG ADMIN, BINLOG MONITOR`.
2. **Legacy `DiscoverySeeds` + PostgreSQL**: `InjectSeed` stores the stub with `provider_type='mysql'`, the instance
   is then polled as MySQL and never reaches the PG provider (legacy-only; `detectProviderType` trusts the stored
   value). Tenants have an explicit provider, `run-m1` verifies it for `postgres-c` with real `DiscoverySeeds`.
3. With the default `MySQLHostnameResolveMethod=@@hostname`, instances are stored under the container hostname, not
   the IP. The tenant configs set `HostnameResolveMethod=none` and `MySQLHostnameResolveMethod=none|@@report_host`.
4. `RecoveryPeriodBlockSeconds` shorter than a whole failover (40-60 s with raft) re-attempts the same recovery (4 recovery
   rows and 4 hook runs for one failover with a 5 s block). Tests use 120 s.
5. PostgreSQL provider details: connects to database `postgres` as the topology user, which is also used as the
   replication user in `primary_conninfo`; needs `pg_monitor` for discovery, `REPLICATION` + a `replication`
   `pg_hba` line and superuser-like rights for promotion (`pg_promote`, `ALTER SYSTEM`).
