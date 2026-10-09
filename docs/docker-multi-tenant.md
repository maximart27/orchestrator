# Multi-tenant orchestrator with Docker and Docker Compose

This guide runs one orchestrator process that manages several tenants (several unrelated MySQL/MariaDB and
PostgreSQL topologies) in a container, from a single node up to a three-node Raft group, with SQLite or with a MariaDB
backend. The concepts (tenants, Raft, tenants file) are in [Multi-tenant orchestrator](multi-tenant.md) and
[Multi-tenant configuration](multi-tenant-config.md); this page is only about packaging and running them in containers.
The single-tenant images of [Docker](docker.md) (`docker/Dockerfile`, `script/dock`) are unrelated and untouched.

Everything described here is in [`docker/multitenant/`](https://github.com/proxysql/orchestrator/tree/master/docker/multitenant):

| File | What it is |
|---|---|
| `Dockerfile` | Multi-stage image: vendored Go build, Alpine runtime, non-root user, data volume, `HEALTHCHECK`. |
| `compose.single.yml` | One node (Raft of one member), three example tenants, SQLite backends. |
| `compose.raft3.yml` | Three nodes, one Raft group, SQLite backends, one volume per node. |
| `compose.mysql-backend.yml` | Three nodes plus one MariaDB server holding every backend (control + one database per tenant, per node). |
| `config/` | Example `orchestrator.conf.json` (one per node) and `tenants.json` files. All valid with the real loader. |
| `.env.example`, `secrets.example/` | Templates for the variables and credential files. The real ones (`.env`, `secrets/`) are git-ignored. |

The examples were run against real MariaDB 11.4 and PostgreSQL 16 topologies (see [Verification](#verification)).

## Contents

* [Image](#image)
* [Configuration and credentials](#configuration-and-credentials)
* [Running with `docker run`](#running-with-docker-run)
* [Compose: one node](#compose-one-node)
* [Compose: three nodes (Raft)](#compose-three-nodes-raft)
* [Compose: MariaDB backend](#compose-mariadb-backend)
* [Volumes and persistence](#volumes-and-persistence)
* [Network and ports](#network-and-ports)
* [Health checks](#health-checks)
* [Orderly shutdown](#orderly-shutdown)
* [Logs](#logs)
* [Memory limits](#memory-limits)
* [Updating the image (rolling)](#updating-the-image-rolling)
* [Restarting or rebuilding a node](#restarting-or-rebuilding-a-node)
* [Troubleshooting](#troubleshooting)
* [Verification](#verification)

## Image

Build from the **root of the repository** (the context needs `go.mod`, `vendor/` and `resources/`):

```shell
docker build -f docker/multitenant/Dockerfile -t orchestrator-mt:local .
# or, tagged with the release and the commit
docker build -f docker/multitenant/Dockerfile \
  --build-arg VERSION="$(cat RELEASE_VERSION)" --build-arg GIT_COMMIT="$(git rev-parse HEAD)" \
  -t orchestrator-mt:"$(cat RELEASE_VERSION)" .
```

Without `--build-arg VERSION` the version is read from `RELEASE_VERSION`. `orchestrator-mt:local` is what the compose
files use unless `ORCHESTRATOR_IMAGE` says otherwise; pin a version tag (`orchestrator-mt:4.30.0`) in production and
push it to your registry like any other image.

* **Offline-friendly.** Go modules come from `vendor/` (`-mod=vendor`): the build needs no module proxy. The only
  network access is `apk add` of the C toolchain (the SQLite driver needs cgo) and of `bash curl jq` in the runtime
  image. Pass `--build-arg GO_IMAGE=...` / `--build-arg RUNTIME_IMAGE=...` to use other (or mirrored) bases; they must
  both be Alpine of the same release, because the binary links against musl. Defaults: `golang:1.25-alpine3.23` and
  `alpine:3.23` (`go.mod` requires Go 1.25.7; the stock `docker/Dockerfile` pins `golang:1.25.7-alpine3.22`).
* **Size**: about 80 MB.

Contract of the image:

| Item | Value |
|---|---|
| User | `orchestrator`, uid/gid **10001**, no login shell |
| Binary / working directory | `/usr/local/orchestrator/orchestrator`, which is also the working dir: the web UI is read from `resources/` relative to it |
| Entrypoint / default command | `orchestrator` / `-config /etc/orchestrator/orchestrator.conf.json http` |
| Configuration (mount read-only) | `/etc/orchestrator/orchestrator.conf.json` and `/etc/orchestrator/tenants.json` |
| Data (**a volume**) | `/var/lib/orchestrator`: Raft directory and SQLite backends |
| Ports | `3000/tcp` HTTP, UI and API; `10008/tcp` Raft (internal only) |
| Signals | `STOPSIGNAL SIGTERM`; run with `--init` / `init: true` so hook child processes are reaped |
| Health | `HEALTHCHECK` every 30 s: `curl -fsS $ORC_HEALTH_URL`, default `http://127.0.0.1:3000/health/live` |
| Extras | `orchestrator-client` in `/usr/local/bin`, with `bash`, `curl` and `jq` |

Unlike `docker/Dockerfile` there is **no entrypoint script that writes a configuration from `ORC_*` variables**:
multi-tenant needs a tenants file and a Raft address, so the configuration is always mounted.

## Configuration and credentials

The `config/` examples have the shape described in [Multi-tenant configuration](multi-tenant-config.md). The points
that are specific to containers:

**Raft address = container IP.** Raft identifies a member by `IP:port` (hostnames in `RaftNodes` are resolved to IPs
when the process starts, and the node id is that address), so the compose networks have a fixed subnet and every node a
fixed address, repeated in its configuration file:

```json
{
  "ListenAddress": ":3000",
  "HTTPAdvertise": "http://172.28.61.12:3000",
  "BackendDB": "sqlite",
  "SQLite3DataFile": "/var/lib/orchestrator/control.db",
  "RaftEnabled": true,
  "RaftDataDir": "/var/lib/orchestrator/raft",
  "RaftBind": "172.28.61.12",
  "RaftAdvertise": "172.28.61.12",
  "DefaultRaftPort": 10008,
  "RaftNodes": ["172.28.61.11", "172.28.61.12", "172.28.61.13"],
  "TenantsConfigFile": "/etc/orchestrator/tenants.json"
}
```

(`config/raft3/orchestrator-2.conf.json`.) One file per node because `RaftBind`/`RaftAdvertise`/`HTTPAdvertise` differ;
a single-node setup uses `"RaftNodes": []`. **Raft is mandatory with more than one tenant**, even with one node.
`HTTPAdvertise` is how followers forward API calls to the leader. The tenants file is identical on all nodes (a local
copy per replica); the `mysql-backend` variant has one per node only because the backend database names differ.

**Credentials.** The three examples show the three ways to give a tenant its topology credentials; none puts a secret in
a compose file:

| Tenant | Method | Where the secret lives | Notes |
|---|---|---|---|
| `mariadb-a` | `MySQLTopologyCredentialsConfigFile` | `secrets/mariadb_a_topology.cnf`, mounted at `/run/secrets/mariadb_a_topology` | my.cnf subset: `[client]`, `user`, `password`. Overrides `MySQLTopologyUser/Password`. Does not reach the process environment. **Recommended.** |
| `mariadb-b` | `"MySQLTopologyPassword": "${ORC_B_TOPOLOGY_PASSWORD}"` | `.env` (git-ignored), passed as an environment variable | Visible to recovery hooks of **every** tenant (they inherit the environment) and to `docker inspect`. |
| `postgres-c` | literal `PostgreSQLTopologyPassword` | in `tenants.json` | This version has **no** `${ENV}` expansion and no credentials file for PostgreSQL: the password is in the tenants file, so keep it `0600` and out of version control. |

The same pair of mechanisms exists for the backend: `MySQLOrchestratorCredentialsConfigFile` (used by the MariaDB
variant) or `"MySQLOrchestratorPassword": "${ENV}"`.

Credential files are bind-mounted with their **host** permissions, so the container user (uid 10001) must be able to
read them:

```shell
cp .env.example .env && chmod 0600 .env
cp -r secrets.example secrets && rm -f secrets/README
chmod 0700 secrets && chmod 0400 secrets/*
sudo chown -R 10001:10001 secrets        # otherwise: "Failed to parse gcfg data ... permission denied"
```

(For the MariaDB variant `secrets/mariadb_root_password` is read by the MariaDB container as root; root bypasses the
file mode, so the `chown` is harmless for it.)

**Instance names.** The examples set `HostnameResolveMethod` and `MySQLHostnameResolveMethod` to `none`: instances are
keyed by exactly what you list in `DiscoverySeeds` and what the servers report (`report_host`). Seeds must therefore use
the same name the topology reports for its replicas (hostnames everywhere or IPs everywhere), or one server appears
twice. The seeds in the examples (`mariadb-a-1.example.com`...) are placeholders: replace them.

**Privileges on the topologies.** On MariaDB 11 the grants of [Discovery](configuration-discovery-basic.md) are not
enough: add `SLAVE MONITOR, REPLICATION SLAVE ADMIN, REPLICATION MASTER ADMIN, READ_ONLY ADMIN, BINLOG ADMIN, BINLOG
MONITOR`. For PostgreSQL see "PostgreSQL tenants: required role and `pg_hba`" in
[Multi-tenant configuration](multi-tenant-config.md). The examples enable no automatic recovery
(`RecoverMasterClusterFilters` is unset); add it per tenant when you are ready, with `RecoveryPeriodBlockSeconds` of at
least 120.

## Running with `docker run`

One node, the same thing `compose.single.yml` does (from `docker/multitenant/`, after the setup of the previous
section):

```shell
docker network create --subnet 172.28.60.0/24 orchestrator-net
docker volume create orchestrator-data
export ORC_B_TOPOLOGY_PASSWORD='...'            # only because tenant mariadb-b uses ${ORC_B_TOPOLOGY_PASSWORD}

docker run -d --name orchestrator --hostname orchestrator --init \
  --network orchestrator-net --ip 172.28.60.11 \
  --restart unless-stopped --stop-timeout 40 --memory 384m \
  -p 127.0.0.1:3000:3000 \
  -v orchestrator-data:/var/lib/orchestrator \
  -v "$PWD/config/single/orchestrator.conf.json":/etc/orchestrator/orchestrator.conf.json:ro \
  -v "$PWD/config/tenants.json":/etc/orchestrator/tenants.json:ro \
  -v "$PWD/secrets/mariadb_a_topology.cnf":/run/secrets/mariadb_a_topology:ro \
  -e ORC_B_TOPOLOGY_PASSWORD \
  orchestrator-mt:local

curl -s http://127.0.0.1:3000/api/tenants
```

`-e ORC_B_TOPOLOGY_PASSWORD` (no value) passes the variable from your shell without writing it in the command line.
Use `docker exec orchestrator orchestrator-client -c tenants` (list) and `-T <tenant> -c clusters` (act on one tenant)
to drive it; with Raft enabled the `orchestrator` CLI itself refuses to run inside the container, as documented in
[Multi-tenant orchestrator](multi-tenant.md#command-line).

## Compose: one node

```shell
cd docker/multitenant
cp .env.example .env && cp -r secrets.example secrets   # edit both; see "Configuration and credentials"
$EDITOR config/tenants.json                              # your tenants and seeds
docker compose -f compose.single.yml up -d --build
docker compose -f compose.single.yml ps                  # "healthy"
curl -s http://127.0.0.1:3000/api/tenants                # three tenants, Healthy: true
curl -s http://127.0.0.1:3000/api/tenants/mariadb-a/clusters
curl -s http://127.0.0.1:3000/api/raft-state             # "Leader"
```

What the file sets, beyond the image defaults: `init: true`, `restart: unless-stopped`, `stop_grace_period: 40s`,
`mem_limit: 384m`, a named volume `orc-data` on `/var/lib/orchestrator`, the configuration mounted read-only, the
secret mounted under `/run/secrets`, a network with a fixed subnet (`ORC_SUBNET`) and fixed node address (`ORC_N1_IP`),
and HTTP published on `127.0.0.1:3000` only. The Raft port 10008 is **not** published: it is only for the other
members of the group. A request for the legacy route `/api/clusters` answers `400` (several tenants, none named
`default`): always use `/api/tenants/<id>/...`.

## Compose: three nodes (Raft)

```shell
cd docker/multitenant
docker compose -f compose.raft3.yml up -d --build
for p in 3001 3002 3003; do curl -s http://127.0.0.1:$p/api/raft-state; echo; done    # one Leader, two Followers
curl -s http://127.0.0.1:3001/api/raft-leader                                         # the leader's Raft address
```

Each node has its own configuration file, volume (`orc-data-1..3`: Raft directory and its own SQLite backends) and
published port (`3001..3003`), the same `tenants.json`, `depends_on` node 1 being healthy for the other two (an
ordering convenience: Raft itself does not care which node starts first), `stop_grace_period: 40s` and its own
`mem_limit`. Any node answers any tenant request: followers forward to the leader, which runs the recoveries of **all**
the tenants (see [Multi-tenant orchestrator](multi-tenant.md#model)). The deployment of a Raft group in production
(addresses, quorum, what to monitor) is in [Raft deployment with several tenants](deployment-raft-multi-tenant.md).

For a load balancer in front of the three nodes probe `GET /health/leader`: `200` only on the leader, `503` on the
followers (`/api/leader-check` gives `200`/`404` the same way). Tenants are always addressed with the
`/api/tenants/<id>/` prefix, so the balancer needs no tenant awareness.

## Compose: MariaDB backend

Instead of SQLite files, every backend is a database of one MariaDB server (`backend` in the compose file): per node a
control database (`orc_n<i>_control`) and one database per tenant (`orc_n<i>_<tenant>`). **Every Raft node needs its own
set of databases**: Raft replicates commands and each node applies them to its own backend, so a database shared between
nodes would apply every command three times to the same rows. The Raft directory stays local (`orc-data-<i>`). The
server-side design (sizing, privileges, what a backend outage does) is in
[MySQL backend with several tenants](multi-tenant-mysql-backend.md).

```shell
cd docker/multitenant
cp .env.example .env && cp -r secrets.example secrets     # edit; secrets/orchestrator_backend.cnf is the backend login
docker compose -f compose.mysql-backend.yml up -d --build
docker compose -f compose.mysql-backend.yml exec backend sh -c \
  'mariadb -uroot -p"$(cat /run/secrets/mariadb_root_password)" -e "show databases like \"orc%\""'    # 12 databases
```

How it is wired:

* The global configuration of each node has `BackendDB: mysql`, `MySQLOrchestratorHost: backend` (the compose service
  name), `MySQLOrchestratorCredentialsConfigFile` and `MySQLOrchestratorMaxPoolConnections: 20`; its
  `MySQLOrchestratorDatabase` is the node's *control* database (`node_health`, election, access tokens).
* Each tenant (`config/mysql-backend/tenants-<i>.json`) only sets `Backend.MySQLOrchestratorDatabase`; host, port and
  credentials are inherited from the node's global configuration. Two tenants resolving to the same database are refused
  when loading.
* `config/mysql-backend/initdb/01-orchestrator-backend.sh` runs once, when the MariaDB data directory is created: it
  creates the backend account from the **same** credentials file orchestrator reads (one secret, one place) with
  `GRANT ALL ON `orc\_%`.*`; orchestrator creates the databases itself. Changing the secret afterwards needs an
  `ALTER USER` by hand, the script does not run again.
* The server does not start orchestrator until it is `healthy` (`depends_on: condition: service_healthy`, with the image's
  `healthcheck.sh --connect --innodb_initialized`).
* `max_connections=400`: each node keeps up to `MySQLOrchestratorMaxPoolConnections` (default 128, here 20) per backend,
  and it has four backends (control + three tenants): 3 x 4 x 20 = 240. Re-do that product when you add tenants or nodes.

## Volumes and persistence

| Data | Where | If you lose it |
|---|---|---|
| **Raft directory** | `/var/lib/orchestrator/raft` (`raft_store.db`, `snapshots/`) | The node forgets its Raft identity and log. Recoverable only by rebuilding the node ([below](#restarting-or-rebuilding-a-node)); if you lose a quorum of them at once the group is gone. |
| **SQLite backends** | `/var/lib/orchestrator/control.db` and `<tenant>.db` (+ `-wal`/`-shm`) | The history of each tenant on that node (audit, recoveries, tags, maintenance, downtimes) and the node's control state. Instances are rediscovered by polling; history is not. In a group a rebuilt node is re-seeded from the others through a Raft snapshot; a single node loses it. |
| **MariaDB backend** (compose.mysql-backend) | volume `backend-data` | The same, for every node and tenant at once: it is a single point of failure for the whole deployment. Back it up like any production database. |

All of it must be on persistent storage: a named volume or a bind mount (a bind-mounted directory must be owned by uid
10001: `sudo chown 10001:10001 /srv/orchestrator-1`). Do **not**:

* use `docker compose down -v` (or `docker volume rm`) except to destroy the deployment on purpose;
* share one `/var/lib/orchestrator` between nodes, or one backend database between nodes or tenants (loading refuses
  duplicates, but different nodes cannot be told apart);
* copy a running node's volume as a backup (SQLite WAL files are in flight): stop the node first, or snapshot at the
  storage layer. A restored node is brought up to date by Raft.

Configuration is **not** state: it is mounted from the host (or built into your own derived image) and is rebuilt
from your repository. The state of the monitored topologies is not here either.

## Network and ports

| Port | Use | Publish? |
|---|---|---|
| `3000/tcp` | HTTP: UI, API, health endpoints | Yes, but only where needed. The compose files bind it to `127.0.0.1` (`ORC_HTTP_BIND` to change it): the API is **unauthenticated by default**; before exposing it configure `AuthenticationMethod` or put a reverse proxy in front ([Security](security.md)). |
| `10008/tcp` | Raft (`DefaultRaftPort`) | **No.** Reachable only between the Raft members, on the compose network. |

* The nodes sit on a user-defined bridge network with a **fixed subnet and fixed addresses** because the Raft member id
  is the IP. To use another range, change `ORC_SUBNET`, `ORC_N<i>_IP` (compose, from `.env`) **and** the addresses in the
  `config/*/orchestrator-<i>.conf.json` files together; the defaults are `172.28.60.0/24` (single), `172.28.61.0/24`
  (raft3), `172.28.62.0/24` (mysql-backend). If another Docker network already uses that range, Docker refuses to create
  the network (`Pool overlaps with other one`).
* The topologies must be reachable from the container: MariaDB/MySQL on the seed ports, PostgreSQL on its port, and the
  addresses they report (`report_host`, `primary_conninfo`) must be routable from the nodes. Bridge networking NATs
  outbound traffic, which is fine for that. Replication-wise nothing connects *back* to orchestrator.
* Recovery hooks and `orchestrator-client` run inside the container: ship their scripts in a derived image or mount
  them, and remember the container has no tools beyond `bash curl jq`.
* Existing network: to attach the compose project to a network that already holds your databases, replace the `ipam`
  block of `orchnet` with `name: <existing>` and `external: true` (an external network cannot carry `ipam`; with an
  override file use `ipam: !reset null`) and keep the fixed addresses free in it. This is how the verification runs
  were done.

## Health checks

| Endpoint | `200` when | Use for |
|---|---|---|
| `GET /health/live` | the HTTP server answers | the image `HEALTHCHECK`; `restart` decisions |
| `GET /health/ready` | the health loop of the node reports healthy (`503` otherwise) | readiness gates |
| `GET /health/leader` | the node is the Raft leader (`503` on followers) | a load balancer that must reach the leader |
| `GET /api/health` | the control backend of the node is healthy; the body lists every tenant (`Healthy`, `Message`, `LastGood`) | monitoring |
| `GET /api/tenants` | always; one entry per tenant with `Healthy` | monitoring of individual tenants |
| `GET /api/raft-state` | always; `"Leader"` / `"Follower"` / `"Candidate"` | monitoring of the group |

All answers observed on the running examples (`/health/leader`: `200` on one node, `503` on the other two). The image
check only proves the HTTP server is up on purpose: a node that has lost its Raft quorum, or one whose tenant backend is
broken (which makes it give up the leadership), is not a reason to restart the container. Alert on `raft-state`
having no `Leader`, on `Healthy: false` in `/api/tenants` and on `orchestrator_tenant_set_mismatch`
([Metrics](multi-tenant.md#metrics)).

If you turn on HTTP authentication, TLS (`UseSSL`) or change `ListenAddress`, the unauthenticated probe breaks: set
`ORC_HEALTH_URL` in the container environment to a URL that works, or override `healthcheck:` in the compose file.

## Orderly shutdown

`docker stop` (and `docker compose stop/down/restart`) sends `SIGTERM`; the process-wide handler audits the shutdown of
every tenant (`auditType:shutdown ... [tenant:mariadb-a] Triggered via SIGTERM`) and exits with status 0. On the test
machine that takes well under a second (0.2-0.4 s measured). The grace period is margin, not an expectation: the
tenants' own stop path (`SIGINT`) waits up to 30 seconds for in-flight work, so the compose files set
`stop_grace_period: 40s` (`--stop-timeout 40` with `docker run`) instead of Docker's default 10 s. After the grace
period Docker sends `SIGKILL`, which Raft survives (the log is durable) but which leaves an unaudited shutdown.

PID 1 is the orchestrator itself; `init: true` / `--init` puts a minimal init in front so the processes that recovery
hooks spawn are reaped and signals reach the binary. On a host shutdown, `restart: unless-stopped` brings the node back
and it rejoins the group from its volume. Do not stop several members at once: with three nodes you tolerate the loss of
one.

## Logs

orchestrator logs to **stderr**; Docker collects it: `docker compose logs -f orchestrator-1`. The compose files set the
`json-file` driver with `max-size: 20m` and `max-file: 5` so a long-lived node does not fill the disk; use your usual
log driver in production. Useful lines:

* `raft: entering leader state` / `entering follower state` / `election won`, and `raft: ... heartbeat timeout reached`;
* one `auditType:shutdown ... [tenant:<id>]` line per tenant at shutdown;
* `FATAL cannot initialise the tenants: tenant "<id>": ...` at start-up ([Troubleshooting](#troubleshooting));
* `WARNING DiscoverInstance(host:port) instance is nil ... error=...` when a topology cannot be polled;
* the HTTP access log (one line per request, also the health probes: hence the 30 s interval of the image check).

`"Debug": true` in the configuration is very verbose (every discovery step, every Raft vote): use it for a few minutes
while debugging, not permanently. For metrics (`PrometheusEnabled`) see [Observability](observability.md); the
tenant-scoped series carry a `tenant` label.

## Memory limits

* **Orchestrator node:** `mem_limit: 384m` in the compose files (`ORC_MEM_LIMIT`). The test suites run nodes with
  384-512 MB; three tenants with two instances each use about 22-31 MiB, so there is headroom for hundreds of
  instances. Raise it with the number of instances, tenants and Raft snapshot size. An OOM-kill of the leader forces a
  re-election; check with `docker inspect -f '{{.State.OOMKilled}}' <container>`.
* **MariaDB backend:** the suites use 256-384 MB with `--innodb-buffer-pool-size=64M`, `--innodb-log-file-size=32M`,
  `--performance-schema=OFF` (as in the compose file). The deployment of this guide (3 nodes x 4 databases, about 85
  connections) used about 210 MiB of the 384 MiB limit. **Keep the container limit well above buffer pool + per-connection
  buffers + `max_connections` overhead**: an under-sized limit shows up as the server being killed (`OOMKilled: true`) and
  all the nodes failing at once. The MariaDB test helpers explicitly check for that.

## Updating the image (rolling)

The procedure and the version rules are in [Upgrading multi-tenant deployments](upgrade-multi-tenant.md); in short,
and **before adding a second tenant to a cluster that is still single-tenant, every node must already run a
multi-tenant version** ([requirement 3](multi-tenant.md#requirements)). With the compose group:

```shell
docker build -f docker/multitenant/Dockerfile -t orchestrator-mt:4.31.0 .     # new tag; never overwrite a tag in use
# in .env: ORCHESTRATOR_IMAGE=orchestrator-mt:4.31.0   (or edit it per service while testing)

for n in <follower> <follower> <leader>; do   # followers first, the current leader last: ask /api/raft-state on 3001..3003
  docker compose -f compose.raft3.yml up -d --no-deps --no-build orchestrator-$n
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "orchestrator-mt-raft3-orchestrator-$n-1")" = healthy ]; do sleep 2; done
  sleep 20; curl -s http://127.0.0.1:300$n/api/raft-state; echo          # Follower/Leader, never empty
done
```

One node at a time, waiting for it to be healthy and to have rejoined (`raft-state` answers) before the next. The data
volume stays, so a node comes back with its Raft state. If you used the MariaDB variant and the new version changes the
backend schema, orchestrator deploys it itself on start-up (each tenant database, about 5 s each).

## Restarting or rebuilding a node

* **Restart** (config change, new image): `docker compose -f compose.raft3.yml restart orchestrator-2`. Tested on the
  leader: another node took over within seconds and the restarted one rejoined as follower. A change of the tenants
  file always needs a restart of the node, one at a time (SIGHUP reloads the global configuration only).
* **Rebuild** a node whose state is lost or must be thrown away (corrupt Raft store, commands that could not be applied):
  delete its **Raft data and backend** and let it rejoin from the others, which re-seed it from a snapshot:

  ```shell
  docker compose -f compose.raft3.yml stop orchestrator-3
  docker compose -f compose.raft3.yml rm -f orchestrator-3
  docker volume rm orchestrator-mt-raft3_orc-data-3          # SQLite variant: removes raft/ AND the SQLite backends
  docker compose -f compose.raft3.yml up -d orchestrator-3
  ```

  Tested: the rebuilt node came back as `Follower`, with a transient `failed to get previous log ... log not found`
  warning and one `raft-follower-health-report ... 500` error while it caught up, and then served all three tenants.
  With the MariaDB backend also drop that node's databases (`orc_n3_*`) so the node does not start from stale
  rows, and see [Operations](multi-tenant-operations.md) for the full procedure, including re-adding a node to the
  group and replacing the leader.

## Troubleshooting

The start-up messages below are the real ones (each ends the container with exit status 1; `docker compose logs` shows
them). The common prefix is `FATAL cannot initialise the tenants:` and it names the tenant.

| Symptom (log) | Cause and fix |
|---|---|
| `tenants file /etc/orchestrator/nope.json: open ...: no such file or directory` | `TenantsConfigFile` points to a path that is not mounted. Mount the file read-only at that path. (If the mount **source** is missing on the host, Docker creates a directory there and the message is `...: is a directory`.) |
| `Cannot read config file: /etc/orchestrator/orchestrator.conf.json open ...: no such file or directory` | The global configuration is not mounted; the image has none. |
| `process: 3 tenants configured but Raft is disabled: with several tenants the leader election requires Raft ...` | `RaftEnabled` is `false`. Several tenants need Raft, even one node. |
| `tenants "mariadb-a" and "mariadb-b" share the same SQLite3DataFile /var/lib/orchestrator/mariadb-a.db` | Two tenants point at one backend. Give each its own file (or its own MariaDB database). |
| `tenant "mariadb-a": Config: field "RaftBind" is node-local and cannot be overridden per tenant` | A global/node-local setting is inside a tenant's `Config`. Move it to `orchestrator.conf.json`. |
| `tenant #3: invalid ID "Postgres C" (must match ^[a-z0-9][a-z0-9_-]{0,62}$)`; `tenant "x": duplicate ID`; `Provider: unknown provider "oracle" (valid: mysql, mariadb, postgresql)` | Fix the tenants file. |
| `tenant "mariadb-a": Failed to parse gcfg data from file: open /run/secrets/...: no such file or directory` | The credentials file is not mounted at that path (check the `secrets:` entry of the service). |
| same, `...: permission denied` | The secret is not readable by uid 10001: `sudo chown 10001:10001 secrets/*` (a `0400` file owned by someone else is unreadable inside the container). |
| `tenant "mariadb-a": db: backend "mariadb-a": cannot create sqlite backend "/var/lib/orchestrator/mariadb-a.db": open ...: permission denied` | The data directory is not writable by uid 10001. A bind mount needs `chown 10001:10001`; a named volume is fine. |
| `failed to open raft store: listen tcp 172.28.61.12:10008: bind: cannot assign requested address` | `RaftBind` is not an address of this container: the node got another IP. Keep the fixed `ipv4_address` and the subnet in sync with the configuration. |
| `tenant "mariadb-a": deploying the backend schema: Error 1045 (28000): Access denied for user 'orcbk'@...` (MariaDB backend) | The backend account does not exist or has another password. The init script only runs on an empty data directory; fix with `CREATE USER`/`ALTER USER` and the grant on `` `orc\_%`.* ``. |
| `tenant "mariadb-a": deploying the backend schema: Error 1044 (42000): Access denied for user 'orcbk'@'%' to database 'forbidden_db'` (MariaDB backend) | The backend account cannot create or use that database: its name does not match the `` `orc\_%` `` grant. Rename the database or widen the grant. |
| `tenants "mariadb-a" and "mariadb-b" share the same MySQL backend backend:3306/orc_n1_mariadb_a` | Two tenants of the node resolve to the same database. Each needs its own `Backend.MySQLOrchestratorDatabase`. |
| `Error response from daemon: invalid pool request: Pool overlaps with other one on this address space` | The subnet is used by another Docker network: change `ORC_SUBNET`, the node addresses and the configuration files. |
| `required variable ORC_B_TOPOLOGY_PASSWORD is missing a value` (compose) | `.env` is missing or does not define it. |

Runtime symptoms:

* **A tenant is `Healthy: true` but has no instances** and the log shows `WARNING DiscoverInstance(host:port) instance is
  nil ... error=tls: failed to verify certificate: x509: cannot validate certificate for <ip> because it doesn't contain
  any IP SANs`: seen here with a **wrong topology password** (the access-denied is retried over TLS and the TLS
  failure is what gets reported). Check the credentials with a client from the same network
  (`docker run --rm --network <net> mariadb:11.4 mariadb -h <host> -u <user> -p`); the same happens when the password
  comes from `${ENV}` and the variable is empty or wrong.
* **Instances appear twice** (once by name, once by IP): seeds and `report_host` use different names, see
  [Configuration and credentials](#configuration-and-credentials).
* **`400` on `/api/clusters`**: several tenants and none called `default`; use `/api/tenants/<id>/clusters`.
* **Two nodes both `Leader`, or none**: addresses of the Raft members are wrong or a node restarted with a different IP;
  compare `RaftNodes` with the real addresses (`docker inspect`) and see
  [Raft deployment with several tenants](deployment-raft-multi-tenant.md).
* **ERROR `unknown tenant` / `tenant_set_mismatch`**: the nodes do not have equivalent tenants files; see
  [Requirements](multi-tenant.md#requirements).
* **Container `Restarting`**: read the first lines of `docker logs`; start-up failures are all `FATAL cannot initialise ...`
  or a Raft bind error, as above.

## Verification

This guide was exercised with Docker Compose v5.4.0 (Compose v2 syntax) on Docker with MariaDB 11.4 and PostgreSQL 16
topologies (a MariaDB master/replica pair for each of two tenants and a PostgreSQL primary/standby for the third) on a
dedicated network:

* the image built from the `Dockerfile` of this directory, `hadolint` clean, running as uid 10001, healthy, stopping with
  exit status 0 on `SIGTERM`;
* every example `orchestrator.conf.json` / `tenants*.json` accepted by the real loader
  (`tests/multitenant/tools/validate-tenants`), including the credentials-file and `${ENV}` forms;
* `docker compose config` on the three compose files; `compose.single.yml` and the `docker run` command: three tenants
  discovered, each with its own master and replica and the right provider, `/api/tenants`, `/api/health` and the health
  endpoints answering, data surviving `stop`/`start`;
* `compose.raft3.yml`: exactly one leader, all three tenants served by every node, a leader restart, a follower stop/start,
  the rebuild of a node from an empty volume and the rolling image update loop above (new tag, followers first, leader
  last: the leadership moved to another node at the end and the group stayed whole);
* `compose.mysql-backend.yml`: twelve backend databases created by the nodes, the same three tenants discovered through
  any node;
* the start-up errors in the troubleshooting table, provoked on purpose.

Not covered by this guide's runs: building the image without network access to the Alpine mirrors (the `apk add` steps
need them; the Go build itself is offline), `linux/arm64`, a failover (recovery) executed from a container, and the
rebuild of a node of the MariaDB-backend variant.
