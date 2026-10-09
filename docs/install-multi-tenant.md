# Installing orchestrator in multi-tenant mode

This guide installs **one** orchestrator process that manages several tenants (independent MySQL/MariaDB and
PostgreSQL topologies) behind a single Raft group, HTTP listener and dashboard. It goes from the binary to a node
that is running, verified and supervised by systemd. What a tenant is, and what is shared between tenants, is in
[Multi-tenant orchestrator](multi-tenant.md); the format of the tenants file is in
[Multi-tenant configuration](multi-tenant-config.md).

The result of this guide is the `m1` layout of the end-to-end tests: one node, a Raft group of one member, three
tenants (`mariadb-a`, `mariadb-b`, `postgres-c`), one SQLite backend per tenant. It is the base for everything else:

* 3 Raft nodes: [Multi-tenant Raft deployment](deployment-raft-multi-tenant.md)
* MySQL/MariaDB as the backend of the tenants: [MySQL backend for multi-tenant](multi-tenant-mysql-backend.md)
* Docker and Compose: [Multi-tenant on Docker](docker-multi-tenant.md)
* Moving from a single-tenant install, rolling upgrade, rollback: [Upgrading to multi-tenant](upgrade-multi-tenant.md)
* Day-2 operations: [Multi-tenant operations](multi-tenant-operations.md)

> Without a tenants file orchestrator behaves exactly as before (one implicit tenant called `default`). Everything
> in this guide that is specific to multi-tenancy is driven by the node setting `TenantsConfigFile`.

## Contents

1. [Before you start](#before-you-start)
2. [Getting the binary](#getting-the-binary): [from source](#a-build-from-source), [packages](#b-deb-and-rpm-packages)
3. [Service user, directories and permissions](#service-user-directories-and-permissions)
4. [Configuration: one node, Raft of one, three tenants](#configuration-one-node-raft-of-one-three-tenants)
5. [The systemd service](#the-systemd-service)
6. [First start and verification](#first-start-and-verification)
7. [Troubleshooting](#troubleshooting)

## Before you start

| Decision / requirement | Why it matters |
|---|---|
| **Raft is mandatory with more than one tenant.** Even a single node needs `RaftEnabled: true` (a Raft group of one: empty `RaftNodes`). | Without Raft the leader election lives in the `active_node` row of one backend; with several backends there would be several leaders. The process refuses to start. |
| **One backend per tenant, never shared.** SQLite file per tenant (this guide) or one MySQL database per tenant (see [MySQL backend](multi-tenant-mysql-backend.md)). | Loading the tenants file fails when two tenants, or a tenant and the control backend of the node, resolve to the same SQLite file or MySQL database. |
| **The same tenants (IDs and providers) on every Raft node.** | A Raft command names its tenant; a node that does not run it drops the command for good. See [Requirements](multi-tenant.md#requirements). |
| **Ports.** HTTP `3000` (`ListenAddress`), Raft `10008` (`DefaultRaftPort`). | Open them between the Raft nodes and from your operators/load balancer. |
| **Grants on every topology.** One orchestrator account **per tenant**, scoped to that topology. | MySQL/MariaDB grants: [Installation](install.md#grant-access-to-orchestrator-on-all-your-mysql-servers). MariaDB 11 needs more than the documented list (`SLAVE MONITOR`, `REPLICATION SLAVE ADMIN`, `REPLICATION MASTER ADMIN`, `READ_ONLY ADMIN`, `BINLOG ADMIN`, `BINLOG MONITOR`); without `SLAVE MONITOR` nothing is discovered (`Error 1227`). PostgreSQL role and `pg_hba`: [PostgreSQL tenants](multi-tenant-config.md#postgresql-tenants-required-role-and-pg_hba). |
| **Agents are not available.** `ServeAgentsHttp` must stay `false`. | The agents (seeds) subsystem works on the legacy single-tenant backend only; the process refuses to start. |

## Getting the binary

Pick one of the two ways. Both end with the same layout:

| Path | What |
|---|---|
| `/usr/local/orchestrator/orchestrator` | the binary (also used as the working directory of the service) |
| `/usr/local/orchestrator/resources/` | web interface, templates and `resources/bin/orchestrator-client`; **read relative to the working directory** |
| `/etc/orchestrator/orchestrator.conf.json` | node configuration (you create it, see below) |
| `/etc/orchestrator/tenants.json` | tenants file (you create it) |
| `/var/lib/orchestrator/` | control SQLite, one SQLite per tenant, Raft data |

### (a) Build from source

Requirements:

* **Go 1.25.x** (`go.mod` declares `go 1.25.7`). An older toolchain cannot build it.
* **gcc** and the C library headers (`build-essential` on Debian/Ubuntu, `gcc glibc-devel` on RHEL): the SQLite
  driver (`go-sqlite3`) uses cgo, so `CGO_ENABLED=1` (Go's default when a C compiler is found).
* `git` and `rsync` if you use `./script/build` (it reads the commit and copies `resources/`).

The repository carries its dependencies in `vendor/`, so **no network access is needed to build**: Go uses the
vendor directory automatically (`-mod=vendor` makes it explicit).

```shell
git clone https://github.com/proxysql/orchestrator.git
cd orchestrator

# Option 1: the project script. Writes bin/orchestrator and bin/resources/.
./script/build

# Option 2: plain go build. Version and commit are embedded with -ldflags (optional).
go build -mod=vendor \
  -ldflags "-X main.AppVersion=$(cat RELEASE_VERSION) -X main.GitCommit=$(git rev-parse HEAD)" \
  -o bin/orchestrator ./go/cmd/orchestrator

bin/orchestrator --version
```

No Go 1.25 on the machine (or an air-gapped host)? Build in a container with the vendored sources; nothing is
downloaded:

```shell
docker run --rm --network none --user "$(id -u):$(id -g)" \
  -e GOCACHE=/tmp/gocache -e HOME=/tmp -e GOFLAGS=-mod=vendor \
  -v "$PWD":/src -w /src golang:1.25 \
  go build -o bin/orchestrator ./go/cmd/orchestrator
```

The binary is dynamically linked against glibc: build on a distribution that is not newer than the one that will
run it. Install it:

```shell
sudo install -d /usr/local/orchestrator
sudo install -m 0755 bin/orchestrator /usr/local/orchestrator/orchestrator
sudo rsync -a resources /usr/local/orchestrator/      # ./script/build already left a copy in bin/resources
```

### (b) .deb and .rpm packages

Packages are produced by `build.sh` with [fpm](https://github.com/jordansissel/fpm) (`gem install fpm`; `rpmbuild`
for the rpm). The simplest way, which needs only Docker and network access for the image build, is:

```shell
RELEASE_VERSION=4.30.0 script/dock pkg /tmp/packages      # runs build.sh inside docker/Dockerfile.packaging
```

or, with Go 1.25, fpm and rsync installed locally, `./build.sh` (add `-i sysv` for a SysV init script instead of
the systemd unit, `-a arm64` for another architecture, `-b` to build the binary only). Both leave the packages in
`/tmp/orchestrator-release` (or the target path given to `dock`). Published releases offer the same three variants
as `.deb`, `.rpm` and `.tar.gz`:

| Package | Contents (from `build.sh`) |
|---|---|
| `orchestrator` | `/usr/local/orchestrator/orchestrator` (binary), `/usr/local/orchestrator/resources/` (web UI, `resources/bin/orchestrator-client`), `/usr/local/orchestrator/orchestrator-sample.conf.json` and `orchestrator-sample-sqlite.conf.json`, and `/etc/systemd/system/orchestrator.service` (or `/etc/init.d/orchestrator` with `-i sysv`). Depends on `jq >= 1.5`. |
| `orchestrator-cli` | `/usr/bin/orchestrator` only: for command-line use and API-only servers without the web UI. |
| `orchestrator-client` | `/usr/bin/orchestrator-client` only (a shell wrapper of the HTTP API, needs `jq`). |

```shell
sudo dpkg -i /tmp/packages/orchestrator_*_amd64.deb            # Debian / Ubuntu
sudo rpm -i  /tmp/packages/orchestrator-[0-9]*.x86_64.rpm          # RHEL family
sudo tar -C / -xzf orchestrator-<version>-linux-amd64.tar.gz   # tarball: its paths start at /, e.g. ./usr/local/orchestrator
```

Two things the package does **not** do, and that this guide adds:

* It does not create a service user: the packaged `orchestrator.service` runs as **root**, starts
  `orchestrator http` with no `--config`, and reads `/etc/orchestrator.conf.json`. For multi-tenant use install the
  unit of this repository instead (see [The systemd service](#the-systemd-service)) and disable the packaged one.
* It does not ship the multi-tenant unit, nor a tenants file.

## Service user, directories and permissions

orchestrator holds credentials (topology accounts, backend passwords) and its SQLite files hold host names, aliases
and the access tokens of the API. Run it as an unprivileged user and keep everything private to it:

```shell
sudo useradd --system --home-dir /var/lib/orchestrator --no-create-home --shell /usr/sbin/nologin orchestrator

sudo install -d -o orchestrator -g orchestrator -m 0750 /etc/orchestrator /etc/orchestrator/credentials
sudo install -d -o orchestrator -g orchestrator -m 0700 /var/lib/orchestrator   # systemd would also create it (StateDirectory)
```

| Path | Owner | Mode | Notes |
|---|---|---|---|
| `/usr/local/orchestrator/` (binary, `resources/`) | `root` | `0755` | The service must be able to read `resources/` but not modify it. |
| `/etc/orchestrator/` | `orchestrator` | `0750` | |
| `/etc/orchestrator/orchestrator.conf.json` | `orchestrator` | `0600` | Global/node-local settings. Holds no tenant credentials in this guide, but may hold the backend ones. |
| `/etc/orchestrator/tenants.json` | `orchestrator` | `0600` | May contain credentials (PostgreSQL password, hook commands). |
| `/etc/orchestrator/credentials/<tenant>.cnf` | `orchestrator` | `0600` | One credentials file per MySQL/MariaDB tenant (below). |
| `/var/lib/orchestrator/` | `orchestrator` | `0700` | SQLite files and Raft data. orchestrator creates new SQLite files as `0600` (with their `-wal`/`-shm`); it logs a warning if it finds an existing one readable by others. It does **not** create the directory: a missing one is a startup error. |

The `0600` configuration files owned by the service user are the layout used by this guide; with
`ProtectSystem=strict` (see the unit) the service cannot write to `/etc` anyway. If you prefer the service not to own
its configuration, make the files `root:orchestrator` `0640`.

### Credentials: files, not `${ENV}`

Hooks are child processes of orchestrator and **inherit its whole environment**. A password given as
`"MySQLTopologyPassword": "${ORC_A_TOPOLOGY_PASSWORD}"` therefore lives in the environment of the process and can
be read by the hooks of *every* tenant (see [Security between tenants](multi-tenant.md#security-between-tenants)).
Give each MySQL/MariaDB tenant its own `MySQLTopologyCredentialsConfigFile`, a `my.cnf`-style file with a `[client]`
section from which orchestrator takes **both** the user and the password (it overrides `MySQLTopologyUser` and
`MySQLTopologyPassword` of that tenant):

```shell
sudo install -o orchestrator -g orchestrator -m 0600 /dev/stdin /etc/orchestrator/credentials/mariadb-a.cnf <<'EOF'
[client]
user=orchestrator_a
password=change-me-a
EOF
sudo install -o orchestrator -g orchestrator -m 0600 /dev/stdin /etc/orchestrator/credentials/mariadb-b.cnf <<'EOF'
[client]
user=orchestrator_b
password=change-me-b
EOF
```

PostgreSQL has no credentials-file or `${ENV}` mechanism: `PostgreSQLTopologyPassword` goes in the tenants file,
which is why that file is `0600`.

## Configuration: one node, Raft of one, three tenants

Two files. The **node configuration** holds what is global or node-local: listen address, the Raft settings and the
backend of the node itself (the *control* backend: node heartbeat, access tokens) and the path of the tenants file.

<!-- validate:install-first:node -->
```json
{
  "Debug": true,
  "ListenAddress": ":3000",
  "HTTPAdvertise": "http://orchestrator1.example.com:3000",
  "BackendDB": "sqlite3",
  "SQLite3DataFile": "/var/lib/orchestrator/control.db",
  "RaftEnabled": true,
  "RaftDataDir": "/var/lib/orchestrator/raft",
  "RaftBind": "10.0.0.11",
  "RaftAdvertise": "10.0.0.11",
  "DefaultRaftPort": 10008,
  "RaftNodes": [],
  "TenantsConfigFile": "/etc/orchestrator/tenants.json"
}
```

* `"RaftNodes": []` is a Raft group of one: the node is its own, implicit, leader. `RaftBind`/`RaftAdvertise` are
  the IP of this host; `HTTPAdvertise` is the URL where this node would be reached if it were the leader (followers
  proxy requests to it).
* `SQLite3DataFile` is the **control** backend. It must be different from the file of every tenant and from the Raft
  store (`<RaftDataDir>/raft_store.db`). There is no tenant called `default` here; if there were one, it would host
  the control backend instead.
* `"Debug": true` makes the first start easy to follow; set it to `false` (or remove it) once it works.
* Everything not listed here keeps its default, and every tenant **inherits** all the global and node-local values.

The **tenants file** declares the tenants. Values that differ between tenants on purpose (credentials, poll
interval, filters, KV prefix, hooks) are what makes a cross-tenant leak visible; adapt them to your databases.

<!-- validate:install-first:tenants -->
```json
{
  "Tenants": [
    {
      "ID": "mariadb-a",
      "Provider": "mysql",
      "Backend": {
        "BackendDB": "sqlite3",
        "SQLite3DataFile": "/var/lib/orchestrator/mariadb-a.db"
      },
      "Config": {
        "MySQLTopologyCredentialsConfigFile": "/etc/orchestrator/credentials/mariadb-a.cnf",
        "InstancePollSeconds": 3,
        "HostnameResolveMethod": "none",
        "MySQLHostnameResolveMethod": "none",
        "DiscoverySeeds": ["db-a1.example.com:3306", "db-a2.example.com:3306"],
        "DetectClusterAliasQuery": "select 'alias-a'",
        "DetectInstanceAliasQuery": "select concat('a-', @@server_id)",
        "RecoverMasterClusterFilters": ["*"],
        "RecoverIntermediateMasterClusterFilters": ["*"],
        "RecoveryPeriodBlockSeconds": 120,
        "KVClusterMasterPrefix": "mariadb-a/master",
        "PostMasterFailoverProcesses": [
          "echo \"tenant=mariadb-a tid={tenantId} env=$ORC_TENANT_ID failed={failedHost} successor={successorHost}\" >> /var/lib/orchestrator/hook-mariadb-a.log"
        ]
      }
    },
    {
      "ID": "mariadb-b",
      "Provider": "mariadb",
      "Backend": {
        "BackendDB": "sqlite3",
        "SQLite3DataFile": "/var/lib/orchestrator/mariadb-b.db"
      },
      "Config": {
        "MySQLTopologyCredentialsConfigFile": "/etc/orchestrator/credentials/mariadb-b.cnf",
        "InstancePollSeconds": 5,
        "ReasonableReplicationLagSeconds": 60,
        "AuditPurgeDays": 3,
        "HostnameResolveMethod": "none",
        "MySQLHostnameResolveMethod": "@@report_host",
        "DiscoverySeeds": ["db-b1.example.com:3306", "db-b2.example.com:3306"],
        "DetectClusterAliasQuery": "select 'alias-b'",
        "DetectInstanceAliasQuery": "select concat('b-', @@server_id)",
        "RecoverMasterClusterFilters": [".*"],
        "RecoverIntermediateMasterClusterFilters": [".*"],
        "RecoveryPeriodBlockSeconds": 120,
        "KVClusterMasterPrefix": "mariadb-b/master",
        "PreFailoverProcesses": [
          "echo \"tenant=mariadb-b pre failed={failedHost}\" >> /var/lib/orchestrator/hook-mariadb-b-pre.log"
        ],
        "PostMasterFailoverProcesses": [
          "echo \"tenant=mariadb-b tid={tenantId} env=$ORC_TENANT_ID failed={failedHost} successor={successorHost}\" >> /var/lib/orchestrator/hook-mariadb-b.log"
        ]
      }
    },
    {
      "ID": "postgres-c",
      "Provider": "postgresql",
      "Backend": {
        "BackendDB": "sqlite3",
        "SQLite3DataFile": "/var/lib/orchestrator/postgres-c.db"
      },
      "Config": {
        "PostgreSQLTopologyUser": "orchestrator_c",
        "PostgreSQLTopologyPassword": "change-me-c",
        "PostgreSQLSSLMode": "disable",
        "InstancePollSeconds": 4,
        "HostnameResolveMethod": "none",
        "DiscoverySeeds": ["pg-c1.example.com:5432", "pg-c2.example.com:5432"],
        "RecoverMasterClusterFilters": [".*"],
        "RecoverIntermediateMasterClusterFilters": [".*"],
        "RecoveryPeriodBlockSeconds": 120,
        "KVClusterMasterPrefix": "postgres-c/master"
      }
    }
  ]
}
```

What to notice:

* `ID` is `[a-z0-9][a-z0-9_-]{0,62}`, unique, and appears in URLs (`/api/tenants/mariadb-a/...`), in the `tenant`
  label of the metrics and as the Raft key of the tenant. Choose it once: renaming a tenant is a new tenant.
* `Provider` is `mysql` (alias `mariadb`) or `postgresql` (alias `postgres`), exactly one engine per tenant.
* `Backend` holds only the backend settings (`BackendDB`, `SQLite3DataFile`, `MySQLOrchestrator*`); `Config` only
  tenant-scoped settings. A Raft, HTTP, authentication or logging setting inside a tenant is rejected, naming the
  field. `ProviderType` is not accepted either: that is the tenant's `Provider`.
* `RecoveryPeriodBlockSeconds` must be **longer than a failover under Raft** (40-60 s); the default (3600) is fine,
  and these examples use 120 as the lowest sensible value.
* The hooks write under `/var/lib/orchestrator/` because that is the only writable place of the service (see the
  unit). They show how a hook learns its tenant: `{tenantId}` in the command line and `$ORC_TENANT_ID` in its
  environment. Replace them with your real scripts.
* Tenants that share a ProxySQL admin endpoint need disjoint hostgroups, and tenants that share a Consul or ZooKeeper
  need different `KVClusterMasterPrefix` values (the three above have their own). Both are checked at load time.

Write both files, set their ownership and mode, and **validate them before starting**.
`orchestrator -c tenants` loads the node configuration and the tenants file with the same code the service uses,
checks everything between tenants, reads the credentials files and prints one line per tenant, without opening any
backend or contacting any database:

```shell
sudoedit /etc/orchestrator/orchestrator.conf.json     # the node configuration above
sudoedit /etc/orchestrator/tenants.json               # the tenants file above
sudo chown orchestrator:orchestrator /etc/orchestrator/*.json
sudo chmod 0600 /etc/orchestrator/*.json

sudo -u orchestrator /usr/local/orchestrator/orchestrator \
  --config /etc/orchestrator/orchestrator.conf.json --ignore-raft-setup -c tenants
```

```text
mariadb-a	mysql	sqlite3:/var/lib/orchestrator/mariadb-a.db
mariadb-b	mysql	sqlite3:/var/lib/orchestrator/mariadb-b.db
postgres-c	postgresql	sqlite3:/var/lib/orchestrator/postgres-c.db
```

`--ignore-raft-setup` is needed because the command line refuses to run when Raft is enabled (all access must go
through the API of the active node); it is safe here because `-c tenants` touches no backend. A mistake ends in a
`FATAL` line that names the tenant and the field (see [Troubleshooting](#troubleshooting)).

> Pass `--config` explicitly in every command-line invocation: without it orchestrator reads
> `/etc/orchestrator.conf.json`, `conf/orchestrator.conf.json` and `orchestrator.conf.json`, not the file above.
> Be careful with `-c dump-config`: it prints the effective configuration of the tenant, **passwords included**.

## The systemd service

The repository ships a ready unit, [`etc/systemd/orchestrator-multitenant.service`](../etc/systemd/orchestrator-multitenant.service)
(and [`orchestrator-multitenant.env.example`](../etc/systemd/orchestrator-multitenant.env.example)). It has its own
name so that a package upgrade never overwrites it. It is checked with `systemd-analyze verify`, and the binary was
also run, as a user-level transient unit, under this unit's syscall filter, address-family restriction,
`NoNewPrivileges` and private `/tmp`, and stopped with `SIGTERM`, to confirm they do not get in the way of discovery,
Raft or the SQLite backends. The capability, device and read-only file-system restrictions cannot be reproduced
that way: test the unit on a staging host (`systemd-analyze security orchestrator-multitenant.service`) before
production.

```shell
sudo systemctl disable --now orchestrator 2>/dev/null     # the packaged unit, if any
sudo install -m 0644 etc/systemd/orchestrator-multitenant.service /etc/systemd/system/orchestrator-multitenant.service
sudo systemctl daemon-reload
sudo systemctl enable --now orchestrator-multitenant
journalctl -u orchestrator-multitenant -f
```

The directives that are not obvious:

| Directive | Why |
|---|---|
| `User=orchestrator`, `Group=orchestrator`, `UMask=0077` | Unprivileged service; everything it creates (SQLite files, Raft data, hook output) is private to it. |
| `WorkingDirectory=/usr/local/orchestrator` | The web interface is read from `resources/` **relative to the working directory**. Move the install and this must follow. |
| `ExecStart=... --config /etc/orchestrator/orchestrator.conf.json $ORC_ARGS http` | An explicit `--config`, so the unit does not depend on the default lookup. Flags must precede `http`. `ORC_ARGS` defaults to `--verbose` and can be overridden in the optional environment file (`-` prefix: absent is fine). |
| `ExecReload=/bin/kill -HUP $MAINPID` | `SIGHUP` re-reads only the **global** configuration (the one of the legacy `default` tenant). A running tenant keeps the configuration it started with: to change a tenant or the tenants file, **restart** the node (one Raft node at a time). |
| `KillSignal=SIGTERM`, `TimeoutStopSec=45` | On `SIGTERM` orchestrator stops the tenants in order (flushes write buffers, closes the backends) and waits **up to 30 s** before exiting. 45 s leaves margin so systemd never sends `SIGKILL` in the middle of an orderly stop. |
| `Restart=on-failure`, `RestartSec=5` | Restart after a crash, not after a clean stop. A fail-fast configuration error would loop every 5 s: fix it or `systemctl stop`. |
| `LimitNOFILE=65536` | One socket per discovered instance of **every** tenant plus HTTP and Raft connections. The packaged unit's 16384 is sized for one tenant. |
| `StateDirectory=orchestrator`, `StateDirectoryMode=0700` | systemd creates `/var/lib/orchestrator` (owned by the service user, `0700`) and keeps it writable under `ProtectSystem=strict`. |
| `ProtectSystem=strict`, `ReadWritePaths=/var/lib/orchestrator` | The whole file system is read-only except the state directory. If you keep SQLite or Raft data elsewhere (`/data/orchestrator`), add that path here: orchestrator cannot create a backend it cannot write. |
| `ProtectHome`, `PrivateTmp`, `PrivateDevices`, `Protect*`, `Restrict*`, `LockPersonality` | Standard hardening: no access to home directories, private `/tmp` and `/dev`, no kernel tunables, no namespaces, no realtime scheduling, no setuid files. `systemd-analyze security` rates the unit "1.5 OK". |
| `NoNewPrivileges=true`, `CapabilityBoundingSet=` | The service and its hooks cannot gain privileges: a hook that calls `sudo` or a setuid binary will fail. Listening on a port below 1024 needs `AmbientCapabilities=CAP_NET_BIND_SERVICE` and the same value in the bounding set. |
| `RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX`, `SystemCallFilter=@system-service` | Enough for TCP to the databases, Raft, HTTP and local sockets (NSS/SSSD). A hook that needs `AF_NETLINK`, raw sockets or special system calls must be allowed explicitly. |
| `PrivateTmp=true` | Hooks see their own `/tmp`: do not use it to exchange files with other services. |

Hooks run with these restrictions too. If a hook writes outside `/var/lib/orchestrator`, add the directory to
`ReadWritePaths=`; if it must reach other users' files or processes, relax the relevant directive deliberately
instead of dropping the sandbox.

## First start and verification

`systemctl start` returns immediately; success means the process stayed up. In the journal look for, in this order:

```text
INFO Backend of tenant mariadb-a is ready          (one per tenant, and "_control" for the node backend)
INFO tenant mariadb-a: continuous discovery: starting    (one per tenant)
INFO Starting HTTP listener on :3000
raft: entering leader state: leader="Node at 10.0.0.11:10008 [Leader]"
```

The tenants are built **before** anything else starts and the HTTP listener opens last, so a node that answers HTTP
has all its tenants. If one cannot be initialised the process exits with one `FATAL cannot initialise the tenants: ...`
line (never a half-started node).

Then verify from the node itself:

```shell
# 1. The tenants this node runs, with provider and backend health (node-local, never proxied to the leader).
curl -s localhost:3000/api/tenants | jq '.Details[] | {ID, Provider, Healthy, Message}'

# 2. Node health: Details.Tenants repeats the health of every tenant; Details.IsRaftLeader says who leads.
curl -s localhost:3000/api/health | jq '{Code, Message, leader: .Details.IsRaftLeader, tenants: [.Details.Tenants[] | {ID, Healthy}]}'

# 3. One Raft group, this node is its leader.
curl -s localhost:3000/api/raft-state; echo
curl -s localhost:3000/api/raft-health; echo

# 4. A call on one tenant only (and the legacy path, which is ambiguous with several tenants and no "default").
curl -s localhost:3000/api/tenants/mariadb-a/clusters; echo
curl -s -o /dev/null -w '%{http_code}\n' localhost:3000/api/clusters       # 400: tenantId required
```

Expected: `/api/tenants` answers `"Code": "OK"`, `"Message": "3 tenants"`, every tenant `Healthy: true` with an empty
`Message`; `/api/health` is `"Application node is healthy"`; `/api/clusters` without a tenant is `400` because the
request names no tenant and there is neither a `default` tenant nor a single one. An unknown tenant is a `404`
(`unknown tenant "x"; available tenants: ...`). A tenant that shows `Healthy: false` has a `Message` with the last
error of its backend; the databases of the examples do not exist, so expect discovery warnings
(`lookup db-a1.example.com ...: no such host`) until you point the seeds at real servers.

With the **command line** (only through `--ignore-raft-setup` while Raft is on, and always for one tenant, chosen
with `-tenant`, `ORCHESTRATOR_TENANT`, the tenant named `default` or the only tenant, in that order):

```shell
sudo -u orchestrator /usr/local/orchestrator/orchestrator --config /etc/orchestrator/orchestrator.conf.json -c tenants --ignore-raft-setup
sudo -u orchestrator /usr/local/orchestrator/orchestrator --config /etc/orchestrator/orchestrator.conf.json -c clusters -tenant mariadb-a --ignore-raft-setup
```

or, much better with Raft, through the API with `orchestrator-client` (`-T <tenant>` or `ORCHESTRATOR_TENANT`; note
that `-t` is the tag flag, the tenant flag is an uppercase `-T`):

```shell
export ORCHESTRATOR_API=http://localhost:3000/api
orchestrator-client -c tenants
orchestrator-client -T mariadb-a -c clusters
ORCHESTRATOR_TENANT=postgres-c orchestrator-client -c clusters
```

Without a tenant, `orchestrator -c clusters` fails with
`3 tenants are configured (...) and none is named "default": choose one with -tenant <id> or the ORCHESTRATOR_TENANT environment variable`.

**Metrics** (`/metrics`, Prometheus): the tenant-scoped series carry a `tenant` label, registered once; the node
level ones have none. The two that tell you whether the tenants are consistent across Raft nodes:

```shell
curl -s localhost:3000/metrics | grep -E '^orchestrator_(tenant_set_mismatch|raft_unknown_tenant_commands_total|raft_is_leader|raft_is_healthy)'
curl -s localhost:3000/metrics | grep -E '^orchestrator_(discoveries_total|instances_total)\{'
```

| Metric | Healthy value |
|---|---|
| `orchestrator_tenant_set_mismatch` | `0`. `1` means the tenants of this node differ from the ones announced by the Raft leader (the leader announces its tenant IDs and providers when elected and every 5 minutes; with one node it only ever compares with itself). Alert on it. The go-metrics name is `raft.tenant_set_mismatch`. |
| `orchestrator_raft_unknown_tenant_commands_total` | `0` and flat. Counts the Raft commands this node could not apply since start because their tenant does not exist here; they are never retried. Go-metrics name: `raft.unknown_tenant_commands`. |
| `orchestrator_discoveries_total{tenant="..."}`, `orchestrator_instances_total{tenant="..."}` | One series per tenant, growing once the seeds are reachable. |

Finally, once real seeds are reachable, open `http://orchestrator1.example.com:3000/web/tenants/mariadb-a/clusters`
(or `/web/` to choose a tenant) and check that each tenant lists only its own clusters.

## Troubleshooting

### The process does not start

All of these are `FATAL` lines at startup, and the node exits (systemd: `status=1/FAILURE`, then `Restart=` retries
every 5 s). The tenant name and the offending field are always in the message. `<file>` stands for the path of
your tenants file.

| Message (abridged) | Cause and fix |
|---|---|
| `cannot initialise the tenants: process: 3 tenants configured but Raft is disabled: with several tenants the leader election requires Raft ...` | More than one tenant with `RaftEnabled: false`. Enable Raft (a group of one is fine: `"RaftEnabled": true`, `"RaftNodes": []`) or keep a single tenant. |
| `ServeAgentsHttp is not supported together with TenantsConfigFile: the agents (seeds) subsystem works on the legacy single-tenant backend only` | Set `ServeAgentsHttp` to `false` (or drop it) in the node configuration. Agents and agent seeds are outside multi-tenant. |
| `tenants file <file>: open <file>: no such file or directory` | `TenantsConfigFile` points to a missing file or one the service user cannot read. Check path, owner and mode (`0600`, owned by `orchestrator`). |
| `tenants file <file>: invalid tenants manifest: invalid character '}' looking for beginning of object key string` (or similar) | Not valid JSON (a trailing comma is the usual cause). Unknown top-level keys and trailing data are rejected too. |
| `tenant #1: invalid ID "Maria DB" (must match ^[a-z0-9][a-z0-9_-]{0,62}$)` / `tenant "mariadb-a": duplicate ID` | Fix the ID: lower case, digits, `_`, `-`, at most 63 characters, unique. |
| `tenant "x": Provider: unknown provider "oracle" (valid: mysql, mariadb, postgresql)` | Also thrown when `Provider` is missing (`Provider is required`). |
| `tenant "x": Config: unknown field "NoSuchSetting"` | A mistyped setting. Field names are case-insensitive but must exist. |
| `tenant "x": Config: field "RaftBind" is node-local and cannot be overridden per tenant (set it in the node configuration)` | Raft, HTTP, authentication, logging, Prometheus... are global or node-local. Move the setting to the node configuration. |
| `tenant "x": Config: field "SQLite3DataFile" is backend node-local and must be placed in the tenant "Backend" section` | Backend settings go in `Backend`, not `Config`. |
| `tenant "x": Config: field "ProviderType" is not accepted in a tenant: use the tenant "Provider" attribute` | Use `Provider`. |
| `tenant "x": Failed to parse gcfg data from file: open /etc/orchestrator/credentials/x.cnf: no such file or directory` | The credentials file is missing, unreadable by the service user, or not a `[client]` file with `user=`/`password=`. |
| `tenants "mariadb-a" and "mariadb-b" share the same SQLite3DataFile <path>` | Two tenants resolve to one file (paths are compared after cleaning and resolving symlinks). One backend per tenant. A tenant with no `Backend` of its own **inherits** the node's one: set it explicitly. |
| `tenant "x" uses the SQLite3DataFile <path> of the node's own (control) backend: give the tenant its own Backend.SQLite3DataFile` | The tenant file equals the node's `SQLite3DataFile`. Same for MySQL (`... uses the MySQL backend host:port/db of the node's own (control) backend`) and `tenant "x" SQLite3DataFile <path> is the Raft store`. |
| `tenants "a" and "b" use the same ProxySQL host:6032 with overlapping hostgroup 20` | Tenants sharing one ProxySQL admin endpoint need disjoint writer/reader hostgroups (`0` means unconfigured). |
| `tenants "a" and "b" share a KV store and the same KVClusterMasterPrefix "mysql/master/": give each tenant its own prefix` | Tenants sharing a Consul or ZooKeeper address must publish masters under different prefixes. The default prefix is the same for everybody. |
| `tenant "x": db: backend "x": cannot create sqlite backend "<path>": open <path>: no such file or directory` | The **directory** of the SQLite file does not exist (orchestrator creates the file, not the directory). Create it, or let `StateDirectory=` do it. |
| `... open <path>: permission denied` (or `read-only file system`) | The service user cannot write there. Check the directory owner and, under systemd, that the path is covered by `StateDirectory=`/`ReadWritePaths=`. |

Two more that are not about the tenants file: `ListenAddress` or the Raft port already in use (another process, or
the packaged `orchestrator` service still enabled: `systemctl disable --now orchestrator`), and the systemd exit
statuses `217/USER` (the `orchestrator` user does not exist), `200/CHDIR` (`WorkingDirectory` missing),
`203/EXEC` (binary path wrong) and `226/NAMESPACE` (a `ReadWritePaths=` directory does not exist yet).

### It runs, but something is off

| Symptom | Cause and fix |
|---|---|
| `GET /api/clusters` (or any legacy route) answers `400 tenantId required: the request names no tenant ...` | With several tenants and none called `default` the legacy routes are ambiguous. Use `/api/tenants/<id>/...`, the `X-Orchestrator-Tenant` header, or the `tenant` query parameter. |
| `orchestrator -c ...` says `Orchestrator configured to run raft ... All access must go through the web API` | Expected with Raft. Use `orchestrator-client` (or `--ignore-raft-setup` for commands that touch no backend, like `-c tenants`). |
| `orchestrator -c clusters` says `N tenants are configured (...) and none is named "default"` / `tenant "x" does not exist; available tenants: ...` | Choose the tenant: `-tenant <id>` or `ORCHESTRATOR_TENANT`. |
| `ERROR orchestrator/raft: the tenants of this node (a,b) differ from the ones of the leader (...): ...` and `orchestrator_tenant_set_mismatch 1` | The nodes do not run the same tenants. Make every `tenants.json` declare the same IDs and providers and restart the node that differs. Only meaningful with more than one Raft node ([Raft deployment](deployment-raft-multi-tenant.md)). |
| `ERROR orchestrator/raft: index=13 op=... tenant=x: command NOT applied on this node, tenant unknown here` and `orchestrator_raft_unknown_tenant_commands_total` > 0 | This node received a command for a tenant it does not run: it is lost **for that node for good**, and adding the tenant later does not recover it. Rebuild the node: stop it, delete its `RaftDataDir`, start it so it re-joins and restores from the leader. |
| A tenant is `Healthy: false` in `/api/tenants` | Its backend is failing (disk full, permissions, MySQL backend down). By design a node with a broken tenant **yields the Raft leadership** so another node runs all the tenants; the process never exits for it. Fix the backend. |
| Failover repeats / the hook runs several times for one failure | `RecoveryPeriodBlockSeconds` shorter than a failover under Raft (40-60 s). Keep it at 120 or more. |
| Changing `tenants.json` has no effect | `SIGHUP` and `reload-configuration` reload only the global configuration. Restart the node (one Raft node at a time). |
| Credentials of tenant A visible to tenant B's hooks | They are in the process environment (`${ENV}`). Switch to `MySQLTopologyCredentialsConfigFile`. |

## Next steps

* Three Raft nodes with the same tenants, and what the leader announces: [Multi-tenant Raft deployment](deployment-raft-multi-tenant.md).
* Already running a single-tenant orchestrator? [Upgrading to multi-tenant](upgrade-multi-tenant.md).
* The end-to-end tests that exercise exactly this layout (`run-m1.sh`, `run-m2.sh`, chaos and mixed-version suites): [`tests/multitenant/README.md`](../tests/multitenant/README.md).
