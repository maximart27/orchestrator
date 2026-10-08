# Multi-tenant configuration

One orchestrator process can manage several independent *tenants*. A tenant is a logical orchestrator instance:
its own configuration, its own database provider (exactly one engine: `mysql`/MariaDB or `postgresql`), its own
backend and its own discovery, analysis and recovery. All tenants of a process share the single Raft group, HTTP
server and dashboard.

Without a tenants file nothing changes: the process runs one implicit tenant called `default`, whose configuration
is the regular orchestrator configuration (`config.Config`) and whose provider is derived from `ProviderType`.

## Enabling it

Add the node-local setting `TenantsConfigFile` to the regular orchestrator configuration file:

```json
{
  "TenantsConfigFile": "/etc/orchestrator/tenants.json"
}
```

The tenants file is local to each node (like the TLS files or the backend settings): every Raft node must be given
an equivalent file.

## Tenants file format

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
        "MySQLTopologyUser": "orchestrator-a",
        "MySQLTopologyPassword": "${ORC_A_TOPOLOGY_PASSWORD}",
        "InstancePollSeconds": 5,
        "RecoverMasterClusterFilters": ["*"],
        "KVClusterMasterPrefix": "mariadb-a/master",
        "ConsulAddress": "consul.example.com:8500",
        "ProxySQLAdminAddress": "proxysql.example.com",
        "ProxySQLAdminUser": "radmin",
        "ProxySQLAdminPassword": "radmin-secret",
        "ProxySQLWriterHostgroup": 10,
        "ProxySQLReaderHostgroup": 20
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
        "MySQLTopologyUser": "orchestrator-b",
        "MySQLTopologyPassword": "${ORC_B_TOPOLOGY_PASSWORD}",
        "InstancePollSeconds": 10,
        "ReasonableReplicationLagSeconds": 60,
        "ReplicationLagQuery": "select unix_timestamp() - ts from percona.heartbeat",
        "PreFailoverProcesses": ["/usr/local/bin/pre-failover-b {failedHost}"],
        "PostMasterFailoverProcesses": ["/usr/local/bin/post-failover-b {successorHost}:{successorPort}"],
        "KVClusterMasterPrefix": "mariadb-b/master",
        "ConsulAddress": "consul.example.com:8500",
        "ProxySQLAdminAddress": "proxysql.example.com",
        "ProxySQLAdminUser": "radmin",
        "ProxySQLAdminPassword": "radmin-secret",
        "ProxySQLWriterHostgroup": 30,
        "ProxySQLReaderHostgroup": 40
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
        "PostgreSQLTopologyUser": "orchestrator-c",
        "PostgreSQLTopologyPassword": "secret-c",
        "PostgreSQLSSLMode": "verify-full",
        "InstancePollSeconds": 5,
        "RecoveryPeriodBlockSeconds": 600
      }
    }
  ]
}
```

| Attribute | Required | Meaning |
|-----------|----------|---------|
| `ID` | yes | Unique, `[a-z0-9][a-z0-9_-]{0,62}`. Used in URLs (`/api/tenants/{ID}/...`) and as Raft tenant key. |
| `Provider` | yes | `mysql` (alias `mariadb`) or `postgresql` (alias `postgres`). One engine per tenant. |
| `Backend` | no | Overrides of the backend database settings of the tenant (see below). |
| `Config` | no | Overrides of tenant-scoped settings (see below). |

Unknown attributes, unknown field names and fields in the wrong section are errors that name the tenant and the
field (for example `tenant "mariadb-b": Config: field "RaftBind" is node-local and cannot be overridden per tenant`).
Field names are matched case-insensitively, like in the regular configuration file. Errors never abort the
process by themselves: `config.LoadTenantConfigs` returns them to the caller.

## Which settings go where

Every field of `Configuration` is classified in `go/config/field_scope.go` (`config.FieldScope`). A test fails when
a field is added without a classification.

* **Tenant-scoped** (`Config` section): credentials and TLS towards the topology, `ProviderType`-specific settings,
  polling and discovery tuning and filters, detection queries, recovery and failover policy and hooks, ProxySQL
  (`ProxySQL*`), key-value stores (`Consul*`, `ZkAddress`, `KVClusterMasterPrefix`), `Discovery*Concurrency`,
  `DiscoveryQueueCapacity`, `TLSCacheTTLFactor`, `MySQLConnect*Seconds`, `ExpiryHostnameResolvesMinutes`,
  `AuditPurgeDays`, `RemoveTextFromHostnameDisplay`.
* **Backend node-local** (`Backend` section): `BackendDB`, `SQLite3DataFile` and `MySQLOrchestrator*`. Each
  tenant has its own backend (SQLite file per tenant in this version).
* **Global** (not allowed in a tenant, one value for the whole cluster): Raft membership, HTTP authentication,
  `PowerAuthUsers`, `PowerAuthGroups`, `ReadOnly`, `URLPrefix`, `WebMessage`, agents, access tokens...
* **Node-local** (not allowed in a tenant, inherited): listen addresses, TLS files of the HTTP server, logging,
  Graphite, Prometheus, write buffers, `TenantsConfigFile`...

`ProviderType` is not accepted inside `Config`: use the tenant's `Provider`.

## How the effective configuration of a tenant is built

1. `Cfg = base.Clone()`: a deep copy of the global configuration (already read and adjusted). Everything not
   mentioned in the tenant is inherited, including the value of global and node-local settings. `Cfg.ProviderType`
   is set to the tenant `Provider` and `Cfg.TenantsConfigFile` is cleared.
2. `Backend` and `Config` are decoded over the copy. Slices are replaced; maps (`ClusterNameToAlias`) are merged
   with the inherited one.
3. When the tenant engine differs from the global `ProviderType` and `DefaultInstancePort` is not set, it defaults
   to the engine port (3306 / 5432).
4. The same post-read adjustments as for the global configuration are applied to the tenant copy: credentials from
   `MySQLTopologyCredentialsConfigFile` / `${ENV}` passwords (only when the tenant sets them), synonym handling,
   `KVClusterMasterPrefix` normalization, `AutoPseudoGTID` patterns, derived defaults and consistency checks.
   Synonym pairs (`SlaveLagQuery`/`ReplicationLagQuery`, `PostponeSlaveRecoveryOnLagMinutes`/..., the `Detach*`
   flags, `RecoveryPeriodBlockMinutes`/`RecoveryPeriodBlockSeconds`) and `ReasonableLockedSemiSyncMasterSeconds`
   (derived from `ReasonableReplicationLagSeconds`) follow "the tenant wins": when the tenant sets only one side,
   the inherited value of the other side is discarded.

## Validation between tenants

Loading fails when tenants would silently interfere with each other:

* Duplicate `ID`.
* Same `SQLite3DataFile` (compared after path cleaning), or same MySQL backend (`host:port/database`). This also
  fires when several tenants inherit the same backend from the node configuration: each tenant needs its own.
* Same ProxySQL admin endpoint (`ProxySQLAdminAddress` + port, default 6032) with overlapping hostgroups
  (`ProxySQLWriterHostgroup` / `ProxySQLReaderHostgroup`; `0` means unconfigured and never conflicts).
* Same `KVClusterMasterPrefix` on tenants that share a KV store (same `ConsulAddress` or `ZkAddress`). This is an
  error, not a warning, because cluster masters of one tenant would overwrite the other's in Consul/ZooKeeper
  without any visible failure. Note that the default prefix (`mysql/master`) is the same for everybody, so tenants
  sharing Consul must set their own prefix, as in the example.

## Reloading

`config.Reload` (SIGHUP / `reload-configuration` API) only re-reads the regular configuration file. The tenants
manifest is not reloaded by it yet. A reload should call `LoadTenantConfigs` on the new base configuration, validate it
completely, and only then swap the tenants atomically: a manifest with errors must leave the running tenants
untouched.
