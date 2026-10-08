# Configuration: basic discovery

Let orchestrator know how to query the MySQL topologies, what information to extract.

```json
{
  "MySQLTopologyCredentialsConfigFile": "/etc/mysql/orchestrator-topology.cnf",
  "InstancePollSeconds": 5,
  "DiscoverByShowSlaveHosts": false,
}
```

`MySQLTopologyCredentialsConfigFile` follows similar rules as `MySQLOrchestratorCredentialsConfigFile`. You may choose to use plaintext credentials:

```
[client]
user=orchestrator
password=orc_topology_password
```

Or, you may choose to use plaintext credentials:

```json
{
  "MySQLTopologyUser": "orchestrator",
  "MySQLTopologyPassword": "orc_topology_password",
}
```

`orchestrator` will probe each server once per `InstancePollSeconds` seconds.

On all your MySQL topologies, grant the following:

```
CREATE USER 'orchestrator'@'orc_host' IDENTIFIED BY 'orc_topology_password';
GRANT SUPER, PROCESS, REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'orchestrator'@'orc_host';
GRANT SELECT ON meta.* TO 'orchestrator'@'orc_host';
GRANT SELECT ON ndbinfo.processes TO 'orchestrator'@'orc_host'; -- Only for NDB Cluster
GRANT SELECT ON performance_schema.replication_group_members TO 'orchestrator'@'orc_host'; -- Only for Group Replication / InnoDB cluster
```

#### MariaDB 10.5.9 and later (including 11.x)

MariaDB split several privileges out of `SUPER`. With only the grants above, `SHOW SLAVE STATUS` fails with
`Error 1227: Access denied; you need (at least one of) the SLAVE MONITOR privilege(s)` and `orchestrator` discovers
nothing. On MariaDB 10.5.9+ / 11.x grant also:

```
GRANT SLAVE MONITOR, REPLICATION SLAVE ADMIN ON *.* TO 'orchestrator'@'orc_host';
GRANT REPLICATION MASTER ADMIN ON *.* TO 'orchestrator'@'orc_host'; -- for SHOW SLAVE HOSTS / SHOW REPLICAS
```

`SLAVE MONITOR` is what `SHOW SLAVE STATUS` needs, `REPLICATION SLAVE ADMIN` lets `orchestrator` run the replication
commands (`STOP SLAVE`, `CHANGE MASTER TO`...) used by topology refactoring and recoveries, and
`REPLICATION MASTER ADMIN` is required to list the connected replicas. The privileges that work in the multi-tenant
end-to-end suite (`tests/multitenant/lib.sh`, `mariadb_users`, verified with `mariadb:11.4`) are `SLAVE MONITOR,
REPLICATION SLAVE ADMIN, REPLICATION MASTER ADMIN, READ_ONLY ADMIN, BINLOG ADMIN, BINLOG MONITOR`.
