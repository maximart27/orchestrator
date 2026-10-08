#!/usr/bin/env bash
# Generates the orchestrator configuration of every node of a profile under $ORCMT_WORK/conf/<node>/:
#   orchestrator.conf.json  global/node-local config (Raft, HTTP, control SQLite, TenantsConfigFile)
#   tenants.json            tenants manifest (docs/multi-tenant-config.md) -- identical on every node
#
#   gen-config.sh legacy-mysql   n1: single-tenant legacy, no tenants file, no Raft, tenant-a settings inline
#   gen-config.sh legacy-pg      n1: single-tenant legacy, ProviderType=postgresql, tenant-c settings inline.
#                                NO DiscoverySeeds: in legacy mode InjectSeed stores the stub with provider_type
#                                'mysql' and the instance is then polled as MySQL (port 5432 never reaches the PG
#                                provider). The legacy PG stage seeds through /api/discover instead.
#   gen-config.sh legacy-raft    n1: legacy + Raft of one node (checks the Raft wiring on today's binary)
#   gen-config.sh m1             n1: 3 tenants, Raft of one node
#   gen-config.sh m2             n1,n2,n3: 3 tenants, Raft of three nodes
#   gen-config.sh legacy-raft3   n1,n2,n3: legacy single tenant (mariadb-a inline, backend /data/mariadb-a.db) in a
#                                Raft of three nodes. Only used by `M2_PROFILE=legacy-raft3 M2_TENANTS=mariadb-a
#                                run-m2.sh` to self-test the mechanics of run-m2.sh (election, leader-only recovery,
#                                leader change, hooks) with one tenant on today's binary. Not a substitute for M2.
#   gen-config.sh m1-emulated    n1: THREE legacy single-tenant processes (ports 3000/3001/3002) writing the same
#                                /data/<tenant>.db files the real runtime will write. Only used by
#                                `ORCMT_M1_EMULATE=1 run-m1.sh` to self-test the assertions of run-m1.sh against
#                                real data before the multi-tenant runtime exists. Not a substitute for M1.
#
# The tenant-scoped settings are DELIBERATELY different between tenants (credentials, poll interval, filters,
# hooks, KV prefix, cluster alias, ...), so cross-tenant leaks show up.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

profile="${1:?usage: gen-config.sh <legacy-mysql|legacy-pg|legacy-raft|m1|m2|m1-emulated|legacy-raft3>}"
A1="$(ip_of a1)"; A2="$(ip_of a2)"; B1="$(ip_of b1)"; B2="$(ip_of b2)"; C1="$(ip_of c1)"; C2="$(ip_of c2)"

# ---- tenant-scoped fragments (the inside of a JSON object, no braces / trailing comma)
# $1 = topology password (tenants file uses ${ENV}, legacy uses the literal)
cfg_a() { cat <<JSON
"MySQLTopologyUser": "$TOPO_USER_A",
"MySQLTopologyPassword": "$1",
"InstancePollSeconds": 3,
"HostnameResolveMethod": "none",
"MySQLHostnameResolveMethod": "none",
"DiscoverySeeds": ["$A1:3306", "$A2:3306"],
"DetectClusterAliasQuery": "select 'alias-a'",
"DetectInstanceAliasQuery": "select concat('a-', @@server_id)",
"RecoverMasterClusterFilters": ["*"],
"RecoverIntermediateMasterClusterFilters": ["*"],
"RecoveryPeriodBlockSeconds": $RECOVERY_BLOCK_SECONDS,
"KVClusterMasterPrefix": "mariadb-a/master",
"PostMasterFailoverProcesses": ["echo \"tenant=$TENANT_A failed={failedHost} successor={successorHost}\" >> /data/hook-$TENANT_A.log"]
JSON
}
cfg_b() { cat <<JSON
"MySQLTopologyUser": "$TOPO_USER_B",
"MySQLTopologyPassword": "$TOPO_PW_B",
"InstancePollSeconds": 5,
"ReasonableReplicationLagSeconds": 60,
"AuditPurgeDays": 3,
"HostnameResolveMethod": "none",
"MySQLHostnameResolveMethod": "@@report_host",
"DiscoverySeeds": ["$B1:3306", "$B2:3306"],
"DetectClusterAliasQuery": "select 'alias-b'",
"DetectInstanceAliasQuery": "select concat('b-', @@server_id)",
"RecoverMasterClusterFilters": [".*"],
"RecoverIntermediateMasterClusterFilters": [".*"],
"RecoveryPeriodBlockSeconds": $RECOVERY_BLOCK_SECONDS,
"KVClusterMasterPrefix": "mariadb-b/master",
"PreFailoverProcesses": ["echo \"tenant=$TENANT_B pre failed={failedHost}\" >> /data/hook-$TENANT_B-pre.log"],
"PostMasterFailoverProcesses": ["echo \"tenant=$TENANT_B failed={failedHost} successor={successorHost}\" >> /data/hook-$TENANT_B.log"]
JSON
}
cfg_c() { cat <<JSON
"PostgreSQLTopologyUser": "$TOPO_USER_C",
"PostgreSQLTopologyPassword": "$TOPO_PW_C",
"PostgreSQLSSLMode": "disable",
"InstancePollSeconds": 4,
"HostnameResolveMethod": "none",
"DiscoverySeeds": ["$C1:5432", "$C2:5432"],
"RecoverMasterClusterFilters": [".*"],
"RecoverIntermediateMasterClusterFilters": [".*"],
"RecoveryPeriodBlockSeconds": $RECOVERY_BLOCK_SECONDS,
"KVClusterMasterPrefix": "postgres-c/master"
JSON
}

# ---- node-level pieces
# base_cfg NODE CONTROL_DB  -> backend + HTTP + logging
base_cfg() { cat <<JSON
"Debug": true,
"ListenAddress": ":$ORC_HTTP_PORT",
"HTTPAdvertise": "http://$(ip_of "$1"):$ORC_HTTP_PORT",
"BackendDB": "sqlite",
"SQLite3DataFile": "$2"
JSON
}
# raft_cfg NODE [NODE...]   (no nodes after NODE = Raft of one: empty RaftNodes, per docs/configuration-raft.md)
raft_cfg() {
  local self="$1"; shift
  local nodes="" n
  for n in "$@"; do nodes="$nodes${nodes:+, }\"$(ip_of "$n")\""; done
  cat <<JSON
"RaftEnabled": true,
"RaftDataDir": "/data/raft",
"RaftBind": "$(ip_of "$self")",
"RaftAdvertise": "$(ip_of "$self")",
"DefaultRaftPort": $ORC_RAFT_PORT,
"RaftNodes": [$nodes]
JSON
}
tenants_json() { cat <<JSON
{
  "Tenants": [
    {
      "ID": "$TENANT_A", "Provider": "mysql",
      "Backend": { "BackendDB": "sqlite", "SQLite3DataFile": "/data/$TENANT_A.db" },
      "Config": { $(cfg_a '${ORC_A_TOPOLOGY_PASSWORD}') }
    },
    {
      "ID": "$TENANT_B", "Provider": "mariadb",
      "Backend": { "BackendDB": "sqlite", "SQLite3DataFile": "/data/$TENANT_B.db" },
      "Config": { $(cfg_b) }
    },
    {
      "ID": "$TENANT_C", "Provider": "postgresql",
      "Backend": { "BackendDB": "sqlite", "SQLite3DataFile": "/data/$TENANT_C.db" },
      "Config": { $(cfg_c) }
    }
  ]
}
JSON
}

emit() {  # emit NODE  (reads the node's orchestrator.conf.json body from stdin, tenants.json optional via $2)
  local n="$1" d="$ORCMT_WORK/conf/$1"; mkdir -p "$d"
  { echo "{"; cat; echo "}"; } > "$d/orchestrator.conf.json"
  if [ "${2:-}" = tenants ]; then tenants_json > "$d/tenants.json"; fi
}

rm -rf "$ORCMT_WORK/conf"
case "$profile" in
  legacy-mysql) { base_cfg n1 /data/legacy.db; echo ,; cfg_a "$TOPO_PW_A"; } | emit n1;;
  legacy-pg)    { base_cfg n1 /data/legacy.db; echo ,; echo '"ProviderType": "postgresql",'; cfg_c | grep -v DiscoverySeeds; } | emit n1;;
  legacy-raft)  { base_cfg n1 /data/legacy.db; echo ,; raft_cfg n1; echo ,; cfg_a "$TOPO_PW_A"; } | emit n1;;
  m1) { base_cfg n1 /data/control.db; echo ,; raft_cfg n1; echo ,; echo '"TenantsConfigFile": "/conf/tenants.json"'; } | emit n1 tenants;;
  m1-emulated)
    { base_cfg n1 "/data/$TENANT_A.db"; echo ,; cfg_a "$TOPO_PW_A"; } | emit n1; mv "$ORCMT_WORK/conf/n1/orchestrator.conf.json" "$ORCMT_WORK/conf/n1/emu-a.json"
    { base_cfg n1 "/data/$TENANT_B.db" | sed "s/:$ORC_HTTP_PORT/:$((ORC_HTTP_PORT+1))/"; echo ,; cfg_b; } | emit n1; mv "$ORCMT_WORK/conf/n1/orchestrator.conf.json" "$ORCMT_WORK/conf/n1/emu-b.json"
    { base_cfg n1 "/data/$TENANT_C.db" | sed "s/:$ORC_HTTP_PORT/:$((ORC_HTTP_PORT+2))/"; echo ,; echo '"ProviderType": "postgresql",'; cfg_c | grep -v DiscoverySeeds; } | emit n1; mv "$ORCMT_WORK/conf/n1/orchestrator.conf.json" "$ORCMT_WORK/conf/n1/emu-c.json";;
  legacy-raft3) for n in n1 n2 n3; do
        { base_cfg "$n" "/data/$TENANT_A.db"; echo ,; raft_cfg "$n" n1 n2 n3; echo ,; cfg_a "$TOPO_PW_A"; } | emit "$n"
      done;;
  m2) for n in n1 n2 n3; do
        { base_cfg "$n" /data/control.db; echo ,; raft_cfg "$n" n1 n2 n3; echo ,; echo '"TenantsConfigFile": "/conf/tenants.json"'; } | emit "$n" tenants
      done;;
  *) die "unknown profile '$profile'";;
esac
log "generated profile '$profile' in $ORCMT_WORK/conf: $(ls "$ORCMT_WORK/conf" | tr '\n' ' ')"
