#!/usr/bin/env bash
# Shared helpers for the multi-tenant end-to-end tests (tests/multitenant).
#
# Everything this suite creates is named "orcmt-*" (containers, network, volumes). Nothing else on the Docker
# host is ever touched: cleanup only removes resources whose name starts with "orcmt-".
#
# Source it:   source "$(dirname "$0")/lib.sh"
# Every knob below can be overridden from the environment.

ORCMT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORCMT_REPO="$(cd "$ORCMT_DIR/../.." && pwd)"

# ---------------------------------------------------------------- parameters
ORCMT_PREFIX="${ORCMT_PREFIX:-orcmt}"
ORCMT_NET="${ORCMT_NET:-${ORCMT_PREFIX}-net}"
ORCMT_SUBNET="${ORCMT_SUBNET:-172.31.77.0/24}"
ORCMT_WORK="${ORCMT_WORK:-/tmp/${ORCMT_PREFIX}-work}"   # generated configs + the compiled binary
ORCMT_KEEP="${ORCMT_KEEP:-0}"                            # 1 = do not clean up on exit (debugging)

# Images (all must already be present locally; nothing is ever pulled)
IMG_MARIADB="${IMG_MARIADB:-mariadb:11.4}"
IMG_POSTGRES="${IMG_POSTGRES:-postgres:16-alpine}"
IMG_GO="${IMG_GO:-golang:1.25}"      # builds the binary AND runs it (has curl, python3+sqlite3, bash, gcc)

# Memory limits (the dev PC is small)
MEM_MARIADB="${MEM_MARIADB:-384m}"
MEM_POSTGRES="${MEM_POSTGRES:-256m}"
MEM_NODE="${MEM_NODE:-512m}"

# Credentials. Root/admin ones are only used by this suite to bootstrap the topologies.
MARIADB_ROOT_PW="${MARIADB_ROOT_PW:-orcmt-root}"
PG_ROOT_PW="${PG_ROOT_PW:-orcmt-root}"
REPL_USER="repl";   REPL_PW="orcmt-repl"
# Per-tenant topology credentials (tenant-scoped config, deliberately different per tenant)
TOPO_USER_A="orcmt_a"; TOPO_PW_A="orcmt-pw-a"
TOPO_USER_B="orcmt_b"; TOPO_PW_B="orcmt-pw-b"
TOPO_USER_C="orcmt_c"; TOPO_PW_C="orcmt-pw-c"

# Tenant ids
TENANT_A="mariadb-a"; TENANT_B="mariadb-b"; TENANT_C="postgres-c"

# Orchestrator
ORC_HTTP_PORT="${ORC_HTTP_PORT:-3000}"
ORC_RAFT_PORT="${ORC_RAFT_PORT:-10008}"

# Timing (seconds). Everything polls; these are only upper bounds.
T_DB_READY="${T_DB_READY:-120}"
T_REPL_READY="${T_REPL_READY:-60}"
T_NODE_READY="${T_NODE_READY:-60}"
T_DISCOVERY="${T_DISCOVERY:-90}"          # M1: how long to wait for all tenants to discover their topology
T_RAFT_ELECTION="${T_RAFT_ELECTION:-60}"
T_FAILOVER="${T_FAILOVER:-120}"           # M2: how long to wait for a recovery to be registered
T_SETTLE="${T_SETTLE:-15}"                # M2: how long followers are watched to confirm they do nothing
# RecoveryPeriodBlockSeconds of every tenant. It must be LONGER than a whole failover (40-60s, more with 3 tenants failing
# at once): analysis keeps reporting the dead master until the promotion ends, and once the block expires the same
# recovery is attempted again (seen: 4 recovery rows + 4 hook runs for ONE failover with a 5s block).
RECOVERY_BLOCK_SECONDS="${RECOVERY_BLOCK_SECONDS:-120}"

export ORCMT_PREFIX ORCMT_NET ORCMT_SUBNET ORCMT_WORK ORCMT_KEEP IMG_MARIADB IMG_POSTGRES IMG_GO MEM_MARIADB MEM_POSTGRES MEM_NODE \
  MARIADB_ROOT_PW PG_ROOT_PW ORC_HTTP_PORT ORC_RAFT_PORT T_DB_READY T_REPL_READY T_NODE_READY T_DISCOVERY T_RAFT_ELECTION T_FAILOVER T_SETTLE RECOVERY_BLOCK_SECONDS

# ------------------------------------------------------------ fixed addresses
# Everything has a fixed IP on orcmt-net: MariaDB report_host / PostgreSQL client_addr / Raft all use IPs, so
# the instance keys orchestrator derives are stable and need no DNS.
ORCMT_PFX="${ORCMT_SUBNET%.*}"            # 172.31.77
ip_of() {
  case "$1" in
    a1) echo "$ORCMT_PFX.11";;  a2) echo "$ORCMT_PFX.12";;
    b1) echo "$ORCMT_PFX.21";;  b2) echo "$ORCMT_PFX.22";;
    c1) echo "$ORCMT_PFX.31";;  c2) echo "$ORCMT_PFX.32";;
    n1) echo "$ORCMT_PFX.101";; n2) echo "$ORCMT_PFX.102";; n3) echo "$ORCMT_PFX.103";;
    *) echo "ip_of: unknown $1" >&2; return 1;;
  esac
}
cname() { echo "${ORCMT_PREFIX}-$1"; }     # a1 -> orcmt-a1

# ------------------------------------------------------------------- logging
_ts() { date +%H:%M:%S; }
log()  { echo "[$(_ts)] $*" >&2; }
die()  { echo "[$(_ts)] FATAL: $*" >&2; exit 1; }
PASS_COUNT=0; FAIL_COUNT=0; SKIP_COUNT=0
pass() { echo "  PASS: $1" >&2; PASS_COUNT=$((PASS_COUNT+1)); }
fail() { echo "  FAIL: $1" >&2; [ -n "${2:-}" ] && echo "        $2" >&2; FAIL_COUNT=$((FAIL_COUNT+1)); }
skip() { echo "  SKIP: $1" >&2; SKIP_COUNT=$((SKIP_COUNT+1)); }
summary() {
  echo "" >&2
  echo "=== RESULTS: $PASS_COUNT passed, $FAIL_COUNT failed, $SKIP_COUNT skipped ===" >&2
  [ "$FAIL_COUNT" -eq 0 ]
}
# assert_eq NAME EXPECTED ACTUAL
assert_eq() { if [ "$2" = "$3" ]; then pass "$1 ($3)"; else fail "$1" "expected '$2', got '$3'"; fi; }

# wait_until TIMEOUT DESCRIPTION cmd args...   (polls every second; returns 0 as soon as cmd succeeds)
wait_until() {
  local timeout="$1" desc="$2" i=0; shift 2
  while ! "$@" >/dev/null 2>&1; do
    i=$((i+1))
    if [ "$i" -ge "$timeout" ]; then log "timeout (${timeout}s) waiting for: $desc"; return 1; fi
    sleep 1
  done
  log "ready: $desc (${i}s)"
}

# ------------------------------------------------------------------- cleanup
# Removes ONLY resources named orcmt-* (never anything else on the host).
orcmt_cleanup() {
  local ids vols
  ids="$(docker ps -aq --filter "name=^${ORCMT_PREFIX}-" 2>/dev/null || true)"
  if [ -n "$ids" ]; then
    # double-check every name before removing
    for id in $ids; do
      case "$(docker inspect -f '{{.Name}}' "$id" 2>/dev/null)" in
        "/${ORCMT_PREFIX}-"*) docker rm -f -v "$id" >/dev/null 2>&1 || true;;
      esac
    done
  fi
  vols="$(docker volume ls -q --filter "name=^${ORCMT_PREFIX}-" 2>/dev/null || true)"
  for v in $vols; do
    case "$v" in "${ORCMT_PREFIX}-"*) docker volume rm -f "$v" >/dev/null 2>&1 || true;; esac
  done
  docker network rm "$ORCMT_NET" >/dev/null 2>&1 || true
  # generated configs only (the compiled binary / Go cache in $ORCMT_WORK are kept); never outside an orcmt* dir
  case "$(basename "$ORCMT_WORK")" in *orcmt*) rm -rf "$ORCMT_WORK/conf" ;; esac
}
# Keeps the logs of every orcmt-* container in $ORCMT_WORK/logs/ (overwritten each run) before they are removed.
orcmt_save_logs() {
  local c; mkdir -p "$ORCMT_WORK/logs"
  for c in $(docker ps -a --format '{{.Names}}' --filter "name=^${ORCMT_PREFIX}-" 2>/dev/null); do
    docker logs "$c" > "$ORCMT_WORK/logs/$c.log" 2>&1 || true
  done
}
orcmt_cleanup_on_exit() {
  local rc=$?
  orcmt_save_logs
  if [ "$ORCMT_KEEP" = "1" ]; then log "ORCMT_KEEP=1: leaving orcmt-* resources running (run $ORCMT_DIR/cleanup.sh)"; return $rc; fi
  orcmt_cleanup
  return $rc
}
# Call from a script that owns the environment:  orcmt_trap_cleanup
orcmt_trap_cleanup() { trap orcmt_cleanup_on_exit EXIT; trap 'exit 130' INT TERM; }

net_up() {
  docker network inspect "$ORCMT_NET" >/dev/null 2>&1 && return 0
  docker network create --subnet "$ORCMT_SUBNET" "$ORCMT_NET" >/dev/null || die "cannot create network $ORCMT_NET ($ORCMT_SUBNET)"
}

# ------------------------------------------------------------------- build
# Compiles the orchestrator binary of this worktree (offline, in Docker) into $ORCMT_WORK/bin.
build_orchestrator() {
  mkdir -p "$ORCMT_WORK/bin" "$ORCMT_WORK/gocache"
  log "building orchestrator + validate-tenants (offline, $IMG_GO)"
  docker run --rm --network none --user "$(id -u):$(id -g)" \
    -e GOCACHE=/gocache -e GOFLAGS=-mod=vendor -e HOME=/tmp \
    -v "$ORCMT_REPO":/src -v "$ORCMT_WORK/gocache":/gocache -v "$ORCMT_WORK/bin":/out -w /src \
    "$IMG_GO" sh -c 'go build -o /out/orchestrator ./go/cmd/orchestrator && go build -o /out/validate-tenants ./tests/multitenant/tools/validate-tenants' \
    || die "build failed"
}

# ------------------------------------------------------------------- MariaDB
# mariadb_start NAME(a1) SERVER_ID GTID_DOMAIN [ro]
mariadb_start() {
  local n="$1" sid="$2" dom="$3" ro="${4:-}" ip; ip="$(ip_of "$n")"
  local extra=()
  [ "$ro" = "ro" ] && extra+=(--read-only=ON)
  docker run -d --name "$(cname "$n")" --network "$ORCMT_NET" --ip "$ip" --memory "$MEM_MARIADB" \
    -v "$(cname "$n")-data:/var/lib/mysql" -e MARIADB_ROOT_PASSWORD="$MARIADB_ROOT_PW" \
    "$IMG_MARIADB" \
    --server-id="$sid" --gtid-domain-id="$dom" --log-bin=mysql-bin --binlog-format=ROW \
    --log-slave-updates=ON --report-host="$ip" --skip-name-resolve \
    --innodb-buffer-pool-size=64M --innodb-log-file-size=32M --performance-schema=OFF --max-connections=100 \
    "${extra[@]}" >/dev/null || die "cannot start $(cname "$n")"
}
# mdb NAME SQL   -- runs SQL as root over the unix socket
mdb() { docker exec "$(cname "$1")" mariadb -uroot -p"$MARIADB_ROOT_PW" -N -B -e "$2"; }
# TCP check on purpose: the temporary init server only listens on the socket, so this fails until the real one is up.
mariadb_ready() { docker exec "$(cname "$1")" mariadb --protocol=tcp -h127.0.0.1 --skip-ssl -uroot -p"$MARIADB_ROOT_PW" -N -B -e 'select 1' 2>/dev/null | grep -qx 1; }
# mariadb_users NAME TOPO_USER TOPO_PW   (not binlogged: created on every node individually, no errant GTIDs)
# Grants = docs/configuration-discovery-basic.md (SUPER, PROCESS, REPLICATION SLAVE, REPLICATION CLIENT, RELOAD,
# SELECT on meta.*) PLUS the privileges MariaDB >= 10.5.9/11.x split out of SUPER: without SLAVE MONITOR
# `SHOW SLAVE STATUS` fails with Error 1227 and orchestrator cannot discover anything (finding, see README).
mariadb_users() {
  mdb "$1" "SET sql_log_bin=0;
    CREATE USER IF NOT EXISTS '$2'@'%' IDENTIFIED BY '$3';
    GRANT SUPER, PROCESS, REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO '$2'@'%';
    GRANT SLAVE MONITOR, REPLICATION SLAVE ADMIN, REPLICATION MASTER ADMIN, READ_ONLY ADMIN, BINLOG ADMIN, BINLOG MONITOR ON *.* TO '$2'@'%';
    CREATE DATABASE IF NOT EXISTS meta;
    GRANT SELECT ON meta.* TO '$2'@'%';
    CREATE USER IF NOT EXISTS '$REPL_USER'@'%' IDENTIFIED BY '$REPL_PW';
    GRANT REPLICATION SLAVE ON *.* TO '$REPL_USER'@'%';
    FLUSH PRIVILEGES;" >/dev/null
}
# mariadb_replicate REPLICA MASTER   (GTID, slave_pos)
mariadb_replicate() {
  mdb "$1" "STOP SLAVE; RESET SLAVE ALL;
    CHANGE MASTER TO MASTER_HOST='$(ip_of "$2")', MASTER_PORT=3306, MASTER_USER='$REPL_USER', MASTER_PASSWORD='$REPL_PW',
      MASTER_USE_GTID=slave_pos, MASTER_CONNECT_RETRY=1;
    START SLAVE;" >/dev/null
}
mariadb_repl_running() {   # $1 replica; succeeds when IO+SQL threads run
  local s; s="$(docker exec "$(cname "$1")" mariadb -uroot -p"$MARIADB_ROOT_PW" -e 'SHOW SLAVE STATUS\G' 2>/dev/null)"
  echo "$s" | grep -q 'Slave_IO_Running: Yes' && echo "$s" | grep -q 'Slave_SQL_Running: Yes'
}
# mariadb_probe_replication MASTER REPLICA TAG   -- writes on master, reads it back on the replica
mariadb_probe_replication() {
  local m="$1" r="$2" tag="$3"
  mdb "$m" "CREATE DATABASE IF NOT EXISTS orcmt_probe; CREATE TABLE IF NOT EXISTS orcmt_probe.t (k varchar(64) primary key, v varchar(64)); REPLACE INTO orcmt_probe.t VALUES ('$tag','ok');" >/dev/null || return 1
  wait_until 30 "probe '$tag' replicated $m -> $r" bash -c "[ \"\$(docker exec $(cname "$r") mariadb -uroot -p$MARIADB_ROOT_PW -N -B -e \"select v from orcmt_probe.t where k='$tag'\" 2>/dev/null)\" = ok ]"
}

# mariadb_pair_up MASTER REPLICA SID_M SID_R DOMAIN TOPO_USER TOPO_PW
mariadb_pair_up() {
  local m="$1" r="$2" sm="$3" sr="$4" dom="$5" user="$6" pw="$7"
  mariadb_start "$m" "$sm" "$dom"
  mariadb_start "$r" "$sr" "$dom" ro
  wait_until "$T_DB_READY" "$(cname "$m") accepting TCP" mariadb_ready "$m" || die "$(cname "$m") not ready"
  wait_until "$T_DB_READY" "$(cname "$r") accepting TCP" mariadb_ready "$r" || die "$(cname "$r") not ready"
  mariadb_users "$m" "$user" "$pw"; mariadb_users "$r" "$user" "$pw"
  mariadb_replicate "$r" "$m"
  wait_until "$T_REPL_READY" "$(cname "$r") IO+SQL threads running" mariadb_repl_running "$r" || die "replication $r<-$m failed"
  mariadb_probe_replication "$m" "$r" "probe-$m-$r" || die "replication probe $m -> $r failed"
}

# ------------------------------------------------------------------- PostgreSQL
PG_FLAGS=(-c wal_level=replica -c max_wal_senders=10 -c max_replication_slots=10 -c wal_keep_size=64MB \
          -c hot_standby=on -c wal_log_hints=on -c "listen_addresses=*" -c shared_buffers=32MB -c max_connections=50)
psql_in() { docker exec "$(cname "$1")" psql -U postgres -v ON_ERROR_STOP=1 -At -c "$2"; }
pg_ready() { docker exec "$(cname "$1")" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && psql_in "$1" 'select 1' >/dev/null 2>&1; }

pg_primary_start() {
  local n="$1" ip; ip="$(ip_of "$n")"
  docker run -d --name "$(cname "$n")" --network "$ORCMT_NET" --ip "$ip" --memory "$MEM_POSTGRES" \
    -v "$(cname "$n")-data:/var/lib/postgresql/data" -e POSTGRES_PASSWORD="$PG_ROOT_PW" \
    "$IMG_POSTGRES" postgres "${PG_FLAGS[@]}" >/dev/null || die "cannot start $(cname "$n")"
}
# pg_primary_setup NAME TOPO_USER TOPO_PW
# Orchestrator's PG provider (go/inst/provider_postgresql.go) connects to database "postgres" as the topology
# user, reads pg_stat_replication / pg_stat_wal_receiver (pg_monitor), and -- for recoveries -- promotes
# (pg_promote), runs ALTER SYSTEM and uses the SAME user as replication user in primary_conninfo. Hence:
# pg_monitor + REPLICATION (+ SUPERUSER, like tests/functional/postgres/init-primary.sh) and a pg_hba
# 'replication' line (the stock "host all all all" rule does not match replication connections).
pg_primary_setup() {
  local n="$1" user="$2" pw="$3"
  psql_in "$n" "CREATE ROLE $REPL_USER WITH REPLICATION LOGIN PASSWORD '$REPL_PW'" >/dev/null
  psql_in "$n" "CREATE ROLE $user WITH LOGIN SUPERUSER REPLICATION PASSWORD '$pw'" >/dev/null
  psql_in "$n" "GRANT pg_monitor TO $user" >/dev/null
  docker exec "$(cname "$n")" sh -c 'printf "host replication all all scram-sha-256\n" >> "$PGDATA/pg_hba.conf"'
  psql_in "$n" 'select pg_reload_conf()' >/dev/null
}
# pg_standby_start NAME PRIMARY   -- pg_basebackup in the entrypoint, then runs as a streaming standby
pg_standby_start() {
  local n="$1" p="$2" ip pip; ip="$(ip_of "$n")"; pip="$(ip_of "$p")"
  docker run -d --name "$(cname "$n")" --network "$ORCMT_NET" --ip "$ip" --memory "$MEM_POSTGRES" --user postgres \
    -v "$(cname "$n")-data:/var/lib/postgresql/data" -e PGPASSWORD="$REPL_PW" \
    --entrypoint sh "$IMG_POSTGRES" -c "
      set -e
      rm -rf \"\$PGDATA\"/* \"\$PGDATA\"/.[!.]* 2>/dev/null || true
      until pg_isready -h $pip -p 5432 >/dev/null 2>&1; do sleep 1; done
      pg_basebackup -h $pip -p 5432 -U $REPL_USER -D \"\$PGDATA\" -X stream
      printf \"primary_conninfo = 'host=$pip port=5432 user=$REPL_USER password=$REPL_PW'\n\" > \"\$PGDATA/postgresql.auto.conf\"
      touch \"\$PGDATA/standby.signal\"
      chmod 700 \"\$PGDATA\"
      exec postgres ${PG_FLAGS[*]}" >/dev/null || die "cannot start $(cname "$n")"
}
pg_standby_streaming() {   # $1 primary: one standby is streaming
  [ "$(psql_in "$1" "select count(*) from pg_stat_replication where state='streaming'" 2>/dev/null)" -ge 1 ]
}
pg_probe_replication() {
  local m="$1" r="$2" tag="$3"
  psql_in "$m" "CREATE TABLE IF NOT EXISTS orcmt_probe (k text primary key, v text); INSERT INTO orcmt_probe VALUES ('$tag','ok') ON CONFLICT (k) DO UPDATE SET v='ok'" >/dev/null || return 1
  wait_until 30 "probe '$tag' replicated $m -> $r" bash -c "[ \"\$(docker exec $(cname "$r") psql -U postgres -At -c \"select v from orcmt_probe where k='$tag'\" 2>/dev/null)\" = ok ]"
}
pg_pair_up() {
  local m="$1" r="$2" user="$3" pw="$4"
  pg_primary_start "$m"
  wait_until "$T_DB_READY" "$(cname "$m") accepting TCP" pg_ready "$m" || die "$(cname "$m") not ready"
  pg_primary_setup "$m" "$user" "$pw"
  pg_standby_start "$r" "$m"
  wait_until "$T_DB_READY" "$(cname "$r") ready (standby)" bash -c "docker exec $(cname "$r") psql -U postgres -At -c 'select pg_is_in_recovery()' 2>/dev/null | grep -qx t" || die "$(cname "$r") not ready"
  wait_until "$T_REPL_READY" "$(cname "$m") has a streaming standby" pg_standby_streaming "$m" || die "PG replication $m -> $r failed"
  pg_probe_replication "$m" "$r" "probe-$m-$r" || die "PG replication probe failed"
}

# ------------------------------------------------------------------- topologies
topology_a_up() { net_up; mariadb_pair_up a1 a2 1001 1002 11 "$TOPO_USER_A" "$TOPO_PW_A"; }   # tenant mariadb-a
topology_b_up() { net_up; mariadb_pair_up b1 b2 2001 2002 22 "$TOPO_USER_B" "$TOPO_PW_B"; }   # tenant mariadb-b (other server_id range / gtid domain)
topology_c_up() { net_up; pg_pair_up c1 c2 "$TOPO_USER_C" "$TOPO_PW_C"; }                      # tenant postgres-c

# ------------------------------------------------------------------- orchestrator nodes
# node_start NODE   (n1..n3). Needs $ORCMT_WORK/conf/NODE/ (gen-config.sh) and $ORCMT_WORK/bin/orchestrator (build).
node_start() {
  local n="$1" c; c="$(cname "$n")"
  docker rm -f "$c" >/dev/null 2>&1 || true
  docker run -d --name "$c" --network "$ORCMT_NET" --ip "$(ip_of "$n")" --memory "$MEM_NODE" --stop-timeout 10 --hostname "$c" \
    -v "$c-data:/data" -v "$ORCMT_WORK/conf/$n":/conf:ro \
    -v "$ORCMT_WORK/bin/orchestrator":/usr/local/bin/orchestrator:ro -v "$ORCMT_REPO/resources":/orchestrator/resources:ro \
    -e ORC_A_TOPOLOGY_PASSWORD="$TOPO_PW_A" -w /orchestrator \
    "$IMG_GO" orchestrator -config /conf/orchestrator.conf.json http >/dev/null || die "cannot start $c"
}
# node_start_emulated -- n1 runs three legacy single-tenant processes (profile m1-emulated); self-test of run-m1.sh only
node_start_emulated() {
  local c; c="$(cname n1)"
  docker rm -f "$c" >/dev/null 2>&1 || true
  docker run -d --name "$c" --network "$ORCMT_NET" --ip "$(ip_of n1)" --memory "$MEM_NODE" --stop-timeout 10 --hostname "$c" \
    -v "$c-data:/data" -v "$ORCMT_WORK/conf/n1":/conf:ro \
    -v "$ORCMT_WORK/bin/orchestrator":/usr/local/bin/orchestrator:ro -v "$ORCMT_REPO/resources":/orchestrator/resources:ro \
    -e ORC_A_TOPOLOGY_PASSWORD="$TOPO_PW_A" -w /orchestrator \
    "$IMG_GO" sh -c 'for t in a b c; do orchestrator -config /conf/emu-$t.json http & done; wait' >/dev/null || die "cannot start $c"
}
node_running() { [ "$(docker inspect -f '{{.State.Running}}' "$(cname "$1")" 2>/dev/null)" = true ]; }
node_logs()    { docker logs "$(cname "$1")" 2>&1; }

# api NODE PATH [curl args...]   -- HTTP GET from inside the node container (no published ports)
api() { local n="$1" p="$2"; shift 2; docker exec "$(cname "$n")" curl -s --max-time 10 "$@" "http://127.0.0.1:$ORC_HTTP_PORT$p"; }
# api_http_code NODE PATH
api_http_code() { docker exec "$(cname "$1")" curl -s -o /dev/null -w '%{http_code}' --max-time 10 "http://127.0.0.1:$ORC_HTTP_PORT$2"; }
node_http_ready() { [ "$(api_http_code "$1" /api/clusters 2>/dev/null)" = 200 ] || [ "$(api_http_code "$1" /api/raft-state 2>/dev/null)" = 200 ] || [ "$(api_http_code "$1" /api/health 2>/dev/null)" = 200 ]; }

# json_len / json_py: tiny helpers on top of python3 (host has it; used only for parsing API output)
json_len() { python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo 0; }

# sql NODE DBFILE SQL   -- read-only query of a SQLite backend of a node (works while the node is stopped, too).
# Output: rows separated by newlines, columns by '|'.
_SQ_PY='import sqlite3,sys
c=sqlite3.connect(sys.argv[1],timeout=15)
for r in c.execute(sys.argv[2]): print("|".join("" if v is None else str(v) for v in r))'
sql() {
  local n="$1" f="$2" q="$3"
  if node_running "$n"; then
    docker exec "$(cname "$n")" python3 -c "$_SQ_PY" "$f" "$q"
  else
    docker run --rm -v "$(cname "$n")-data:/data" "$IMG_GO" python3 -c "$_SQ_PY" "$f" "$q"
  fi
}
# db file of a tenant on a node (see gen-config.sh): /data/<tenant>.db ; control db: /data/control.db
tenant_db() { echo "/data/$1.db"; }

# ------------------------------------------------------------------- assertions on a discovered topology
# topology_rows NODE DBFILE  -> "host|port|provider_type|master_host|cluster_name" per database_instance row
topology_rows() { sql "$1" "$2" "select hostname, port, provider_type, master_host, cluster_name from database_instance order by hostname"; }

# topology_matches NODE DBFILE PROVIDER MASTER_IP REPLICA_IP PORT
# Succeeds only when database_instance holds EXACTLY the master and the replica (nothing else, so nothing from
# another tenant), with the right provider_type, the replica pointing at the master and one shared cluster name.
topology_matches() {
  local n="$1" f="$2" prov="$3" m="$4" r="$5" port="$6" rows cm cr
  rows="$(topology_rows "$n" "$f" 2>/dev/null)" || return 1
  [ "$(printf '%s\n' "$rows" | grep -c .)" = 2 ] || return 1
  cm="$(printf '%s\n' "$rows" | awk -F'|' -v h="$m" -v p="$port" -v t="$prov" '$1==h && $2==p && $3==t && $4=="" {print $5}')"
  cr="$(printf '%s\n' "$rows" | awk -F'|' -v h="$r" -v p="$port" -v t="$prov" -v mh="$m" '$1==h && $2==p && $3==t && $4==mh {print $5}')"
  [ -n "$cm" ] && [ "$cm" = "$cr" ]
}
# cluster_name of the (single) cluster in a backend
topology_cluster() { sql "$1" "$2" "select distinct cluster_name from database_instance" | head -1; }

# assert_topology LABEL NODE DBFILE PROVIDER MASTER_IP REPLICA_IP PORT  -- waits up to $T_DISCOVERY, then asserts
assert_topology() {
  local label="$1"; shift
  if wait_until "$T_DISCOVERY" "$label discovered master+replica" topology_matches "$@"; then
    pass "$label: backend holds exactly master+replica ($(topology_cluster "$1" "$2"))"
  else
    fail "$label: topology not discovered as expected" "$(topology_rows "$1" "$2" 2>&1 | tr '\n' ';')"
  fi
}
# recovery_rows NODE DBFILE -> hostname|successor|analysis of every topology_recovery row
recovery_rows() { sql "$1" "$2" "select hostname, ifnull(successor_hostname,''), ifnull(analysis,'') from topology_recovery order by recovery_id"; }
recovery_count() { sql "$1" "$2" "select count(*) from topology_recovery" 2>/dev/null || echo 0; }

# ------------------------------------------------------------------- tenant catalogue
TENANTS=("$TENANT_A" "$TENANT_B" "$TENANT_C")
tenant_prov()    { case "$1" in "$TENANT_C") echo postgresql;; *) echo mysql;; esac; }
tenant_master()  { case "$1" in "$TENANT_A") ip_of a1;; "$TENANT_B") ip_of b1;; "$TENANT_C") ip_of c1;; esac; }
tenant_replica() { case "$1" in "$TENANT_A") ip_of a2;; "$TENANT_B") ip_of b2;; "$TENANT_C") ip_of c2;; esac; }
tenant_port()    { case "$1" in "$TENANT_C") echo 5432;; *) echo 3306;; esac; }
tenant_alias()   { case "$1" in "$TENANT_A") echo alias-a;; "$TENANT_B") echo alias-b;; *) echo "";; esac; }
tenant_hook()    { echo "/data/hook-$1.log"; }          # written by PostMasterFailoverProcesses (a, b only), node-local
tenant_poll()    { case "$1" in "$TENANT_A") echo 3;; "$TENANT_B") echo 5;; "$TENANT_C") echo 4;; esac; }
# "other" instance IPs of a tenant, as a quoted SQL list ('ip','ip',...)
others_sql() {
  local t="$1" o out=""
  for o in "${TENANTS[@]}"; do [ "$o" = "$t" ] && continue
    out="$out${out:+,}'$(tenant_master "$o")','$(tenant_replica "$o")'"
  done
  echo "$out"
}

# ------------------------------------------------------------------- raft helpers (tenant-independent)
# raft_state NODE -> Leader / Follower / Candidate ...   (via /api/raft-state, answered locally by the node)
raft_state() { api "$1" /api/raft-state 2>/dev/null | tr -d '"'; }
# raft_leader_node NODE... -> the node (n1..n3) that reports itself Leader; fails unless exactly one does
raft_leader_node() {
  local n leaders=() ; for n in "$@"; do node_running "$n" && [ "$(raft_state "$n")" = Leader ] && leaders+=("$n"); done
  [ ${#leaders[@]} -eq 1 ] && echo "${leaders[0]}"
}
# node_of_ip IP -> n1/n2/n3
node_of_ip() { for n in n1 n2 n3; do [ "$(ip_of $n)" = "$1" ] && { echo $n; return; }; done; }
# recovery rows with the node that processed them: hostname|successor|analysis|processing_node
recovery_detail() { sql "$1" "$2" "select hostname, ifnull(successor_hostname,''), ifnull(analysis,''), processing_node_hostname from topology_recovery order by recovery_id"; }

# hook_lines NODE TENANT -> number of lines of the tenant's failover hook log on that node's /data (0 when absent).
# The hook (PostMasterFailoverProcesses in the tenant's Config) only runs on the node that executes the recovery,
# so the file is node-local proof of WHICH node ran WHICH tenant's recovery. Works for stopped nodes too.
hook_lines() {
  local n="$1" f; f="$(tenant_hook "$2")"
  if node_running "$n"; then
    docker exec "$(cname "$n")" sh -c "[ -f $f ] && wc -l < $f || echo 0"
  else
    docker run --rm -v "$(cname "$n")-data:/data" "$IMG_GO" sh -c "[ -f $f ] && wc -l < $f || echo 0"
  fi | tr -d ' \r'
}
hook_last() { docker exec "$(cname "$1")" sh -c "tail -1 $(tenant_hook "$2") 2>/dev/null"; }
# log_count NODE REGEX -> number of matching lines in the node's log
log_count() { node_logs "$1" | grep -c -E "$2"; }
# mariadb_promoted_ok NAME   -> read_only=0
mariadb_writable() { [ "$(docker exec "$(cname "$1")" mariadb -uroot -p"$MARIADB_ROOT_PW" -N -B -e 'select @@read_only' 2>/dev/null)" = 0 ]; }
pg_promoted() { [ "$(psql_in "$1" 'select pg_is_in_recovery()' 2>/dev/null)" = f ]; }
# mariadb_rejoin OLD NEW  -- restarts a stopped ex-master and makes it a (read-only) GTID replica of NEW
mariadb_rejoin() {
  local old="$1" new="$2"
  docker start "$(cname "$old")" >/dev/null || return 1
  wait_until "$T_DB_READY" "$(cname "$old") accepting TCP" mariadb_ready "$old" || return 1
  mdb "$old" "SET GLOBAL read_only=1; STOP SLAVE; RESET SLAVE ALL;
    CHANGE MASTER TO MASTER_HOST='$(ip_of "$new")', MASTER_PORT=3306, MASTER_USER='$REPL_USER', MASTER_PASSWORD='$REPL_PW',
      MASTER_USE_GTID=current_pos, MASTER_CONNECT_RETRY=1;
    START SLAVE;" >/dev/null || return 1
  wait_until "$T_REPL_READY" "$(cname "$old") replicating from $(cname "$new")" mariadb_repl_running "$old"
}
# reintegrated NODE TENANT NEWMASTER_IP OLDMASTER_IP -- the node's backend sees NEW as master and OLD replicating from it
reintegrated() {
  local n="$1" db; db="$(tenant_db "$2")"
  [ "$(sql "$n" "$db" "select count(*) from database_instance where hostname='$3' and master_host='' and last_seen > datetime('now','-30 seconds')" 2>/dev/null)" = 1 ] &&
  [ "$(sql "$n" "$db" "select count(*) from database_instance where hostname='$4' and master_host='$3'" 2>/dev/null)" = 1 ]
}
