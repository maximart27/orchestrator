#!/usr/bin/env bash
# Helpers of the mixed-version (rolling upgrade) suite, tests/multitenant/run-mixed.sh.
#
# It sources lib.sh and only overrides what a small PC needs: its own prefix/subnet/work dir (orcmtv-*),
# small memory limits, ONE MariaDB topology (a1 -> a2) and three orchestrator nodes that can each run the OLD
# (upstream master, pre multi-tenant) or the NEW binary on the same data volume.
#
#   OLD = $ORCMT_WORK/bin/orchestrator-old   (git archive of `master`, built offline with -mod=vendor)
#   NEW = $ORCMT_WORK/bin/orchestrator-new   (this tree)

export ORCMT_PREFIX="${ORCMT_PREFIX:-orcmtv}"
export ORCMT_SUBNET="${ORCMT_SUBNET:-172.31.99.0/24}"
export ORCMT_WORK="${ORCMT_WORK:-/tmp/orcmtv-work}"
export MEM_MARIADB="${MEM_MARIADB:-256m}"
export MEM_NODE="${MEM_NODE:-256m}"
# Failovers of ONE tenant take ~20-40s; the block must outlive one failover, and the next one waits it out.
export RECOVERY_BLOCK_SECONDS="${RECOVERY_BLOCK_SECONDS:-60}"
export T_DISCOVERY="${T_DISCOVERY:-60}"
export T_FAILOVER="${T_FAILOVER:-120}"
MIN_FREE_MB="${MIN_FREE_MB:-1500}"

# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# Logs of every container incarnation of the running scenario (a node swapped to another binary keeps its old log here).
LOGDIR="${LOGDIR:-$ORCMT_WORK/logs}"
orcmt_save_logs() {   # overrides lib.sh: logs go to $LOGDIR, the previous incarnations (<c>.old.log / <c>.new.log) stay next to them
  local c; mkdir -p "$LOGDIR"
  for c in $(docker ps -a --format '{{.Names}}' --filter "name=^${ORCMT_PREFIX}-" 2>/dev/null); do
    docker logs "$c" > "$LOGDIR/$c.log" 2>&1 || true
  done
}

NODES=(n1 n2 n3)
DB_A="/data/$TENANT_A.db"          # legacy profile: the single backend of every node (gen-config.sh legacy-raft3)
HOOK_A="/data/hook-$TENANT_A.log"

# ---------------------------------------------------------------- memory gate
avail_mb() { free -m | awk '/^Mem:/ {print $7}'; }
# wait_memory: blocks (up to 10 min) until MIN_FREE_MB are available, instead of starting on a starved PC.
wait_memory() {
  local i=0 a
  while a="$(avail_mb)"; [ "$a" -lt "$MIN_FREE_MB" ]; do
    [ "$i" -ge 60 ] && die "only ${a} MB available (need ${MIN_FREE_MB}) after 10 minutes"
    log "only ${a} MB available (< ${MIN_FREE_MB}); waiting 10s ($i/60)"; sleep 10; i=$((i+1))
  done
  log "memory ok: ${a} MB available"
}

# ---------------------------------------------------------------- binaries
OLD_BIN="$ORCMT_WORK/bin/orchestrator-old"
NEW_BIN="$ORCMT_WORK/bin/orchestrator-new"
_go_build() {  # SRC_DIR OUT_NAME
  docker run --rm --network none --user "$(id -u):$(id -g)" -e GOCACHE=/gocache -e GOFLAGS=-mod=vendor -e HOME=/tmp \
    -v "$1":/src -v "$ORCMT_WORK/gocache":/gocache -v "$ORCMT_WORK/bin":/out -w /src "$IMG_GO" \
    go build -o "/out/$2" ./go/cmd/orchestrator || die "build of $2 failed"
}
# build_binaries: OLD from `git archive master` (the vendor/ dir is copied from this tree: same go.mod), NEW from this tree.
build_binaries() {
  mkdir -p "$ORCMT_WORK/bin" "$ORCMT_WORK/gocache"
  local up="${ORCMT_UPSTREAM_REPO:-$ORCMT_REPO}" src="$ORCMT_WORK/old-src"
  if [ ! -x "$OLD_BIN" ] || [ "${ORCMT_REBUILD:-0}" = 1 ]; then
    log "building OLD (upstream master) offline"
    rm -rf "$src"; mkdir -p "$src"
    git -C "$up" archive "${ORCMT_OLD_REF:-master}" | tar -x -C "$src"
    [ -d "$src/vendor" ] || cp -r "$ORCMT_REPO/vendor" "$src/vendor"
    diff -q <(git -C "$up" show "${ORCMT_OLD_REF:-master}:go.mod") "$ORCMT_REPO/go.mod" >/dev/null || log "WARNING: go.mod differs between OLD and NEW; vendor/ may not match"
    _go_build "$src" orchestrator-old
  fi
  log "building NEW (this tree) offline"
  _go_build "$ORCMT_REPO" orchestrator-new
}

# ---------------------------------------------------------------- MariaDB (small buffer pool)
mariadb_start() {
  local n="$1" sid="$2" dom="$3" ro="${4:-}" ip; ip="$(ip_of "$n")"
  local extra=(); [ "$ro" = "ro" ] && extra+=(--read-only=ON)
  docker run -d --name "$(cname "$n")" --network "$ORCMT_NET" --ip "$ip" --memory "$MEM_MARIADB" \
    -v "$(cname "$n")-data:/var/lib/mysql" -e MARIADB_ROOT_PASSWORD="$MARIADB_ROOT_PW" \
    "$IMG_MARIADB" \
    --server-id="$sid" --gtid-domain-id="$dom" --log-bin=mysql-bin --binlog-format=ROW \
    --log-slave-updates=ON --report-host="$ip" --skip-name-resolve \
    --innodb-buffer-pool-size=32M --innodb-log-file-size=16M --performance-schema=OFF --max-connections=40 \
    --key-buffer-size=1M --table-open-cache=64 \
    "${extra[@]}" >/dev/null || die "cannot start $(cname "$n")"
}

# ---------------------------------------------------------------- nodes with a chosen binary
declare -A NODE_VER   # n1 -> old|new
# node_start_ver NODE old|new   (same volume, same config; the Raft dir and the SQLite backend survive)
node_start_ver() {
  local n="$1" ver="$2" c bin; c="$(cname "$n")"
  case "$ver" in old) bin="$OLD_BIN";; new) bin="$NEW_BIN";; *) die "node_start_ver: $ver";; esac
  docker rm -f "$c" >/dev/null 2>&1 || true
  docker run -d --name "$c" --network "$ORCMT_NET" --ip "$(ip_of "$n")" --memory "$MEM_NODE" --stop-timeout 10 --hostname "$c" \
    -v "$c-data:/data" -v "$ORCMT_WORK/conf/$n":/conf:ro \
    -v "$bin":/usr/local/bin/orchestrator:ro -v "$ORCMT_REPO/resources":/orchestrator/resources:ro \
    -e ORC_A_TOPOLOGY_PASSWORD="$TOPO_PW_A" -w /orchestrator \
    "$IMG_GO" orchestrator -config /conf/orchestrator.conf.json http >/dev/null || die "cannot start $c"
  NODE_VER[$n]="$ver"
}
# node_restart_ver NODE VER: stops the node (gracefully), keeps its log, starts it again with VER.
node_swap() {
  local n="$1" ver="$2" c; c="$(cname "$n")"
  mkdir -p "$LOGDIR"
  docker stop -t 8 "$c" >/dev/null 2>&1 || true
  docker logs "$c" >> "$LOGDIR/$c.${NODE_VER[$n]}.log" 2>&1 || true
  node_start_ver "$n" "$ver"
  wait_until "$T_NODE_READY" "$c ($ver) HTTP" node_http_ready "$n" || { node_logs "$n" | tail -30 >&2; return 1; }
}
# all logs of a node since the beginning of the run (previous incarnations + current)
node_logs_all() {
  local n="$1" c; c="$(cname "$n")"
  cat "$LOGDIR/$c".*.log 2>/dev/null
  docker logs "$c" 2>&1
}
nlog_count() { node_logs_all "$1" | grep -c -E "$2" || true; }

# ---------------------------------------------------------------- raft / cluster checks
leader_of() { raft_leader_node "${NODES[@]}"; }
# healthy_cluster: exactly one leader and every node agrees on it
healthy_cluster() {
  local l n; l="$(leader_of)" || return 1; [ -n "$l" ] || return 1
  for n in "${NODES[@]}"; do api "$n" /api/raft-leader 2>/dev/null | grep -q "$(ip_of "$l")" || return 1; done
}
other_nodes() { local x="$1" n; for n in "${NODES[@]}"; do [ "$n" != "$x" ] && echo "$n"; done; }
# discovered NODE: its backend holds a1+a2 with the right topology
discovered() { topology_matches "$1" "$DB_A" mysql "$(ip_of a1)" "$(ip_of a2)" 3306; }
# discovered_swapped NODE M R: M is master, R replica
discovered_pair() { topology_matches "$1" "$DB_A" mysql "$(ip_of "$2")" "$(ip_of "$3")" 3306; }
# versions: "n1=old n2=new ..."
versions() { local n o=""; for n in "${NODES[@]}"; do o="$o$n=${NODE_VER[$n]} "; done; echo "$o"; }

# ---------------------------------------------------------------- API (always via the leader, forwarded anyway)
# api_ok NODE PATH: succeeds when the orchestrator API answers {"Code":"OK"...}
api_ok() { api "$1" "$2" | grep -q '"Code": *"OK"'; }
api_out() { api "$1" "$2" | head -c 300; }

# ---------------------------------------------------------------- state digest of the replicated tables
# digest NODE -> the raft-replicated state, normalised (no timestamps that are per-node clocks)
digest() {
  local n="$1"
  {
    echo "tags:";      sql "$n" "$DB_A" "select hostname,port,tag_name,tag_value from database_instance_tags order by 1,2,3"
    echo "downtime:";  sql "$n" "$DB_A" "select hostname,port,owner,reason from database_instance_downtime order by 1,2"
    echo "candidate:"; sql "$n" "$DB_A" "select hostname,port,promotion_rule from candidate_database_instance order by 1,2"
    echo "alias_override:"; sql "$n" "$DB_A" "select cluster_name,alias from cluster_alias_override order by 1"
    echo "unresolve:"; sql "$n" "$DB_A" "select hostname,unresolved_hostname from hostname_unresolve order by 1"
    echo "recovery_disabled:"; sql "$n" "$DB_A" "select count(*) from global_recovery_disable"
    echo "recoveries:"; sql "$n" "$DB_A" "select hostname,analysis,acknowledged from topology_recovery order by recovery_id"
    echo "instances:"; sql "$n" "$DB_A" "select hostname,port from database_instance order by 1,2"
  } 2>&1
}
# digests_equal NODE...: every node has the same digest (prints the differing node on failure)
digests_equal() {
  local first="$1" d0 d n; d0="$(digest "$first")"; shift
  for n in "$@"; do d="$(digest "$n")"; [ "$d" = "$d0" ] || { echo "digest of $n differs from $first"; return 1; }; done
}
# hook_runs NODE...: total failover hook executions on the given nodes
hook_total() { local n t=0 c; for n in "$@"; do c="$(hook_lines "$n" "$TENANT_A")"; t=$((t+${c:-0})); done; echo "$t"; }

# ---------------------------------------------------------------- environment
# end_downtimes LEADER: ends the downtime of both instances (a downtimed master or replica is not recovered)
end_downtimes() { local l="$1"; api "$l" "/api/end-downtime/$(ip_of a1)/3306" >/dev/null; api "$l" "/api/end-downtime/$(ip_of a2)/3306" >/dev/null; }
# env_reset: removes every orcmtv-* resource and brings up topology a (a1 master -> a2 replica) + the legacy Raft-of-3 config.
env_reset() {
  orcmt_save_logs; orcmt_cleanup
  NODE_VER=(); CUR_M=a1; CUR_R=a2
  wait_memory
  "$ORCMT_DIR/topology.sh" up a || die "topology a failed"
  "$ORCMT_DIR/gen-config.sh" legacy-raft3 >/dev/null || die "gen-config failed"
}
# start_nodes V1 V2 V3: starts n1..n3 with the given binaries (old|new) and waits for one leader everybody agrees on
start_nodes() {   # start_nodes old new new
  local n i=1
  for n in "${NODES[@]}"; do node_start_ver "$n" "${!i}"; i=$((i+1)); done
  for n in "${NODES[@]}"; do wait_until "$T_NODE_READY" "$n HTTP (${NODE_VER[$n]})" node_http_ready "$n" || { node_logs "$n" | tail -30 >&2; die "$n did not come up"; }; done
  wait_until 60 "one leader, all agree" healthy_cluster || die "no healthy raft cluster"
}
# make_leader NODE: asks the leader to yield until NODE leads (election is random, so it retries)
make_leader() {
  local want="$1" i l
  for i in 1 2 3 4 5 6 7 8 9 10; do
    l="$(leader_of)" && [ "$l" = "$want" ] && return 0
    [ -n "$l" ] && api "$l" "/api/raft-yield-hint/$(cname "$want")" >/dev/null 2>&1
    sleep 8; wait_until 40 "one leader" healthy_cluster >/dev/null 2>&1
  done
  [ "$(leader_of)" = "$want" ]
}
# stable_leader: the leader once the cluster is healthy
cur_leader() { leader_of; }
# wipe_node NODE: removes the whole /data (Raft dir + SQLite backend) of a STOPPED node
wipe_node() { docker run --rm -v "$(cname "$1")-data:/data" "$IMG_GO" sh -c 'rm -rf /data/* /data/.[!.]*' ; }

# ---------------------------------------------------------------- raft log / snapshot helpers
# raft_entries_with_tenant NODE: log entries whose JSON envelope carries a "tenant" field (a legacy tenant never writes it)
raft_entries_with_tenant() { sql "$1" /data/raft/raft_store.db "select count(*) from raft_log where cast(data as text) like '%\"tenant\"%'"; }
raft_entries() { sql "$1" /data/raft/raft_store.db "select count(*) from raft_log"; }
# force_install_snapshot LEADER: takes a snapshot on the leader and removes from its log store every entry before the snapshot
# (what raft's own compaction does once TrailingLogs entries have piled up -- days of traffic), so that a follower that
# starts empty can only be brought up to date with an InstallSnapshot.
force_install_snapshot() {
  local l="$1"
  api "$l" /api/raft-snapshot | grep -q '"Code": *"ERROR"' && return 1
  sleep 3
  docker exec "$(cname "$l")" python3 -c '
import json,glob,sqlite3
idx=max(json.load(open(f))["Index"] for f in glob.glob("/data/raft/snapshots/*/meta.json"))
c=sqlite3.connect("/data/raft/raft_store.db",timeout=30)
c.execute("delete from raft_log where log_index < ?",(idx,)); c.commit()
print("snapshot index",idx,"log entries left",c.execute("select count(*) from raft_log").fetchone()[0])'
}
# snapshot_installed NODE: the node logged that it installed a snapshot sent by the leader
snapshot_installed() { node_logs "$1" | grep -qiE 'installed remote snapshot|installing snapshot|Restored from snapshot|restoring snapshot'; }

# ---------------------------------------------------------------- state-changing API battery (every legacy raft command)
A1IP() { ip_of a1; }
# seed_state LEADER LABEL: puts something of every replicated kind in the cluster through the API.
# (NOT covered, on purpose: delete-instance-tag and submit-pool-instances. /api/untag runs on the node that receives the call and
# is never published to Raft, in OLD and NEW alike; submit-pool-instances writes nothing readable in the sqlite backend, in both.)
seed_state() {
  local l="$1" lab="$2" m r; m="$(ip_of "$CUR_M")"; r="$(ip_of "$CUR_R")"
  api_ok "$l" "/api/begin-downtime/$r/3306/owner-$lab/reason-$lab/1h"            || fail "$lab: begin-downtime" "$(api_out "$l" "/api/begin-downtime/$r/3306/owner-$lab/reason-$lab/1h")"
  api_ok "$l" "/api/tag/$r/3306/tag-$lab/value-$lab"                              || fail "$lab: put-instance-tag"
  api_ok "$l" "/api/register-candidate/$r/3306/prefer"                            || fail "$lab: register-candidate"
  api_ok "$l" "/api/set-cluster-alias/$m:3306?alias=alias-$lab"                   || fail "$lab: set-cluster-alias-manual-override"
  api_ok "$l" "/api/register-hostname-unresolve/$r/3306/virtual-$lab"             || fail "$lab: register-hostname-unresolve"
}

# recovery_for NODE MASTER_IP -> number of topology_recovery rows of that master
recovery_for() { sql "$1" "$DB_A" "select count(*) from topology_recovery where hostname='$2'" 2>/dev/null; }

# node_stop_save NODE: stops the node and keeps the log of this incarnation in $LOGDIR
node_stop_save() {
  local c; c="$(cname "$1")"; mkdir -p "$LOGDIR"
  docker stop -t 8 "$c" >/dev/null 2>&1 || true
  docker logs "$c" >> "$LOGDIR/$c.${NODE_VER[$1]}.log" 2>&1 || true
}

# gen_manifest_conf NODE: turns the generated legacy config of NODE into a tenants manifest: the tenant "default" (the same
# backend file and the same settings as before) plus "tenant-b" (its own SQLite backend, no seeds). The node-level keys stay.
gen_manifest_conf() {
  python3 - "$ORCMT_WORK/conf/$1" <<'PY'
import json, sys
d = sys.argv[1]
c = json.load(open(d + "/orchestrator.conf.json"))
tenant_keys = ["MySQLTopologyUser", "MySQLTopologyPassword", "InstancePollSeconds", "HostnameResolveMethod",
               "MySQLHostnameResolveMethod", "DiscoverySeeds", "DetectClusterAliasQuery", "DetectInstanceAliasQuery",
               "RecoverMasterClusterFilters", "RecoverIntermediateMasterClusterFilters", "RecoveryPeriodBlockSeconds",
               "KVClusterMasterPrefix", "PostMasterFailoverProcesses"]
cfg = {k: c.pop(k) for k in tenant_keys if k in c}
c["SQLite3DataFile"] = "/data/control.db"
c["TenantsConfigFile"] = "/conf/tenants.json"
json.dump(c, open(d + "/orchestrator.conf.json", "w"), indent=1)
b = {k: cfg[k] for k in ["MySQLTopologyUser", "MySQLTopologyPassword", "InstancePollSeconds", "HostnameResolveMethod", "MySQLHostnameResolveMethod"]}
json.dump({"Tenants": [
    {"ID": "default", "Provider": "mysql", "Backend": {"BackendDB": "sqlite", "SQLite3DataFile": "/data/mariadb-a.db"}, "Config": cfg},
    {"ID": "tenant-b", "Provider": "mysql", "Backend": {"BackendDB": "sqlite", "SQLite3DataFile": "/data/tenant-b.db"}, "Config": b}]},
    open(d + "/tenants.json", "w"), indent=1)
PY
}
