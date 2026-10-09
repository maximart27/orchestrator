#!/usr/bin/env bash
# Helpers of the CHAOS suite (tests/multitenant/chaos-*.sh, run-chaos.sh): fault injection against the Raft of the
# multi-tenant orchestrator with real containers. Sources lib.sh (does not modify it).
#
# Isolation from the other suites of this directory:
#   * prefix  orcmtc   (containers/volumes/networks are "orcmtc-*"; lib.sh filters with the anchored regex ^orcmtc-
#                       so it can never match "orcmt-*", and the other way round)
#   * subnet  172.31.88.0/24 (databases + HTTP) and 172.31.89.0/24 (Raft only, see below)
#
# Network model. Every node has TWO interfaces:
#   orcmtc-net      .101-.103  databases + HTTP clients (the address of lib.sh's ip_of)
#   orcmtc-raftnet  .101-.103  Raft (RaftBind/RaftAdvertise/RaftNodes) and HTTPAdvertise (follower -> leader proxy)
# A "raft partition" (partition_raft) disconnects a node from orcmtc-raftnet only: it still reaches every database,
# which is the dangerous case (an isolated node that wrongly believes it is the leader would act on real masters).
# A "full isolation" (partition_full) disconnects it from both.
#
# Source it:   source "$(dirname "$0")/lib-chaos.sh"
#
# Knobs (environment): CHAOS_SHARED=1 (the environment is owned by run-chaos.sh: scenario scripts only remove their
# own nodes/helpers on exit, not the databases), CHAOS_PRE_SLEEP_A (seconds the PreFailoverProcesses hook of tenant
# mariadb-a sleeps; 0 = no pre hook), CHAOS_TMPFS_B=1 (tenant mariadb-b's SQLite on a small tmpfs /bk, scenario 7),
# CHAOS_DROP_TENANT_N3 (tenant removed from n3's tenants.json, scenario 8), CHAOS_SEED, ORCMT_KEEP=1 (debug).

CHAOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export ORCMT_PREFIX="${ORCMT_PREFIX:-orcmtc}"
export ORCMT_SUBNET="${ORCMT_SUBNET:-172.31.88.0/24}"
if [ "$ORCMT_PREFIX" = "orcmt" ] && [ "${CHAOS_ALLOW_ORCMT:-0}" != 1 ]; then
  echo "lib-chaos.sh: refusing to run with ORCMT_PREFIX=orcmt (the chaos suite uses orcmtc to stay away from the other suites)" >&2
  exit 2
fi
source "$CHAOS_DIR/lib.sh"

CHAOS_RAFT_NET="${ORCMT_PREFIX}-raftnet"
CHAOS_RAFT_SUBNET="${CHAOS_RAFT_SUBNET:-172.31.89.0/24}"
CHAOS_RAFT_PFX="${CHAOS_RAFT_SUBNET%.0/24}"
CHAOS_LOGS_DIR="$ORCMT_WORK/logs"
CHAOS_SCENARIO="${CHAOS_SCENARIO:-adhoc}"
NODES=(n1 n2 n3)
export CHAOS_RAFT_NET CHAOS_RAFT_SUBNET CHAOS_RAFT_PFX

raft_ip() { case "$1" in n1) echo "$CHAOS_RAFT_PFX.101";; n2) echo "$CHAOS_RAFT_PFX.102";; n3) echo "$CHAOS_RAFT_PFX.103";; *) echo "raft_ip: unknown $1" >&2; return 1;; esac; }
node_of_raft_ip() { for n in n1 n2 n3; do [ "$(raft_ip $n)" = "$1" ] && { echo $n; return; }; done; }

# ------------------------------------------------------------------ cleanup / logs
# Only resources named ${ORCMT_PREFIX}-* (anchored): containers, volumes, the two networks.
chaos_cleanup() {
  orcmt_cleanup
  docker network rm "$CHAOS_RAFT_NET" >/dev/null 2>&1 || true
}
# Saves the logs of every container into $ORCMT_WORK/logs/<scenario>/ (and keeps a copy of the last run in logs/)
chaos_save_logs() {
  local d="$CHAOS_LOGS_DIR/${1:-$CHAOS_SCENARIO}" c; mkdir -p "$d"
  for c in $(docker ps -a --format '{{.Names}}' --filter "name=^${ORCMT_PREFIX}-" 2>/dev/null); do
    docker logs "$c" > "$d/$c.log" 2>&1 || true
  done
}
chaos_scenario_exit() {
  local rc=$?
  trap - EXIT
  chaos_save_logs
  [ -n "${CHAOS_GEN_PID:-}" ] && kill "$CHAOS_GEN_PID" 2>/dev/null
  if [ "$ORCMT_KEEP" = "1" ]; then log "ORCMT_KEEP=1: leaving ${ORCMT_PREFIX}-* resources (run-chaos.sh cleanup)"; return $rc; fi
  if [ "${CHAOS_SHARED:-0}" = 1 ]; then
    # the databases stay for the next scenario; nodes and helpers go
    local c; for c in $(docker ps -a --format '{{.Names}}' --filter "name=^${ORCMT_PREFIX}-n[0-9]"); do docker rm -f -v "$c" >/dev/null 2>&1; done
    docker volume ls -q --filter "name=^${ORCMT_PREFIX}-n[0-9]" | xargs -r docker volume rm -f >/dev/null 2>&1
  else
    chaos_cleanup
  fi
  return $rc
}
chaos_trap() { trap chaos_scenario_exit EXIT; trap 'exit 130' INT TERM; }

raftnet_up() {
  docker network inspect "$CHAOS_RAFT_NET" >/dev/null 2>&1 && return 0
  docker network create --subnet "$CHAOS_RAFT_SUBNET" "$CHAOS_RAFT_NET" >/dev/null || die "cannot create network $CHAOS_RAFT_NET ($CHAOS_RAFT_SUBNET)"
}

# ------------------------------------------------------------------ build
chaos_build_if_needed() {
  mkdir -p "$ORCMT_WORK/bin"
  [ -x "$ORCMT_WORK/bin/orchestrator" ] && [ -x "$ORCMT_WORK/bin/validate-tenants" ] && [ "${ORCMT_REBUILD:-0}" != 1 ] || build_orchestrator
}

# ------------------------------------------------------------------ database topologies
mariadb_topo_healthy() { # a|b
  local l="$1"
  mariadb_ready "${l}1" && mariadb_ready "${l}2" && mariadb_writable "${l}1" && ! mariadb_writable "${l}2" && mariadb_repl_running "${l}2" \
    && docker exec "$(cname ${l}2)" mariadb -uroot -p"$MARIADB_ROOT_PW" -e 'SHOW SLAVE STATUS\G' 2>/dev/null | grep -q "Master_Host: $(ip_of ${l}1)\$"
}
pg_topo_healthy() { # c
  pg_ready c1 && pg_ready c2 && pg_promoted c1 && [ "$(psql_in c2 'select pg_is_in_recovery()' 2>/dev/null)" = t ] && pg_standby_streaming c1
}
topo_healthy() { case "$1" in a|b) mariadb_topo_healthy "$1";; c) pg_topo_healthy;; esac; }
topo_remove() { # a|b|c
  local l="$1" c
  for c in "${l}1" "${l}2"; do docker rm -f -v "$(cname $c)" >/dev/null 2>&1 || true; docker volume rm -f "$(cname $c)-data" >/dev/null 2>&1 || true; done
}
# topo_ensure a b c ... : (re)builds, in parallel, every requested topology that is missing or not in its pristine state
# (master = x1 writable, replica = x2 replicating from it). Dirty ones (after a failover) are rebuilt from scratch.
topo_ensure() {
  net_up; raftnet_up
  local t bad=()
  for t in "$@"; do topo_healthy "$t" >/dev/null 2>&1 || bad+=("$t"); done
  [ ${#bad[@]} -eq 0 ] && { log "topologies already pristine: $*"; return 0; }
  log "(re)building topologies: ${bad[*]}"
  for t in "${bad[@]}"; do topo_remove "$t"; done
  "$ORCMT_DIR/topology.sh" up "${bad[@]}" || die "topologies ${bad[*]} failed"
}

# ------------------------------------------------------------------ node configuration and start
# chaos_gen_config : gen-config.sh m2, then (python3, on the host) puts Raft/HTTPAdvertise on the raft network, replaces
# the hooks by /conf/hook.sh (records timestamp, node, the node's raft state AT THE TIME the hook runs, tenant), and
# applies the per-scenario knobs (CHAOS_PRE_SLEEP_A, CHAOS_TMPFS_B, CHAOS_DROP_TENANT_N3).
chaos_gen_config() {
  "$ORCMT_DIR/gen-config.sh" m2 >/dev/null || die "gen-config failed"
  CONF="$ORCMT_WORK/conf" PFX_DB="$ORCMT_PFX" PFX_RAFT="$CHAOS_RAFT_PFX" PRE_A="${CHAOS_PRE_SLEEP_A:-0}" TMPFS_B="${CHAOS_TMPFS_B:-0}" \
  DROP_N3="${CHAOS_DROP_TENANT_N3:-}" T_A="$TENANT_A" T_B="$TENANT_B" T_C="$TENANT_C" python3 - <<'PY' || die "chaos config post-processing failed"
import json, os
conf = os.environ["CONF"]; pdb = os.environ["PFX_DB"]; prf = os.environ["PFX_RAFT"]
hook = r'''#!/bin/bash
# hook.sh TENANT PHASE SLEEP key=value...   (chaos suite) -- PHASE pre|post
t="$1"; ph="$2"; sl="$3"; shift 3
st=$(curl -s -m 2 http://127.0.0.1:3000/api/raft-state | tr -d '"')
f=/data/hook-$t.log; [ "$ph" = pre ] && f=/data/hook-$t-pre.log
echo "$(date +%s) node=$(hostname) state=$st phase=$ph ev=start tenant=$t env=$ORC_TENANT_ID $*" >> "$f"
if [ "$sl" -gt 0 ] 2>/dev/null; then sleep "$sl"; echo "$(date +%s) node=$(hostname) phase=$ph ev=end tenant=$t $*" >> "$f"; fi
exit 0
'''
for n in ("n1", "n2", "n3"):
    d = os.path.join(conf, n)
    p = os.path.join(d, "orchestrator.conf.json")
    c = json.load(open(p))
    def sw(v): return v.replace(pdb + ".", prf + ".", 1)
    c["RaftBind"] = sw(c["RaftBind"]); c["RaftAdvertise"] = sw(c["RaftAdvertise"])
    c["RaftNodes"] = [sw(x) for x in c["RaftNodes"]]
    c["HTTPAdvertise"] = sw(c["HTTPAdvertise"])
    json.dump(c, open(p, "w"), indent=1)
    open(os.path.join(d, "hook.sh"), "w").write(hook)
    tp = os.path.join(d, "tenants.json")
    tj = json.load(open(tp))
    keep = []
    for t in tj["Tenants"]:
        cfg = t["Config"]; tid = t["ID"]
        cfg["PostMasterFailoverProcesses"] = ["bash /conf/hook.sh %s post 0 failed={failedHost} successor={successorHost}" % tid]
        if tid == os.environ["T_B"]:
            cfg["PreFailoverProcesses"] = ["bash /conf/hook.sh %s pre 0 failed={failedHost}" % tid]
            if os.environ["TMPFS_B"] == "1":
                t["Backend"]["SQLite3DataFile"] = "/bk/%s.db" % tid
        if tid == os.environ["T_A"] and int(os.environ["PRE_A"]) > 0:
            cfg["PreFailoverProcesses"] = ["bash /conf/hook.sh %s pre %s failed={failedHost}" % (tid, os.environ["PRE_A"])]
        if n == "n3" and os.environ["DROP_N3"] == tid:
            continue
        keep.append(t)
    tj["Tenants"] = keep
    json.dump(tj, open(tp, "w"), indent=1)
PY
}

# cnode_start NODE : like lib.sh's node_start, but with the second (Raft) interface attached BEFORE the process starts
cnode_start() {
  local n="$1" c; c="$(cname "$n")"
  docker rm -f "$c" >/dev/null 2>&1 || true
  local extra=()
  [ "${CHAOS_TMPFS_B:-0}" = 1 ] && extra+=(--tmpfs "/bk:rw,size=${CHAOS_TMPFS_SIZE:-6m},mode=1777")
  docker create --name "$c" --network "$ORCMT_NET" --ip "$(ip_of "$n")" --memory "$MEM_NODE" --stop-timeout 10 --hostname "$c" \
    -v "$c-data:/data" -v "$ORCMT_WORK/conf/$n":/conf:ro \
    -v "$ORCMT_WORK/bin/orchestrator":/usr/local/bin/orchestrator:ro -v "$ORCMT_REPO/resources":/orchestrator/resources:ro \
    -e ORC_A_TOPOLOGY_PASSWORD="$TOPO_PW_A" -w /orchestrator "${extra[@]}" \
    "$IMG_GO" orchestrator -config /conf/orchestrator.conf.json http >/dev/null || die "cannot create $c"
  docker network connect --ip "$(raft_ip "$n")" "$CHAOS_RAFT_NET" "$c" || die "cannot attach $c to $CHAOS_RAFT_NET"
  docker start "$c" >/dev/null || die "cannot start $c"
}
node_remove_volume() { docker volume rm -f "$(cname "$1")-data" >/dev/null 2>&1 || true; }

# nodes_fresh : brand-new nodes (empty volumes, new Raft state) with the current knobs; waits for HTTP, one Leader,
# agreement, and every tenant discovered on the leader. Sets LEADER.
nodes_fresh() {
  local n
  for n in "${NODES[@]}"; do docker rm -f -v "$(cname $n)" >/dev/null 2>&1 || true; node_remove_volume "$n"; done
  chaos_gen_config
  for n in "${NODES[@]}"; do cnode_start "$n"; done
  for n in "${NODES[@]}"; do wait_until "$T_NODE_READY" "$(cname $n) HTTP" node_http_ready "$n" || { node_logs "$n" | tail -30 >&2; die "$n did not come up"; }; done
  cluster_stable "${NODES[@]}" || die "no stable Raft cluster after start"
  wait_until "$T_DISCOVERY" "leader $LEADER discovered every tenant" chaos_all_tenants_on "$LEADER" || die "tenants not discovered on $LEADER"
}

# ------------------------------------------------------------------ raft observation
node_paused() { [ "$(docker inspect -f '{{.State.Paused}}' "$(cname "$1")" 2>/dev/null)" = true ]; }
node_usable() { node_running "$1" && ! node_paused "$1"; }
rstate() { node_usable "$1" || { echo down; return; }; local s; s="$(api "$1" /api/raft-state 2>/dev/null | tr -d '"')"; echo "${s:-unknown}"; }
# leaders_among NODE... -> the nodes that report themselves Leader (one per line)
leaders_among() { local n; for n in "$@"; do [ "$(rstate "$n")" = Leader ] && echo "$n"; done; }
states_line() { local n out=""; for n in "$@"; do out="$out$n=$(rstate "$n") "; done; echo "$out"; }
# single_leader NODE... : exactly one node is Leader (prints it)
single_leader() { local l; l="$(leaders_among "$@")"; [ "$(echo "$l" | grep -c .)" = 1 ] && echo "$l"; }
agree_on() { # LEADER NODE... : every usable node reports the leader's RAFT address
  local l="$1" n; shift
  for n in "$@"; do node_usable "$n" || continue
    api "$n" /api/raft-leader 2>/dev/null | grep -q "$(raft_ip "$l")" || return 1
  done
}
followers_ok() { local l="$1" n; shift; agree_on "$l" "$@" || return 1; for n in "$@"; do [ "$n" = "$l" ] || [ "$(rstate "$n")" = Follower ] || return 1; done; }
# cluster_stable NODE... : exactly one Leader among them, every other one Follower, all agree. Sets LEADER.
cluster_stable() {
  local nodes=("$@") l n
  wait_until "$T_RAFT_ELECTION" "single Leader among ${nodes[*]}" single_leader "${nodes[@]}" || { states_line "${nodes[@]}" >&2; return 1; }
  l="$(single_leader "${nodes[@]}")"
  wait_until 40 "all of ${nodes[*]} follow $l" followers_ok "$l" "${nodes[@]}" || return 1
  LEADER="$l"; FOLLOWERS=(); for n in "${nodes[@]}"; do [ "$n" != "$l" ] && FOLLOWERS+=("$n"); done
  return 0
}
chaos_all_tenants_on() { # NODE
  local t n="$1"
  for t in "${TENANTS[@]}"; do topology_matches "$n" "$(tdb "$t")" "$(tenant_prov "$t")" "$(tenant_master "$t")" "$(tenant_replica "$t")" "$(tenant_port "$t")" || return 1; done
}
# tdb TENANT : path of the tenant's SQLite inside a node
tdb() { if [ "${CHAOS_TMPFS_B:-0}" = 1 ] && [ "$1" = "$TENANT_B" ]; then echo "/bk/$1.db"; else tenant_db "$1"; fi; }

# ------------------------------------------------------------------ fault injection
partition_raft() { docker network disconnect "$CHAOS_RAFT_NET" "$(cname "$1")" >/dev/null || return 1; log "PARTITION $1 (raft network cut, databases still reachable)"; }
heal_raft()      { docker network connect --ip "$(raft_ip "$1")" "$CHAOS_RAFT_NET" "$(cname "$1")" >/dev/null || return 1; log "HEAL $1 (raft network back)"; }
partition_full() { docker network disconnect "$CHAOS_RAFT_NET" "$(cname "$1")" >/dev/null && docker network disconnect "$ORCMT_NET" "$(cname "$1")" >/dev/null || return 1; log "ISOLATE $1 (raft + database networks cut)"; }
heal_full()      { docker network connect --ip "$(ip_of "$1")" "$ORCMT_NET" "$(cname "$1")" >/dev/null && docker network connect --ip "$(raft_ip "$1")" "$CHAOS_RAFT_NET" "$(cname "$1")" >/dev/null || return 1; log "HEAL $1 (all networks back)"; }
node_pause()   { docker pause "$(cname "$1")" >/dev/null; log "PAUSE $1 (frozen like a GC pause / VM freeze)"; }
node_unpause() { docker unpause "$(cname "$1")" >/dev/null; log "UNPAUSE $1"; }
node_kill9()   { docker kill "$(cname "$1")" >/dev/null; log "KILL -9 $1"; }
node_up()      { docker start "$(cname "$1")" >/dev/null; log "START $1"; }
node_stop()    { docker stop -t "${2:-10}" "$(cname "$1")" >/dev/null; log "STOP $1"; }

# ------------------------------------------------------------------ tenant commands (through the tenant API of a node)
# tapi NODE TENANT PATH [curl args] -> raw body
tapi() { local n="$1" t="$2" p="$3"; shift 3; api "$n" "/api/tenants/$t/$p" "$@"; }
# tapi_ok NODE TENANT PATH -> succeeds when {"Code":"OK"}
tapi_ok() { tapi "$1" "$2" "$3" 2>/dev/null | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("Code")=="OK" else 1)' 2>/dev/null; }
# tag_set NODE TENANT -> "host|port|name|value" of every row of database_instance_tags (sorted)
tag_rows() { sql "$1" "$(tdb "$2")" "select hostname,port,tag_name,tag_value from database_instance_tags order by hostname,port,tag_name"; }
tag_count() { sql "$1" "$(tdb "$2")" "select count(*) from database_instance_tags" 2>/dev/null || echo -1; }
downtime_rows() { sql "$1" "$(tdb "$2")" "select hostname,port,owner,reason from database_instance_downtime order by hostname,port"; }
# state fingerprint of a tenant on a node: only columns that must be identical on every node (no timestamps / discovery data)
tenant_fp() {
  local n="$1" t="$2" d; d="$(tdb "$t")"
  {
    echo "tags:"; sql "$n" "$d" "select hostname,port,tag_name,tag_value from database_instance_tags order by 1,2,3"
    # 'lost-in-recovery' is written by the leader that runs a recovery, locally (not through Raft): upstream behaviour,
    # excluded here on purpose (documented in the suite README)
    echo "downtime:"; sql "$n" "$d" "select hostname,port,owner,reason from database_instance_downtime where reason <> 'lost-in-recovery' order by 1,2"
    echo "ack:"; sql "$n" "$d" "select hostname,port,analysis,acknowledged,acknowledged_by from topology_recovery order by hostname,port,analysis,recovery_id" 2>/dev/null
  } | md5sum | cut -c1-12
}
# converged TENANT NODE... : every node has the same fingerprint for the tenant
converged() { local t="$1" first="" n fp; shift; for n in "$@"; do fp="$(tenant_fp "$n" "$t")"; [ -z "$first" ] && first="$fp"; [ "$fp" = "$first" ] || return 1; done; }

# ------------------------------------------------------------------ evidence
# recovery start lines of a node's log (the line the leader logs when it runs a recovery)
REC_RE='will handle Dead(Master|Primary) event on'
node_rec_count() { node_logs "$1" | grep -c -E "$REC_RE"; }
# post/pre hook lines of a tenant across nodes: "node: lines"
hooks_by_node() { local n t="$1" out=""; for n in "${NODES[@]}"; do out="$out$n=$(hook_lines "$n" "$t") "; done; echo "$out"; }
hooks_total() { local n t="$1" s=0 v; for n in "${NODES[@]}"; do v="$(hook_lines "$n" "$t")"; s=$((s+${v:-0})); done; echo "$s"; }
# hook file content of a node (works for stopped nodes)
hook_file() { # NODE TENANT [pre]
  local n="$1" f="/data/hook-$2.log"; [ "${3:-}" = pre ] && f="/data/hook-$2-pre.log"
  if node_running "$n"; then docker exec "$(cname "$n")" sh -c "cat $f 2>/dev/null"; else docker run --rm -v "$(cname "$n")-data:/data" "$IMG_GO" sh -c "cat $f 2>/dev/null"; fi
}
# hooks_run_by_non_leader TENANT -> hook lines (all nodes) whose recorded raft state is not Leader
hooks_non_leader() { local n; for n in "${NODES[@]}"; do hook_file "$n" "$1" | grep 'ev=start' | grep -v 'state=Leader' | sed "s/^/$n: /"; done; }
# bad_log_lines NODE -> suspicious lines (panic/fatal/unknown tenant/stack traces)
bad_log_lines() { node_logs "$1" | grep -E -i 'panic|fatal|unknown tenant|goroutine [0-9]+ \[|nil pointer|segmentation' | head -5; }
assert_clean_logs() { # label NODE...
  local label="$1" n b; shift
  for n in "$@"; do b="$(bad_log_lines "$n")"; [ -z "$b" ] && pass "$label: $n log has no panic/fatal/'unknown tenant'" || fail "$label: $n has suspicious log lines" "$b"; done
}
# restarts of a node's container (docker's counter; a process crash with restart policy would show here)
node_alive_pid() { docker exec "$(cname "$1")" sh -c 'pgrep -x orchestrator | head -1' 2>/dev/null; }

# Scenario banner / verdict
scenario_begin() { CHAOS_SCENARIO="$1"; CHAOS_T0=$SECONDS; echo "" >&2; echo "######## $1 ########" >&2; }
scenario_verdict() { # prints PASS/FAIL line for the scenario
  local dur=$((SECONDS-CHAOS_T0))
  if [ "$FAIL_COUNT" -eq 0 ]; then echo "SCENARIO-RESULT $CHAOS_SCENARIO PASS (${PASS_COUNT} assertions, ${dur}s)" >&2; else echo "SCENARIO-RESULT $CHAOS_SCENARIO FAIL (${FAIL_COUNT} failed / ${PASS_COUNT} passed, ${dur}s)" >&2; fi
}
# chaos_setup NEEDED_TOPOLOGIES... : common preamble of every scenario script
chaos_setup() {
  chaos_trap
  chaos_build_if_needed
  topo_ensure "$@"
}

# ------------------------------------------------------------------ recovery helpers
# rec_rows NODE TENANT MASTER_IP -> number of topology_recovery rows for that failed master in the node's backend
rec_rows() { sql "$1" "$(tdb "$2")" "select count(*) from topology_recovery where hostname='$3'" 2>/dev/null || echo 0; }
rec_rows_at_least() { [ "$(rec_rows "$1" "$2" "$3")" -ge "$4" ] 2>/dev/null; }
hook_at_least() { [ "$(hook_lines "$1" "$2")" -ge "$3" ] 2>/dev/null; }
# master_down TENANT : stops the tenant's master (x1), like a crash
master_down() { local t="$1" l; case "$t" in "$TENANT_A") l=a;; "$TENANT_B") l=b;; "$TENANT_C") l=c;; esac; docker stop -t 2 "$(cname ${l}1)" >/dev/null; log "master of $t ($(cname ${l}1)) stopped"; }
promoted() { # TENANT : the replica (x2) of the tenant is the writable master
  case "$1" in "$TENANT_A") mariadb_writable a2;; "$TENANT_B") mariadb_writable b2;; "$TENANT_C") pg_promoted c2;; esac
}
# cluster_name_of NODE TENANT -> cluster_name of the tenant's recovery rows on that node
cluster_name_of() { sql "$1" "$(tdb "$2")" "select cluster_name from topology_recovery order by recovery_id limit 1"; }
unack_count() { sql "$1" "$(tdb "$2")" "select count(*) from topology_recovery where acknowledged=0"; }
ack_count() { sql "$1" "$(tdb "$2")" "select count(*) from topology_recovery where acknowledged=1"; }
nonleader_lines() { node_logs "$1" | grep -c "NOT detecting/recovering host (raft non-leader)"; }
tag_present() { [ "$(sql "$1" "$(tdb "$2")" "select count(*) from database_instance_tags where tag_name='$3'" 2>/dev/null)" -ge 1 ] 2>/dev/null; }

# ------------------------------------------------------------------ traffic generator (host side)
# traffic_start PREFIX LOGFILE [workers] [pace] [max] -> starts chaos-traffic.py in the background (CHAOS_GEN_PID, CHAOS_GEN_STOP)
traffic_start() {
  local prefix="$1" log="$2" workers="${3:-4}" pace="${4:-0.05}" max="${5:-0}" t spec=""
  for t in "${TENANTS[@]}"; do spec="$spec${spec:+,}$t=$(tenant_master "$t"):$(tenant_port "$t")"; done
  CHAOS_GEN_STOP="$ORCMT_WORK/stop-$prefix"; rm -f "$CHAOS_GEN_STOP" "$log"; CHAOS_GEN_LOG="$log"
  python3 "$CHAOS_DIR/chaos-traffic.py" run --nodes "$(ip_of n1),$(ip_of n2),$(ip_of n3)" --tenants "$spec" --prefix "$prefix" \
    --log "$log" --workers "$workers" --pace "$pace" --max "$max" --stop-file "$CHAOS_GEN_STOP" >/dev/null 2>&1 &
  CHAOS_GEN_PID=$!
  log "traffic generator '$prefix' started (pid $CHAOS_GEN_PID, $workers workers)"
}
traffic_stop() { touch "$CHAOS_GEN_STOP"; local i=0; while kill -0 "$CHAOS_GEN_PID" 2>/dev/null && [ $i -lt 60 ]; do sleep 1; i=$((i+1)); done; kill "$CHAOS_GEN_PID" 2>/dev/null; CHAOS_GEN_PID=""; }
traffic_stats() { python3 "$CHAOS_DIR/chaos-traffic.py" stats --log "$CHAOS_GEN_LOG"; }
# traffic_verify PREFIX NODE... : every ACCEPTED command (tags minus accepted untags) is present in every node, per tenant.
# Sets TRAFFIC_MISSING / TRAFFIC_EXTRA (totals over nodes) and prints one evidence line per node/tenant.
traffic_verify() {
  local prefix="$1" t n exp act miss extra; shift
  TRAFFIC_MISSING=0; TRAFFIC_EXTRA=0
  for t in "${TENANTS[@]}"; do
    exp="$(python3 "$CHAOS_DIR/chaos-traffic.py" expected --log "$CHAOS_GEN_LOG" --tenant "$t")"
    for n in "$@"; do
      act="$(sql "$n" "$(tdb "$t")" "select tag_name||'|'||tag_value from database_instance_tags where tag_name like '${prefix}-%' order by 1")"
      miss="$(comm -23 <(printf '%s\n' "$exp" | sort) <(printf '%s\n' "$act" | sort) | grep -c .)"
      extra="$(comm -13 <(printf '%s\n' "$exp" | sort) <(printf '%s\n' "$act" | sort) | grep -c .)"
      log "verify $t on $n: expected=$(printf '%s\n' "$exp" | grep -c .) actual=$(printf '%s\n' "$act" | grep -c .) missing=$miss extra=$extra"
      TRAFFIC_MISSING=$((TRAFFIC_MISSING+miss)); TRAFFIC_EXTRA=$((TRAFFIC_EXTRA+extra))
    done
  done
}

# fp_detail NODE TENANT -> the rows behind tenant_fp, on one line per set (diagnostics for a failed convergence)
fp_detail() {
  local n="$1" t="$2" d; d="$(tdb "$t")"
  echo "tags=$(sql "$n" "$d" "select count(*) from database_instance_tags") non-bulk-tags=[$(sql "$n" "$d" "select tag_name from database_instance_tags where tag_name not like 'r5-%' and tag_name not like 's6-%' and tag_name not like 'w6-%' and tag_name not like 'k6-%' and tag_name not like 'soak-%' order by 1" | tr '\n' ',')]" \
       "downtime=[$(sql "$n" "$d" "select hostname,reason from database_instance_downtime where reason <> 'lost-in-recovery'" | tr '\n' ';')]" \
       "recoveries=[$(sql "$n" "$d" "select recovery_id,hostname,ifnull(successor_hostname,'-'),acknowledged,in_active_period from topology_recovery order by recovery_id" | tr '\n' ';')]"
}
diverge_report() { local t="$1" n; shift; for n in "$@"; do echo "$n fp=$(tenant_fp "$n" "$t") $(fp_detail "$n" "$t")"; done; }
