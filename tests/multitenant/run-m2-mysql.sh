#!/usr/bin/env bash
# Milestone 2 with a MySQL/MariaDB BACKEND: THREE orchestrator nodes (Raft of three), each running the same tenants,
# ONE MariaDB backend server holding one database per NODE and TENANT (orc_<node>_<tenant>, orc_<node>_control).
# Same flow and assertions as run-m2.sh (leader-only recoveries, leader change, snapshot v2) re-pointed at the databases,
# plus sections 6b-6e specific to MySQL (database purity, schema, node_health mirrors, snapshot restore of a restarted node
# and of a brand new one, with per-tenant table hashes compared across the nodes' databases). See README-mysql.md.
#   flock /tmp/orcmt-docker.lock tests/multitenant/run-m2-mysql.sh
#   M2_TENANTS="mariadb-a postgres-c"   fewer tenants (memory)
#   M2_NEWNODE_OPS=10600                tag commands published to compact the Raft log before the new-node test (0 = skip it)
set -o pipefail
export BK_PER_NODE=1
source "$(dirname "$0")/lib-mysql.sh"
orcmt_trap_cleanup
NODES=(n1 n2 n3)
T0=$SECONDS
# Self-test knobs (see gen-config.sh legacy-raft3): run the same flow with fewer tenants / today's legacy binary.
M2_PROFILE=m2
M2_NEWNODE_OPS="${M2_NEWNODE_OPS:-10600}"
[ -n "${M2_TENANTS:-}" ] && TENANTS=($M2_TENANTS)
MYS=(); PGS=()      # tenants by engine: MariaDB ones are failed over and re-attached, PostgreSQL ones only failed over
for t in "${TENANTS[@]}"; do [ "$(tenant_prov "$t")" = mysql ] && MYS+=("$t") || PGS+=("$t"); done
tenant_letter() { case "$1" in "$TENANT_A") echo a;; "$TENANT_B") echo b;; "$TENANT_C") echo c;; esac; }
t_m1() { echo "$(tenant_letter "$1")1"; }; t_m2() { echo "$(tenant_letter "$1")2"; }     # a1/a2 ... (containers)

recovered_rows() { # NODE TENANT -> rows of the tenant's topology_recovery on that node
  recovery_detail "$1" "$(tenant_db "$2")" 2>/dev/null; }
leader_now()  { raft_leader_node "$@"; }
follower_agree() { # LEADER_NODE NODE... : every running node reports the leader's raft address
  local l="$1" n; shift
  for n in "$@"; do node_running "$n" || continue
    api "$n" /api/raft-leader 2>/dev/null | grep -q "$(ip_of "$l")" || return 1
  done
}
all_tenants_on() { # NODE : all tenants discovered on that node's backends
  local t n="$1"
  for t in "${TENANTS[@]}"; do
    topology_matches "$n" "$(tenant_db "$t")" "$(tenant_prov "$t")" "$(tenant_master "$t")" "$(tenant_replica "$t")" "$(tenant_port "$t")" || return 1
  done
}
recovered_on() { # NODE TENANT MASTER_IP MINCOUNT : at least MINCOUNT rows for that master
  [ "$(sql "$1" "$(tenant_db "$2")" "select count(*) from topology_recovery where hostname='$3'" 2>/dev/null)" -ge "$4" ] 2>/dev/null; }

# ---------------------------------------------------------------------------------------------- setup
avail=$(free -m | awk '/^Mem:/{print $7}'); [ "${avail:-0}" -ge 1500 ] || log "WARNING: only ${avail}MB available memory (suite wants >= 1500)"
[ -x "$ORCMT_WORK/bin/orchestrator" ] && [ -x "$ORCMT_WORK/bin/validate-tenants" ] && [ "${ORCMT_REBUILD:-0}" != 1 ] || build_orchestrator
orcmt_cleanup
net_up
( bk_up ) & bkpid=$!
"$ORCMT_DIR/topology.sh" up $(for t in "${TENANTS[@]}"; do tenant_letter "$t"; done) || die "topologies failed"
wait $bkpid || die "backend server failed"
"$ORCMT_DIR/gen-config-mysql.sh" m2 >/dev/null || die "gen-config-mysql failed"
if [ -z "${M2_TENANTS:-}" ]; then
  for n in "${NODES[@]}"; do
    "$ORCMT_DIR/validate-config.sh" $n 2>&1 | grep -c '^tenant=' | grep -qx 3 || die "tenants manifest of $n rejected by config.LoadTenantConfigs"
  done
else
  # fewer tenants: drop the others from every node's tenants.json
  for n in "${NODES[@]}"; do python3 - "$ORCMT_WORK/conf/$n/tenants.json" $M2_TENANTS <<'PY'
import json, sys
p = sys.argv[1]; keep = set(sys.argv[2:]); t = json.load(open(p)); t["Tenants"] = [x for x in t["Tenants"] if x["ID"] in keep]; json.dump(t, open(p, "w"))
PY
  done
fi
log "3 nodes x ${#TENANTS[@]} tenants: config accepted; infrastructure ready after $((SECONDS-T0))s"
for n in "${NODES[@]}"; do node_start $n; done
for n in "${NODES[@]}"; do
  wait_until "$T_NODE_READY" "$(cname $n) HTTP" node_http_ready $n || { node_logs $n | tail -40 >&2; die "$n did not come up"; }
done

echo "== 1. one raft Leader for all tenants =="
wait_until "$T_RAFT_ELECTION" "exactly one Leader" leader_now "${NODES[@]}" || { for n in "${NODES[@]}"; do echo "$n: $(raft_state $n)"; done >&2; die "no single leader"; }
L1="$(leader_now "${NODES[@]}")"
pass "single Leader elected: $L1 ($(for n in "${NODES[@]}"; do echo -n "$n=$(raft_state $n) "; done))"
wait_until 30 "followers agree on the leader" follower_agree "$L1" "${NODES[@]}" && pass "all nodes report $(ip_of $L1) as leader" || fail "nodes disagree on the leader"
if wait_until "$T_DISCOVERY" "leader $L1 discovered all 3 tenants" all_tenants_on "$L1"; then
  pass "leader $L1: every tenant discovered its own master+replica"
else
  fail "leader $L1: tenants not discovered" "$(for t in "${TENANTS[@]}"; do echo "$t: $(topology_rows $L1 "$(tenant_db "$t")" | tr '\n' ';')"; done)"
fi
for n in "${NODES[@]}"; do   # per-tenant API on every node (followers forward to the leader, tenant included)
  for t in "${TENANTS[@]}"; do
    api "$n" "/api/tenants/$t/clusters" | grep -q "$(tenant_master "$t")" && pass "$n: GET /api/tenants/$t/clusters" || fail "$n: GET /api/tenants/$t/clusters has no $(tenant_master "$t")" "$(api "$n" "/api/tenants/$t/clusters" | head -c 200)"
  done
done
for n in "${NODES[@]}"; do [ "$n" = "$L1" ] && continue
  all_tenants_on "$n" && log "follower $n also probes every tenant (expected: raft followers discover independently)" || log "follower $n: not all tenants discovered yet (informational)"
done

# ---------------------------------------------------------------------------------------------- failover 1
echo "== 2. leader $L1 recovers ALL tenants; followers recover none =="
LC="$(cname $L1)"
T_FO1="$SECONDS"
log "stopping the masters of ${TENANTS[*]} at once"
docker stop -t 2 $(for t in "${TENANTS[@]}"; do cname "$(t_m1 "$t")"; done) >/dev/null
for t in "${TENANTS[@]}"; do
  if wait_until "$T_FAILOVER" "$t: recovery registered on leader $L1" recovered_on "$L1" "$t" "$(tenant_master "$t")" 1; then
    pass "$t: leader $L1 registered a recovery: $(recovered_rows $L1 $t | tr '\n' ';')"
  else
    fail "$t: leader $L1 registered no recovery" "$(node_logs $L1 | grep -i recover | tail -5)"
  fi
done
for t in "${MYS[@]}"; do
  wait_until "$T_FAILOVER" "$t: failover hook ran on leader" bash -c "[ \"\$(source $ORCMT_DIR/lib.sh; hook_lines $L1 $t)\" -gt 0 ]" \
    && pass "$t: hook executed on the leader $L1: $(hook_last $L1 $t)" || fail "$t: hook never ran on leader $L1"
done
for t in "${MYS[@]}"; do wait_until "$T_FAILOVER" "$(t_m2 $t) writable" mariadb_writable "$(t_m2 $t)" && pass "$t: $(t_m2 $t) promoted" || fail "$t: $(t_m2 $t) not promoted"; done
for t in "${PGS[@]}"; do wait_until "$T_FAILOVER" "$(t_m2 $t) promoted" pg_promoted "$(t_m2 $t)" && pass "$t: $(t_m2 $t) promoted" || fail "$t: $(t_m2 $t) not promoted"; done
log "settling ${T_SETTLE}s to give followers a chance to (wrongly) act"; sleep "$T_SETTLE"
for n in "${NODES[@]}"; do [ "$n" = "$L1" ] && continue
  for t in "${MYS[@]}"; do
    assert_eq "follower $n: no failover hook for $t" 0 "$(hook_lines $n $t)"
  done
  assert_eq "follower $n: never started a recovery" 0 "$(log_count $n 'RecoverDeadMaster: will recover|RecoverDeadPrimary|will handle Dead(Master|Primary) event')"
done
# NOTE: raft replicates topology_recovery to the followers, and each follower stamps the replicated row with ITS OWN
# hostname in processing_node_hostname (verified on the legacy binary) -> only the leader's backend is authoritative
# for "who ran it"; for followers the proofs are "no hook file" and "no recovery log line".
for t in "${TENANTS[@]}"; do
  assert_eq "$t: leader backend: every recovery row processed by $LC" 0 "$(sql $L1 "$(tenant_db "$t")" "select count(*) from topology_recovery where processing_node_hostname <> '$LC'")"
done
for t in "${MYS[@]}"; do
  assert_eq "$t: the failover hook ran exactly once on the leader (no repeated recovery)" 1 "$(hook_lines $L1 $t)"
done
for t in "${TENANTS[@]}"; do
  [ "$(log_count $L1 "will handle Dead(Master|Primary) event on $(tenant_master "$t"):")" -ge 1 ] && pass "$t: leader log shows it ran the recovery of $(tenant_master "$t")" || fail "$t: leader log has no recovery of $(tenant_master "$t")"
done

# ---------------------------------------------------------------------------------------------- re-attach
echo "== 3. old masters of a and b rejoin as replicas of the promoted ones =="
for t in "${MYS[@]}"; do
  mariadb_rejoin "$(t_m1 $t)" "$(t_m2 $t)" && pass "$t: $(t_m1 $t) is now a replica of $(t_m2 $t)" || fail "$t: $(t_m1 $t) could not rejoin as replica of $(t_m2 $t)"
  wait_until "$T_DISCOVERY" "leader $L1 sees $(t_m2 $t) master + $(t_m1 $t) replica" reintegrated "$L1" "$t" "$(ip_of $(t_m2 $t))" "$(ip_of $(t_m1 $t))" \
    && pass "$t: topology reintegrated on leader" || fail "$t: leader does not see $(t_m2 $t)->$(t_m1 $t)" "$(topology_rows $L1 "$(tenant_db $t)" | tr '\n' ';')"
done

# ---------------------------------------------------------------------------------------------- leader change
echo "== 4. leader $L1 dies: a different node takes over =="
declare -A old_hooks; for t in "${MYS[@]}"; do old_hooks[$t]="$(hook_lines $L1 $t)"; done
REST=(); for n in "${NODES[@]}"; do [ "$n" != "$L1" ] && REST+=("$n"); done
log "stopping $LC (the whole orchestrator process)"; docker stop -t 5 "$LC" >/dev/null
wait_until "$T_RAFT_ELECTION" "new single Leader among ${REST[*]}" bash -c "source $ORCMT_DIR/lib.sh; l=\$(raft_leader_node ${REST[*]}); [ -n \"\$l\" ] && [ \"\$l\" != $L1 ]" \
  || { for n in "${REST[@]}"; do echo "$n: $(raft_state $n)" >&2; done; die "no new leader"; }
L2="$(leader_now "${REST[@]}")"; F2="$(for n in "${REST[@]}"; do [ "$n" != "$L2" ] && echo $n; done)"
pass "new Leader: $L2 (was $L1); follower: $F2"
wait_until 30 "survivors agree on the new leader" follower_agree "$L2" "${REST[@]}" && pass "survivors report $(ip_of $L2)" || fail "survivors disagree on the leader"
for t in "${MYS[@]}"; do
  wait_until "$T_DISCOVERY" "new leader sees $(t_m2 $t)->$(t_m1 $t)" bash -c "source $ORCMT_DIR/lib.sh; reintegrated $L2 $t $(ip_of $(t_m2 $t)) $(ip_of $(t_m1 $t))" \
    && pass "$t: new leader $L2 sees the re-attached topology" || fail "$t: new leader $L2 does not see the current topology"
done

# ---------------------------------------------------------------------------------------------- failover 2
echo "== 5. new leader $L2 recovers BOTH tenants together =="
LC2="$(cname $L2)"
# the first recoveries must be out of their RecoveryPeriodBlockSeconds, or the second ones are (rightly) refused
wait_s=$((RECOVERY_BLOCK_SECONDS + 5 - (SECONDS - T_FO1))); [ "$wait_s" -gt 0 ] && { log "waiting ${wait_s}s for the recovery block period of the first failover to expire"; sleep "$wait_s"; }
log "stopping the new masters of ${MYS[*]} at once"
docker stop -t 2 $(for t in "${MYS[@]}"; do cname "$(t_m2 $t)"; done) >/dev/null
for t in "${MYS[@]}"; do
  m="$(tenant_replica "$t")"                # the promoted replica of step 2 is now the master: a2/b2 (tenant_replica == a2/b2)
  if wait_until "$T_FAILOVER" "$t: second recovery registered on $L2" recovered_on "$L2" "$t" "$m" 1; then
    pass "$t: new leader $L2 registered the recovery of $m: $(recovered_rows $L2 $t | tail -1)"
  else
    fail "$t: new leader $L2 registered no recovery of $m" "$(node_logs $L2 | grep -i recover | tail -5)"
  fi
  assert_eq "$t: the new recovery was processed by $LC2" "$LC2" "$(sql $L2 "$(tenant_db $t)" "select processing_node_hostname from topology_recovery where hostname='$m' order by recovery_id desc limit 1")"
  wait_until "$T_FAILOVER" "$t: hook ran on new leader" bash -c "[ \"\$(source $ORCMT_DIR/lib.sh; hook_lines $L2 $t)\" -gt 0 ]" \
    && pass "$t: hook executed on the NEW leader $L2: $(hook_last $L2 $t)" || fail "$t: hook never ran on the new leader"
done
for t in "${MYS[@]}"; do wait_until "$T_FAILOVER" "$(t_m1 $t) writable" mariadb_writable "$(t_m1 $t)" && pass "$t: $(t_m1 $t) promoted" || fail "$t: $(t_m1 $t) not promoted"; done
log "settling ${T_SETTLE}s"; sleep "$T_SETTLE"
for t in "${MYS[@]}"; do
  assert_eq "follower $F2: no failover hook for $t" 0 "$(hook_lines $F2 $t)"
  assert_eq "$t: the failover hook ran exactly once on the new leader" 1 "$(hook_lines $L2 $t)"
done
assert_eq "follower $F2: never started a recovery" 0 "$(log_count $F2 'RecoverDeadMaster: will recover|will handle Dead(Master|Primary) event')"
for t in "${MYS[@]}"; do
  assert_eq "dead ex-leader $L1: hook log of $t untouched" "${old_hooks[$t]}" "$(hook_lines $L1 $t)"
  assert_eq "$t: processed by exactly one node in the new term ($LC2)" 0 "$(sql $L2 "$(tenant_db $t)" "select count(*) from topology_recovery where hostname='$(tenant_replica "$t")' and processing_node_hostname <> '$LC2'")"
done
# (Other leader-only duties -- expire/forget/ack loops, KV master submission -- have no per-tenant observable
#  in this environment; recoveries + hooks are the evidence used.)

# ---------------------------------------------------------------------------------------------- snapshot
echo "== 6. raft snapshot is tenant-aware (format v2: gzip(JSON{FormatVersion:2, Tenants:{id:...}})) =="
api "$L2" /api/raft-snapshot >/dev/null
snap_json() { docker exec "$(cname $L2)" sh -c 'f=$(ls -t /data/raft/snapshots/*/state.bin 2>/dev/null | head -1); [ -n "$f" ] && gzip -dc "$f"'; }
snapshot_ok() { local j; j="$(snap_json)"; echo "$j" | grep -q '"FormatVersion":2' && for t in "${TENANTS[@]}"; do echo "$j" | grep -q "\"$t\":" || return 1; done; }
wait_until 30 "snapshot with FormatVersion 2 and every tenant" snapshot_ok \
  && pass "raft snapshot is v2 and carries a section for each of: ${TENANTS[*]}" || fail "raft snapshot lacks FormatVersion 2 / tenant sections" "$(snap_json | head -c 300)"

# ---------------------------------------------------------------------------------------------- MySQL specifics
ALIVE=("$L2" "$F2")
echo "== 6b. every database holds ONLY its own tenant's data (databases of all three nodes) =="
dbs="$(bk_root "show databases like 'orc\\_%'" | sort | tr '\n' ' ')"
assert_eq "the backend server holds exactly orc_<node>_control + orc_<node>_<tenant> for the 3 nodes" "$(expected_dbs "${NODES[@]}" | tr '\n' ' ')" "$dbs"
for n in "${ALIVE[@]}"; do
  for t in "${TENANTS[@]}"; do
    k="$(tenant_db "$t")"; oth="$(others_sql "$t")"
    assert_eq "$n/$t ($(be_db $n $t)): no database_instance of another tenant" 0 "$(sql $n "$k" "select count(*) from database_instance where hostname in ($oth)")"
    assert_eq "$n/$t: no audit of another tenant's instances" 0 "$(sql $n "$k" "select count(*) from audit where hostname in ($oth)")"
    assert_eq "$n/$t: no topology_recovery of another tenant" 0 "$(sql $n "$k" "select count(*) from topology_recovery where hostname in ($oth)")"
    assert_eq "$n/$t: no instance of another provider" 0 "$(sql $n "$k" "select count(*) from database_instance where provider_type <> '$(tenant_prov "$t")'")"
  done
  assert_eq "$n control ($(be_db $n control)): no tenant instances, no recoveries" 0 "$(sql $n control "select (select count(*) from database_instance)+(select count(*) from topology_recovery)")"
done
echo "== 6c. schema complete in every database; node_health mirrored per database (6d of the plan) =="
ref="$(tables_of $L2 control)"
[ "$(echo "$ref" | grep -c .)" -ge 20 ] && echo "$ref" | grep -qx orchestrator_db_deployments && pass "$(be_db $L2 control) has $(echo "$ref" | grep -c .) tables incl. orchestrator_db_deployments" || fail "control schema incomplete" "$ref"
for n in "${NODES[@]}"; do
  for k in control "${TENANTS[@]}"; do
    [ "$(tables_of $n "$(tenant_db "$k")")" = "$ref" ] && pass "$(be_db $n $k): same $(echo "$ref" | grep -c .) tables as the reference" || fail "$(be_db $n $k): table set differs from the reference"
    [ "$(sql $n "$(tenant_db "$k")" "select count(*) from orchestrator_db_deployments")" -ge 1 ] || fail "$(be_db $n $k): orchestrator_db_deployments empty"
  done
done
for n in "${ALIVE[@]}"; do
  for k in control "${TENANTS[@]}"; do
    assert_eq "$(be_db $n $k): node_health has a fresh heartbeat of $(cname $n)" 1 "$(sql $n "$(tenant_db "$k")" "select count(*) from node_health where hostname='$(cname $n)' and last_seen_active > now() - interval 60 second")"
  done
done
echo "== 6d. the recoveries of the failovers are only in the database of the tenant that failed over (leaders' backends) =="
for t in "${MYS[@]}"; do
  m="$(tenant_master "$t")"
  [ "$(sql $L1 "$(tenant_db "$t")" "select count(*) from topology_recovery" 2>/dev/null)" -ge 1 ] && pass "$t: $(be_db $L1 $t) (ex-leader's backend) has its recovery rows" || fail "$t: no recovery rows in $(be_db $L1 $t)"
done
for t in "${TENANTS[@]}"; do
  oth="$(others_sql "$t")"
  for n in "${NODES[@]}"; do
    assert_eq "$t on $n: no recovery_steps / detections of another tenant's hosts" 0 "$(sql $n "$(tenant_db "$t")" "select (select count(*) from topology_failure_detection where hostname in ($oth))+(select count(*) from topology_recovery where hostname in ($oth))")"
  done
done

tapi() { local n="$1" t="$2" p="$3"; shift 3; api "$n" "/api/tenants/$t/$p" "$@"; }
# ---- 6e. snapshot restore on MySQL backends
echo "== 6e. snapshot v2 restored over MySQL backends: a restarted node and a brand new node converge (table hashes per tenant) =="
tfp() { # NODE TENANT -> md5 of the tables that must be identical on every node (no timestamps / discovery data)
  local n="$1" t="$2" k; k="$(tenant_db "$t")"
  {
    echo tags;     sql "$n" "$k" "select hostname,port,tag_name,tag_value from database_instance_tags order by 1,2,3"
    echo downtime; sql "$n" "$k" "select hostname,port,owner,reason from database_instance_downtime where reason <> 'lost-in-recovery' order by 1,2"
    echo recovery; sql "$n" "$k" "select hostname,port,analysis,acknowledged,acknowledged_by from topology_recovery order by hostname,port,analysis,recovery_id"
    echo candid;   sql "$n" "$k" "select hostname,port from candidate_database_instance order by 1,2"
  } | md5sum | cut -c1-12
}
same_fp() { local t="$1" first="" n fp; shift; for n in "$@"; do fp="$(tfp "$n" "$t")"; [ -z "$first" ] && first="$fp"; [ "$fp" = "$first" ] || return 1; done; }
tspec() { local t spec=""; for t in "${TENANTS[@]}"; do spec="$spec${spec:+,}$t=$(tenant_master "$t"):$(tenant_port "$t")"; done; echo "$spec"; }
publish() { # PREFIX MAX LOGFILE : tag commands against the two live nodes
  python3 "$ORCMT_DIR/chaos-traffic.py" run --nodes "$(ip_of $L2),$(ip_of $F2)" --tenants "$(tspec)" --prefix "$1" --log "$3" --workers 6 --pace 0 --max "$2" >/dev/null 2>&1
}
log "publishing 150 tag commands + downtimes through Raft"
publish e1 150 "$ORCMT_WORK/e1.jsonl"
for t in "${TENANTS[@]}"; do tapi "$L2" "$t" "begin-downtime/$(tenant_master "$t")/$(tenant_port "$t")/mt/e1-test" >/dev/null; done
for t in "${TENANTS[@]}"; do
  wait_until 60 "$t: $L2 and $F2 databases converge" same_fp "$t" "$L2" "$F2" && pass "$t: $(be_db $L2 $t) and $(be_db $F2 $t) have the same table hashes ($(tfp $L2 $t))" || fail "$t: databases of $L2 and $F2 diverge" "$L2=$(tfp $L2 $t) $F2=$(tfp $F2 $t)"
  [ "$(sql $F2 "$(tenant_db "$t")" "select count(*) from database_instance_tags where tag_name like 'e1-%'")" -ge 40 ] && pass "$t: tags replicated into $(be_db $F2 $t)" || fail "$t: tags missing in the follower's database"
done
api "$L2" /api/raft-snapshot >/dev/null; api "$F2" /api/raft-snapshot >/dev/null
declare -A fp_before; for t in "${TENANTS[@]}"; do fp_before[$t]="$(tfp $F2 $t)"; done
log "restarting $F2 (it restores its latest snapshot into its populated MySQL databases at start)"
docker stop -t 10 "$(cname $F2)" >/dev/null; docker start "$(cname $F2)" >/dev/null
wait_until "$T_NODE_READY" "$F2 HTTP" node_http_ready "$F2" || { node_logs $F2 | tail -20 >&2; fail "$F2 did not restart"; }
wait_until "$T_RAFT_ELECTION" "cluster stable" bash -c "source $ORCMT_DIR/lib.sh; [ \"\$(raft_state $F2)\" = Follower ]" && pass "$F2 rejoined as Follower" || fail "$F2 did not rejoin"
rl="$(node_logs $F2 | grep -i -E 'restor' | head -3 | cut -c1-200)"
[ -n "$rl" ] && pass "restart: log shows a snapshot restore: $(echo "$rl" | head -1)" || fail "restart: no snapshot restore line in the log of $F2"
for t in "${TENANTS[@]}"; do
  wait_until 60 "$t: restarted $F2 converged" same_fp "$t" "$L2" "$F2" && pass "$t: restarted $F2 identical to the leader (hash $(tfp $F2 $t), was ${fp_before[$t]})" || fail "$t: $F2 diverges after the restart restore" "$L2=$(tfp $L2 $t) $F2=$(tfp $F2 $t)"
done
assert_eq "restart: no restore errors in the log of $F2" 0 "$(node_logs $F2 | grep -c -i -E 'restore.*(error|fail)|Error 1[0-9]{3}')"

if [ "$M2_NEWNODE_OPS" -gt 0 ]; then
  echo "== 6f. a brand new node ($L1: empty Raft dir and EMPTY databases) gets the snapshot installed by the leader =="
  log "publishing $M2_NEWNODE_OPS commands so that the Raft log is compacted (TrailingLogs=10240)"
  T1=$SECONDS; publish ne "$M2_NEWNODE_OPS" "$ORCMT_WORK/ne.jsonl"; log "published in $((SECONDS-T1))s: $(python3 "$ORCMT_DIR/chaos-traffic.py" stats --log "$ORCMT_WORK/ne.jsonl")"
  api "$L2" /api/raft-snapshot >/dev/null; api "$F2" /api/raft-snapshot >/dev/null
  rdb() { docker exec "$(cname $L2)" python3 -c "$_SQ_PY" /data/raft/raft_store.db "$1" 2>/dev/null; }
  first_ok() { [ "$(rdb 'select min(log_index) from raft_log')" -gt 1 ] 2>/dev/null; }
  wait_until 90 "leader log compacted" first_ok && pass "leader log compacted: first index $(rdb 'select min(log_index) from raft_log')" || fail "leader log not compacted (last index $(rdb 'select max(log_index) from raft_log'))"
  # wipe the dead node: volume AND its databases
  docker rm -f -v "$(cname $L1)" >/dev/null 2>&1; docker volume rm -f "$(cname $L1)-data" >/dev/null 2>&1
  for k in control "${TENANTS[@]}"; do bk_root "drop database if exists \`$(be_db $L1 $k)\`" ; done
  assert_eq "$L1's databases dropped (the new node starts empty)" 0 "$(bk_root "show databases like 'orc\\_${L1}\\_%'" | grep -c .)"
  node_start "$L1"
  wait_until "$T_NODE_READY" "$L1 HTTP" node_http_ready "$L1" || { node_logs $L1 | tail -30 >&2; fail "$L1 (new node) did not start"; }
  wait_until "$T_RAFT_ELECTION" "$L1 follows the leader" bash -c "source $ORCMT_DIR/lib.sh; [ \"\$(raft_state $L1)\" = Follower ]" && pass "new node $L1 joined as Follower" || fail "new node $L1 did not join"
  for t in "${TENANTS[@]}"; do
    wait_until 180 "$t: new $L1 converged" same_fp "$t" "$L2" "$F2" "$L1" && pass "$t: new node's $(be_db $L1 $t) identical to the other nodes (hash $(tfp $L1 $t))" || fail "$t: new node diverges" "$L2=$(tfp $L2 $t) $L1=$(tfp $L1 $t)"
    assert_eq "$t on $L1: tag count equals the leader's" "$(sql $L2 "$(tenant_db "$t")" "select count(*) from database_instance_tags")" "$(sql $L1 "$(tenant_db "$t")" "select count(*) from database_instance_tags")"
    assert_eq "$t on $L1: nothing of another tenant" 0 "$(sql $L1 "$(tenant_db "$t")" "select count(*) from database_instance_tags where tag_name like 'ne-%' and tag_name not like '%\\_$t'")"
  done
  node_logs "$L2" | grep -q "installing snapshot on.*$(ip_of $L1)" && pass "the leader installed a snapshot on $L1 (not a log replay)" || fail "no snapshot installation on $L1 in the leader log"
  [ "$(sql $L1 control "select count(*) from orchestrator_db_deployments")" -ge 1 ] && pass "new node: schema deployed by EnsureSchema on its fresh databases" || fail "new node: no schema"
  ALIVE=("$L2" "$F2" "$L1")
fi

echo "== 7. process health =="
for n in "$L2" "$F2"; do
  [ "$(docker inspect -f '{{.State.Running}} {{.RestartCount}}' "$(cname $n)")" = "true 0" ] && pass "$n still running, never restarted" || fail "$n died or restarted"
done
for n in "${NODES[@]}"; do
  bad="$(node_logs $n | grep -E -i 'panic|fatal|unknown tenant|goroutine [0-9]+ \[|nil pointer' | head -3)"
  [ -z "$bad" ] && pass "$n: log has no panic/fatal/'unknown tenant'" || fail "$n: suspicious log lines" "$bad"
done

log "total $((SECONDS-T0))s"
summary
