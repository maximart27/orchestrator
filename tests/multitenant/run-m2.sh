#!/usr/bin/env bash
# Milestone 2 acceptance: THREE orchestrator nodes (orcmt-n1..n3) in ONE Raft group, each running the same THREE
# tenants (mariadb-a, mariadb-b, postgres-c). Proves that leader-only work follows the single Raft leader for ALL
# tenants at once, and that a new leader takes over all tenants together.
#
#   1. one Leader, two Followers agreeing on who it is; the leader discovers every tenant
#   2. stop the masters of mariadb-a, mariadb-b and postgres-c at once -> the LEADER (and only it) recovers all three
#      (topology_recovery.processing_node_hostname, node-local failover-hook files, recovery log lines)
#   3. re-attach the old masters of a and b as replicas of the promoted ones
#   4. stop the leader process -> new election -> a different node is Leader
#   5. stop the new masters of a and b at once -> the NEW leader recovers BOTH tenants together; the remaining
#      follower and the dead ex-leader do nothing
#
# Evidence is node-local on purpose (hook files in each node's /data volume, processing_node_hostname, logs),
# because raft replicates topology_recovery rows to followers' backends: a row existing on a follower proves nothing.
#
# Needs the multi-tenant runtime + tenant-aware raft. Parameters (times, names): see lib.sh.
#   ORCMT_KEEP=1 run-m2.sh   leave everything running for debugging (then ./cleanup.sh)
set -o pipefail
source "$(dirname "$0")/lib.sh"
orcmt_trap_cleanup
NODES=(n1 n2 n3)
T0=$SECONDS
# Self-test knobs (see gen-config.sh legacy-raft3): run the same flow with fewer tenants / today's legacy binary.
M2_PROFILE="${M2_PROFILE:-m2}"
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
[ -x "$ORCMT_WORK/bin/orchestrator" ] && [ -x "$ORCMT_WORK/bin/validate-tenants" ] && [ "${ORCMT_REBUILD:-0}" != 1 ] || build_orchestrator
"$ORCMT_DIR/topology.sh" up $(for t in "${TENANTS[@]}"; do tenant_letter "$t"; done) || die "topologies failed"
"$ORCMT_DIR/gen-config.sh" "$M2_PROFILE" >/dev/null || die "gen-config failed"
if [ "$M2_PROFILE" = m2 ]; then
  for n in "${NODES[@]}"; do
    "$ORCMT_DIR/validate-config.sh" $n 2>&1 | grep -c '^tenant=' | grep -qx 3 || die "tenants manifest of $n rejected by config.LoadTenantConfigs"
  done
fi
log "3 nodes x ${#TENANTS[@]} tenants (profile $M2_PROFILE): config accepted by the real loader; infrastructure ready after $((SECONDS-T0))s"
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
