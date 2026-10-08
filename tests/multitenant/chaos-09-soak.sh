#!/usr/bin/env bash
# Scenario 9 -- SOAK (only with CHAOS_SOAK=1): seeded random chaos for CHAOS_SOAK_SECONDS (default 600).
# Each cycle injects ONE node-level fault (kill -9 + restart, pause, Raft partition, full isolation, graceful restart),
# sometimes together with the crash of a tenant master (mariadb-a / mariadb-b; postgres-c at most once), waits for the
# cluster to heal and checks the invariants:
#   I1 never two Leaders for two consecutive samples 1.5s apart (a stepping-down old leader may show for an instant)
#   I2 no panic/fatal/'unknown tenant'/stack trace in any node log
#   I3 no recovery hook ran on a node whose Raft state was not Leader at that moment (hook.sh records it)
#   I4 the failover of a crashed master completes (the replica is promoted) and the old master can rejoin
#   I5 final: one Leader, the three nodes identical per tenant (tags/downtime/recovery rows), traffic never lost
# Reproduce a run with CHAOS_SEED=<n> (printed at start).
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "09-soak"
[ "${CHAOS_SOAK:-0}" = 1 ] || { echo "SKIP: set CHAOS_SOAK=1 to run the soak" >&2; exit 0; }
SOAK_SECONDS="${CHAOS_SOAK_SECONDS:-600}"
SEED="${CHAOS_SEED:-$((RANDOM*32768+RANDOM))}"; RANDOM=$SEED
export RECOVERY_BLOCK_SECONDS="${SOAK_BLOCK_SECONDS:-45}"
log "SOAK seed=$SEED duration=${SOAK_SECONDS}s RecoveryPeriodBlockSeconds=$RECOVERY_BLOCK_SECONDS"
chaos_setup a b c
nodes_fresh
traffic_start soak "$ORCMT_WORK/traffic-9.jsonl" 2 0.1
VIOL=(); declare -A LASTFO; ACTIONS=(); CDONE=0
CUR_M_a=a1; CUR_R_a=a2; CUR_M_b=b1; CUR_R_b=b2
violation() { VIOL+=("$1"); fail "VIOLATION: $1"; }
badcount_prev=0
# I1: sample the leaders twice, 1.5s apart
sample_leaders() {
  local l1 l2; l1="$(leaders_among "${NODES[@]}" | grep -c .)"; sleep 1.5; l2="$(leaders_among "${NODES[@]}" | grep -c .)"
  [ "$l1" -gt 1 ] && [ "$l2" -gt 1 ] && violation "two Leaders for >1.5s: $(states_line "${NODES[@]}")"
  return 0
}
check_logs() { local n b; for n in "${NODES[@]}"; do b="$(bad_log_lines "$n")"; [ -n "$b" ] && violation "bad log lines on $n: $(echo "$b" | head -2 | cut -c1-200)"; done; return 0; }
check_hooks() { local t b; for t in "${TENANTS[@]}"; do b="$(hooks_non_leader "$t")"; [ -n "$b" ] && violation "hook of $t ran on a non-leader: $b"; done; return 0; }
fail_master() { # a|b|c
  local l="$1" m r
  case "$l" in a) m=$CUR_M_a; r=$CUR_R_a;; b) m=$CUR_M_b; r=$CUR_R_b;; c) m=c1; r=c2;; esac
  local since=$(( SECONDS - ${LASTFO[$l]:-0} ))
  [ -n "${LASTFO[$l]:-}" ] && [ "$since" -lt $((RECOVERY_BLOCK_SECONDS+10)) ] && return 1
  log "SOAK: crash of master $m (tenant $l)"; docker stop -t 2 "$(cname $m)" >/dev/null; LASTFO[$l]=$SECONDS
  echo "$l $m $r" > "$ORCMT_WORK/soak-pending-$l"
}
settle_master() { # a|b|c : wait for the promotion and re-attach the old master (mariadb)
  local l="$1" m r; [ -f "$ORCMT_WORK/soak-pending-$l" ] || return 0; read -r _ m r < "$ORCMT_WORK/soak-pending-$l"
  local ok=0
  if [ "$l" = c ]; then wait_until 240 "c: $r promoted" pg_promoted "$r" && ok=1
  else wait_until 240 "$l: $r writable" mariadb_writable "$r" && ok=1; fi
  if [ $ok = 1 ]; then
    if [ "$l" != c ]; then
      mariadb_rejoin "$m" "$r" >/dev/null 2>&1 && log "SOAK: $m re-attached under $r" || violation "$m could not rejoin under $r"
      if [ "$l" = a ]; then CUR_M_a=$r; CUR_R_a=$m; else CUR_M_b=$r; CUR_R_b=$m; fi
    else CDONE=1; fi
  else violation "failover of tenant $l (master $m) did not complete within 240s"; fi
  rm -f "$ORCMT_WORK/soak-pending-$l"
}
T_END=$((SECONDS+SOAK_SECONDS)); cycle=0
while [ $SECONDS -lt $T_END ]; do
  cycle=$((cycle+1))
  node="${NODES[$((RANDOM%3))]}"; kind=$((RANDOM%5)); dur=$((8+RANDOM%15)); mf=$((RANDOM%100))
  tenant_fail=""; if [ $mf -lt 40 ]; then case $((RANDOM%5)) in 0|1) tenant_fail=a;; 2|3) tenant_fail=b;; 4) [ $CDONE = 0 ] && tenant_fail=c;; esac; fi
  [ -n "$tenant_fail" ] && ! fail_master "$tenant_fail" && tenant_fail=""
  case $kind in
    0) act="kill9+restart $node (${dur}s)"; ACTIONS+=("$act"); log "SOAK cycle $cycle: $act master-failure=${tenant_fail:-none}"; node_kill9 "$node"; sleep "$dur"; node_up "$node";;
    1) act="pause $node (${dur}s)"; ACTIONS+=("$act"); log "SOAK cycle $cycle: $act master-failure=${tenant_fail:-none}"; node_pause "$node"; sleep "$dur"; sample_leaders; node_unpause "$node";;
    2) act="raft-partition $node (${dur}s)"; ACTIONS+=("$act"); log "SOAK cycle $cycle: $act master-failure=${tenant_fail:-none}"; partition_raft "$node"; sleep 2; sample_leaders; sleep "$dur"; sample_leaders; heal_raft "$node";;
    3) act="full-isolation $node (${dur}s)"; ACTIONS+=("$act"); log "SOAK cycle $cycle: $act master-failure=${tenant_fail:-none}"; partition_full "$node"; sleep 2; sample_leaders; sleep "$dur"; heal_full "$node";;
    4) act="graceful restart $node"; ACTIONS+=("$act"); log "SOAK cycle $cycle: $act master-failure=${tenant_fail:-none}"; node_stop "$node" 15; sleep 3; node_up "$node";;
  esac
  wait_until "$T_NODE_READY" "$node HTTP" node_http_ready "$node" || violation "$node HTTP did not come back after: $act"
  cluster_stable "${NODES[@]}" || violation "no stable cluster after: $act ($(states_line "${NODES[@]}"))"
  sample_leaders
  [ -n "$tenant_fail" ] && settle_master "$tenant_fail"
  check_logs; check_hooks
  sleep 3
done
log "SOAK done after $cycle cycles; final healing and convergence"
traffic_stop; traffic_stats >&2
for n in "${NODES[@]}"; do node_usable "$n" || { node_paused "$n" && node_unpause "$n"; node_running "$n" || node_up "$n"; }; done
cluster_stable "${NODES[@]}" && pass "final: single Leader $LEADER ($(states_line "${NODES[@]}"))" || violation "no stable cluster at the end"
for t in "${TENANTS[@]}"; do
  wait_until 120 "$t converged" converged "$t" "${NODES[@]}" && pass "final: $t identical on the three nodes" || violation "tenant $t diverges at the end: $(for n in "${NODES[@]}"; do echo -n "$n=$(tenant_fp $n $t) "; done)"
done
check_logs; check_hooks
log "hook runs per tenant (cluster-wide): $(for t in "${TENANTS[@]}"; do echo -n "$t=$(hooks_total $t) "; done)"
log "actions ($cycle): $(printf '%s; ' "${ACTIONS[@]}")"
[ ${#VIOL[@]} -eq 0 ] && pass "soak (seed $SEED, ${cycle} cycles): no invariant violated" || fail "soak (seed $SEED): ${#VIOL[@]} violations" "$(printf '%s\n' "${VIOL[@]}" | head -10)"
scenario_verdict; summary
