#!/usr/bin/env bash
# Scenario 1 -- PARTITION OF THE LEADER.
#   phase 0  full isolation of the leader (cut from Raft AND from the databases, so it sees every master dead):
#            the majority elects a new leader, the isolated node stops being Leader, nobody recovers anything.
#   phase 1  Raft-only partition of the (current) leader: it keeps reaching the databases. The masters of ALL THREE
#            tenants are stopped during the partition: ONLY the new leader recovers them (rows + hooks), the isolated
#            node never starts a recovery nor runs a hook. After healing the old leader is a follower and the three
#            tenants converge with no duplicated recoveries/hooks.
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "01-leader-partition"
chaos_setup a b c
nodes_fresh
log "leader=$LEADER followers=${FOLLOWERS[*]}"

# ------------------------------------------------------------------ phase 0
if [ "${S1_PHASE0:-1}" = 1 ]; then
echo "== phase 0: full isolation of the leader $LEADER (it sees all masters dead) ==" >&2
OLD="$LEADER"; REST=("${FOLLOWERS[@]}")
base_rec="$(node_rec_count "$OLD")"; base_nl="$(nonleader_lines "$OLD")"
partition_full "$OLD"
T=$SECONDS; while [ "$(rstate "$OLD")" = Leader ] && [ $((SECONDS-T)) -lt 20 ]; do sleep 0.3; done
[ "$(rstate "$OLD")" != Leader ] && pass "isolated old leader $OLD stopped being Leader after $((SECONDS-T))s (now $(rstate "$OLD"))" || fail "isolated node $OLD still claims to be Leader after 20s"
wait_until "$T_RAFT_ELECTION" "majority elects a leader" single_leader "${REST[@]}" && { NL="$(single_leader "${REST[@]}")"; pass "majority elected $NL"; } || { fail "no leader in the majority"; NL="${REST[0]}"; }
log "isolated for 10s more (it would see every master unreachable)"; sleep 10
assert_eq "isolated $OLD still not Leader" "false" "$([ "$(rstate "$OLD")" = Leader ] && echo true || echo false)"
for t in "${TENANTS[@]}"; do
  for n in "${NODES[@]}"; do assert_eq "phase0: no recovery row for $t on $n" 0 "$(sql "$n" "$(tdb "$t")" 'select count(*) from topology_recovery' 2>/dev/null || echo 0)"; done
done
assert_eq "phase0: no failover hook anywhere (mariadb-a)" 0 "$(hooks_total "$TENANT_A")"
assert_eq "phase0: no failover hook anywhere (mariadb-b)" 0 "$(hooks_total "$TENANT_B")"
assert_eq "phase0: no 'will handle' recovery on the isolated node" "$base_rec" "$(node_rec_count "$OLD")"
log "info: isolated node logged $(( $(nonleader_lines "$OLD") - base_nl )) 'NOT detecting/recovering (raft non-leader)' lines (a node outside the quorum is not even 'active': it does not poll nor analyse)"
heal_full "$OLD"
cluster_stable "${NODES[@]}" && pass "after healing the cluster has one Leader ($LEADER): $(states_line "${NODES[@]}")" || fail "cluster not stable after healing phase 0"
wait_until "$T_DISCOVERY" "$OLD sees every tenant again" chaos_all_tenants_on "$OLD" && pass "old leader $OLD discovers every tenant again" || fail "$OLD does not rediscover its tenants"
assert_eq "phase0: still no recovery/hook after healing" 0 "$(( $(hooks_total "$TENANT_A") + $(hooks_total "$TENANT_B") + $(hooks_total "$TENANT_C") ))"
fi

# ------------------------------------------------------------------ phase 1
echo "== phase 1: Raft-only partition of the leader $LEADER while the masters of all tenants fail ==" >&2
OLD="$LEADER"; REST=("${FOLLOWERS[@]}")
base_rec="$(node_rec_count "$OLD")"; base_nl="$(nonleader_lines "$OLD")"
partition_raft "$OLD"
T=$SECONDS; while [ "$(rstate "$OLD")" = Leader ] && [ $((SECONDS-T)) -lt 20 ]; do sleep 0.3; done
[ "$(rstate "$OLD")" != Leader ] && pass "isolated leader $OLD stepped down after $((SECONDS-T))s (now $(rstate "$OLD"))" || fail "isolated node $OLD still Leader after 20s"
wait_until "$T_RAFT_ELECTION" "majority elects a leader" single_leader "${REST[@]}" && NL="$(single_leader "${REST[@]}")" || { fail "no leader in the majority"; NL="${REST[0]}"; }
pass "new leader $NL (old: $OLD)"
for t in "${TENANTS[@]}"; do master_down "$t"; done
for t in "${TENANTS[@]}"; do
  wait_until "$T_FAILOVER" "$t: recovery on new leader $NL" rec_rows_at_least "$NL" "$t" "$(tenant_master "$t")" 1 \
    && pass "$t: new leader $NL registered the recovery of $(tenant_master "$t")" || fail "$t: new leader registered no recovery" "$(node_logs "$NL" | grep -i 'recover' | tail -3)"
  wait_until "$T_FAILOVER" "$t: hook on new leader" hook_at_least "$NL" "$t" 1 && pass "$t: failover hook ran on $NL: $(hook_file "$NL" "$t" | tail -1)" || fail "$t: hook never ran on $NL"
  wait_until "$T_FAILOVER" "$t: replica promoted" promoted "$t" && pass "$t: replica promoted" || fail "$t: replica not promoted"
done
log "settling ${T_SETTLE}s while still partitioned"; sleep "$T_SETTLE"
for t in "${TENANTS[@]}"; do
  assert_eq "$t: isolated $OLD registered no recovery of its own" 0 "$(sql "$OLD" "$(tdb "$t")" 'select count(*) from topology_recovery' 2>/dev/null || echo 0)"
  assert_eq "$t: isolated $OLD ran no failover hook" 0 "$(hook_lines "$OLD" "$t")"
  assert_eq "$t: exactly one hook run cluster-wide so far" 1 "$(hooks_total "$t")"
done
assert_eq "isolated $OLD: no 'will handle <master> event' (no recovery started)" "$base_rec" "$(node_rec_count "$OLD")"
nl_seen=$(( $(nonleader_lines "$OLD") - base_nl ))
log "info: isolated $OLD logged $nl_seen 'NOT detecting/recovering (raft non-leader)' lines (outside the quorum it is not 'active': it neither polls nor analyses)"
assert_eq "partitioned $OLD is not Leader" "false" "$([ "$(rstate "$OLD")" = Leader ] && echo true || echo false)"
heal_raft "$OLD"
cluster_stable "${NODES[@]}" && pass "healed: single Leader $LEADER: $(states_line "${NODES[@]}")" || fail "cluster not stable after healing"
[ "$LEADER" != "$OLD" ] && pass "the old leader $OLD rejoined as follower, the new leader $LEADER kept the term" || log "note: old leader $OLD became leader again"
for t in "${TENANTS[@]}"; do
  wait_until 90 "$t converged on ${NODES[*]}" converged "$t" "${NODES[@]}" && pass "$t: identical state on all 3 nodes (tags/downtime/recovery rows)" || fail "$t: nodes diverge" "$(for n in "${NODES[@]}"; do echo "$n=$(tenant_fp $n $t)"; done | tr '\n' ' ')"
  assert_eq "$t: recovery rows identical on old leader and leader" "$(rec_rows "$LEADER" "$t" "$(tenant_master "$t")")" "$(rec_rows "$OLD" "$t" "$(tenant_master "$t")")"
  assert_eq "$t: ONE recovery of $(tenant_master "$t") on the leader (no duplicates)" 1 "$(rec_rows "$LEADER" "$t" "$(tenant_master "$t")")"
  assert_eq "$t: failover hook ran exactly once cluster-wide" 1 "$(hooks_total "$t")"
  [ "$(sql "$OLD" "$(tdb "$t")" "select count(*) from database_instance where hostname='$(tenant_replica "$t")' and master_host=''" 2>/dev/null)" = 1 ] \
    && pass "$t: old leader sees $(tenant_replica "$t") as the new master" || log "info: old leader $OLD has not (yet) re-discovered the new master of $t"
done
sleep "$T_SETTLE"
assert_eq "hooks: none run by a node that was not Leader at that moment" "" "$(for t in "${TENANTS[@]}"; do hooks_non_leader "$t"; done)"
for t in "${TENANTS[@]}"; do assert_eq "$t: still one hook run after settling" 1 "$(hooks_total "$t")"; done
assert_clean_logs "end" "${NODES[@]}"
scenario_verdict; summary
