#!/usr/bin/env bash
# Scenario 4 -- kill -9 OF THE LEADER IN THE MIDDLE OF A FAILOVER.
# Tenant a's PreFailoverProcesses hook sleeps 20s so that the recovery is "in flight" for a while: the leader registered
# the recovery (row replicated through Raft: in_active_period=1, no end_recovery), is running the pre hook, and is killed.
# What does the new leader do with that orphan recovery? Repeat it? Leave it half done? This scenario MEASURES it:
# time until the topology is repaired, pre/post hook runs (1 vs 2), rows of topology_recovery, state of the replicas.
# Hard assertions: the failover completes under the new leader, the post hook runs once and only on a Leader, nothing
# panics, the killed node rejoins and converges. RecoveryPeriodBlockSeconds (anti-flapping) is reported, not asserted.
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "04-kill-leader-mid-failover"
export CHAOS_PRE_SLEEP_A=20
export RECOVERY_BLOCK_SECONDS="${S4_BLOCK_SECONDS:-90}"
S4_WAIT="${S4_WAIT:-240}"
chaos_setup a b c
nodes_fresh
OLD="$LEADER"; REST=("${FOLLOWERS[@]}")
log "leader=$OLD RecoveryPeriodBlockSeconds=$RECOVERY_BLOCK_SECONDS pre-hook sleep=${CHAOS_PRE_SLEEP_A}s"
rows() { sql "$1" "$(tdb "$TENANT_A")" "select recovery_id,hostname,ifnull(successor_hostname,'-'),processing_node_hostname,ifnull(end_recovery,'-'),is_successful,acknowledged,in_active_period from topology_recovery order by recovery_id" | tr '\n' ';'; }
master_down "$TENANT_A"; TF=$SECONDS
pre_started() { hook_file "$OLD" "$TENANT_A" pre | grep -q 'ev=start'; }
wait_until "$T_FAILOVER" "recovery of a in flight on $OLD (pre hook started)" pre_started && pass "recovery of a is in flight on $OLD" || { fail "recovery never started on $OLD"; scenario_verdict; summary; exit 1; }
wait_until 20 "recovery row of $OLD replicated to followers" rec_rows_at_least "${REST[0]}" "$TENANT_A" "$(tenant_master "$TENANT_A")" 1 && pass "the in-flight recovery row is already replicated to ${REST[0]}" || log "info: row not replicated to ${REST[0]} before the kill"
node_kill9 "$OLD"; TK=$SECONDS
wait_until "$T_RAFT_ELECTION" "new leader among ${REST[*]}" single_leader "${REST[@]}" && NL="$(single_leader "${REST[@]}")" || { fail "no new leader"; NL="${REST[0]}"; }
pass "new leader $NL elected $((SECONDS-TK))s after the kill"

log "observing up to ${S4_WAIT}s (sampling every 10s): timeline of the orphan recovery"
done_at=""
while [ $((SECONDS-TK)) -lt "$S4_WAIT" ]; do
  sleep 10
  pre_n="$(hook_file "$NL" "$TENANT_A" pre | grep -c 'ev=start')"; post_n="$(hook_lines "$NL" "$TENANT_A")"
  log "t+$((SECONDS-TK))s: $NL pre-hook starts=$pre_n post-hooks=$post_n a2-writable=$(mariadb_writable a2 && echo yes || echo no) rows[$NL]: $(rows "$NL" | cut -c1-200)"
  if mariadb_writable a2 && [ "$post_n" -ge 1 ]; then done_at=$((SECONDS-TK)); break; fi
done
if [ -n "$done_at" ]; then pass "failover completed by the new leader $NL ${done_at}s after the leader was killed (${RECOVERY_BLOCK_SECONDS}s anti-flapping block configured)"; else fail "failover NOT completed within ${S4_WAIT}s after the kill" "$(node_logs "$NL" | grep -E 'AttemptRecoveryRegistration|will handle|recent' | tail -4 | cut -c1-250)"; fi
sleep 25
log "---- evidence ----"
for n in "${REST[@]}"; do echo "[$n pre ]" >&2; hook_file "$n" "$TENANT_A" pre | cut -c1-170 >&2; echo "[$n post]" >&2; hook_file "$n" "$TENANT_A" | cut -c1-170 >&2; echo "[$n rows] $(rows "$n")" >&2; done
echo "[$OLD (killed) pre]" >&2; hook_file "$OLD" "$TENANT_A" pre | cut -c1-170 >&2
log "log hints on $NL: $(node_logs "$NL" | grep -c 'has recently experienced a failover\|recently been promoted') 'blocked by active recovery' lines; 'will handle' lines: $(node_rec_count "$NL")"
posts="$(hooks_total "$TENANT_A")"
assert_eq "post-failover hook ran exactly once cluster-wide (hook runs: 1 = no repeated recovery)" 1 "$posts"
assert_eq "no post hook ran on a node that was not Leader at that moment" "" "$(hooks_non_leader "$TENANT_A")"
pre_total=$(( $(hook_file "$OLD" "$TENANT_A" pre | grep -c 'ev=start') + $(hook_file "${REST[0]}" "$TENANT_A" pre | grep -c 'ev=start') + $(hook_file "${REST[1]}" "$TENANT_A" pre | grep -c 'ev=start') ))
log "RESULT-INFO: pre-failover hook started $pre_total times in total (1 = only the killed leader's; 2 = the new leader repeated the recovery from scratch)"
mariadb_writable a2 && pass "a2 is the writable master at the end" || fail "a2 not promoted"
for t in "$TENANT_B" "$TENANT_C"; do assert_eq "$t untouched by the failure of a: no recovery rows on $NL" 0 "$(sql "$NL" "$(tdb "$t")" 'select count(*) from topology_recovery')"; done

# the killed node comes back
node_up "$OLD"
wait_until "$T_NODE_READY" "$OLD HTTP" node_http_ready "$OLD" || fail "$OLD did not come back"
cluster_stable "${NODES[@]}" && pass "killed node rejoined: $(states_line "${NODES[@]}")" || fail "cluster not stable after restarting $OLD"
wait_until 60 "a converged on ${NODES[*]}" converged "$TENANT_A" "${NODES[@]}" && pass "tenant a converged on all 3 nodes" || fail "tenant a diverges" "$(for n in "${NODES[@]}"; do echo "$n=$(tenant_fp $n $TENANT_A) rows: $(rows $n)"; echo "   $n active-period columns (id,start_active_period,in_active_period,end_active_period_unixtime): $(sql $n "$(tdb $TENANT_A)" "select recovery_id,start_active_period,in_active_period,end_active_period_unixtime from topology_recovery order by recovery_id" | tr '\n' ';')"; echo "   $n raft applies of write-recovery: $(node_logs $n | grep 'op=write-recovery$' | sed 's/ DEBUG.*applying command/ cmd/' | cut -c1-60 | tr '\n' ';')"; done)"
sleep 10
assert_eq "restarting the killed leader did not re-run any hook" "$posts" "$(hooks_total "$TENANT_A")"
assert_clean_logs "end" "${NODES[@]}"
scenario_verdict; summary
