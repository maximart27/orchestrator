#!/usr/bin/env bash
# Scenario 3 -- PAUSE OF THE LEADER (docker pause = GC pause / VM freeze: the process is alive but frozen, its sockets
# stay open, and it wakes up believing nothing happened).
#   3a  the leader is frozen for ~25s while the masters of tenants a and c fail: the new leader recovers them; when the old
#       leader wakes up it must find out it is no longer the leader WITHOUT running a recovery or a hook with its stale view.
#   3b  the leader is frozen (24s) in the middle of a recovery (tenant a's PreFailoverProcesses hook sleeps 20s): when it
#       wakes up, does the zombie leader carry on with its in-flight recovery (promotion + post hooks) although it lost the
#       term? Recoveries check leadership once, when they start (executeCheckAndRecoverFunction); this phase measures what
#       that means in practice.
#   3c  (only with CHAOS_LONG=1 or CHAOS_SOAK=1) the leader is frozen for 60s (> fatalAfterUnhealthyDuration = 30s): on wake-up it must not act as leader; what the
#       process does (upstream: log.Fatal "node is unable to register health" and exit, a supervisor restarts it) is recorded.
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "03-leader-pause"
chaos_setup a b c

# ---------------------------------------------------------------------------------------------------- 3a
echo "== 3a: leader frozen while two masters fail ==" >&2
nodes_fresh
OLD="$LEADER"; REST=("${FOLLOWERS[@]}")
log "leader=$OLD"
base_rec="$(node_rec_count "$OLD")"
node_pause "$OLD"; TP=$SECONDS
wait_until "$T_RAFT_ELECTION" "new leader among ${REST[*]}" single_leader "${REST[@]}" && NL="$(single_leader "${REST[@]}")" || { fail "no new leader while $OLD is frozen"; NL="${REST[0]}"; }
pass "new leader $NL elected while $OLD is frozen ($((SECONDS-TP))s after the freeze)"
master_down "$TENANT_A"; master_down "$TENANT_C"
for t in "$TENANT_A" "$TENANT_C"; do
  wait_until "$T_FAILOVER" "$t: recovery on $NL" rec_rows_at_least "$NL" "$t" "$(tenant_master "$t")" 1 && pass "$t: $NL registered the recovery" || fail "$t: no recovery on $NL"
  wait_until "$T_FAILOVER" "$t: hook on $NL" hook_at_least "$NL" "$t" 1 && pass "$t: hook on $NL: $(hook_file "$NL" "$t" | tail -1)" || fail "$t: no hook on $NL"
  wait_until "$T_FAILOVER" "$t: promoted" promoted "$t" || fail "$t: not promoted"
done
while [ $((SECONDS-TP)) -lt 20 ]; do sleep 1; done
node_unpause "$OLD"; TU=$SECONDS
# how long does the woken-up node keep believing it is the leader?
first_state=""; while [ $((SECONDS-TU)) -lt 20 ]; do s="$(rstate "$OLD")"; [ -z "$first_state" ] && first_state="$s"; [ "$s" = Follower ] && break; sleep 0.2; done
log "first state reported by $OLD after unpause: $first_state; Follower after $((SECONDS-TU))s"
assert_eq "woken-up $OLD is a Follower" Follower "$(rstate "$OLD")"
[ "$first_state" = Leader ] && log "NOTE: $OLD still answered Leader right after waking up (window of stale leadership) -- see recovery/hook assertions below"
sleep "$T_SETTLE"
for t in "$TENANT_A" "$TENANT_C"; do
  assert_eq "$t: woken-up $OLD ran no failover hook" 0 "$(hook_lines "$OLD" "$t")"
  assert_eq "$t: exactly one hook run cluster-wide" 1 "$(hooks_total "$t")"
done
assert_eq "woken-up $OLD started no recovery ('will handle ... event')" "$base_rec" "$(node_rec_count "$OLD")"
assert_eq "hooks run by a node that was not Leader at that moment" "" "$(for t in "$TENANT_A" "$TENANT_C"; do hooks_non_leader "$t"; done)"
cluster_stable "${NODES[@]}" && pass "single Leader $LEADER after the wake-up: $(states_line "${NODES[@]}")" || fail "cluster not stable after the wake-up"
[ "$LEADER" = "$NL" ] && pass "leadership stayed with $NL (term not stolen by the woken-up node)" || fail "leader changed back to $LEADER"
for t in "${TENANTS[@]}"; do
  wait_until 45 "$t converged" converged "$t" "${NODES[@]}" && pass "$t: identical state on all 3 nodes" || fail "$t: nodes diverge" "$(for n in "${NODES[@]}"; do echo "$n=$(tenant_fp $n $t)"; done | tr '\n' ' ')"
done
log "evidence (raft lines of $OLD after waking up):"; node_logs "$OLD" | grep -E "raft: .*(entering|heartbeat|lost leadership|failed to|stepping|rejecting|term)" | tail -6 | cut -c1-200 >&2
assert_clean_logs "3a" "${NODES[@]}"
chaos_save_logs "03a-leader-pause"
for n in "${NODES[@]}"; do docker rm -f -v "$(cname $n)" >/dev/null 2>&1; node_remove_volume "$n"; done

# ---------------------------------------------------------------------------------------------------- 3b
echo "== 3b: leader frozen (24s) in the middle of a recovery (pre-failover hook sleeping 20s) ==" >&2
topo_ensure a b c
export CHAOS_PRE_SLEEP_A=20
nodes_fresh
OLD="$LEADER"; REST=("${FOLLOWERS[@]}")
log "leader=$OLD"
master_down "$TENANT_A"
pre_started() { hook_file "$OLD" "$TENANT_A" pre | grep -q 'ev=start'; }
wait_until "$T_FAILOVER" "pre-failover hook of a started on $OLD" pre_started && pass "recovery of a is in flight on $OLD (pre hook sleeping)" || fail "recovery of a never started on $OLD"
node_pause "$OLD"; TP=$SECONDS
wait_until "$T_RAFT_ELECTION" "new leader among ${REST[*]}" single_leader "${REST[@]}" && NL="$(single_leader "${REST[@]}")" || { fail "no new leader"; NL="${REST[0]}"; }
pass "new leader $NL elected while $OLD is frozen mid-recovery"
while [ $((SECONDS-TP)) -lt 24 ]; do sleep 1; done
node_unpause "$OLD"; TU=$SECONDS
log "woken up; observing until the failover completes (max 200s)"
done_at=""
while [ $((SECONDS-TU)) -lt 200 ]; do
  sleep 10
  log "t+$((SECONDS-TU))s: states: $(states_line "${NODES[@]}") a2-writable=$(mariadb_writable a2 && echo yes || echo no) post-hooks=$(hooks_by_node "$TENANT_A")"
  mariadb_writable a2 && [ "$(hooks_total "$TENANT_A")" -ge 1 ] && { done_at=$((SECONDS-TU)); break; }
done
[ -n "$done_at" ] && pass "the failover of a completed ${done_at}s after the wake-up" || fail "the failover of a never completed (200s after the wake-up)"
sleep 10
log "---- hook evidence (pre/post, every node) ----"
for n in "${NODES[@]}"; do echo "[$n pre ]" >&2; hook_file "$n" "$TENANT_A" pre | cut -c1-170 >&2; echo "[$n post]" >&2; hook_file "$n" "$TENANT_A" | cut -c1-170 >&2; done
log "---- recovery rows ($TENANT_A) per node: id,failed,successor,processing node,successful,acknowledged ----"
for n in "${NODES[@]}"; do echo "[$n] $(sql "$n" "$(tdb "$TENANT_A")" "select recovery_id,hostname,ifnull(successor_hostname,'-'),processing_node_hostname,is_successful,acknowledged from topology_recovery order by recovery_id" | tr '\n' ';')" >&2; done
log "state of the woken-up node: $(node_running "$OLD" && echo running || echo "STOPPED (exit code $(docker inspect -f '{{.State.ExitCode}}' "$(cname $OLD)"))") ; $(node_logs "$OLD" | grep -c 'FATAL') FATAL lines"
posts="$(hooks_total "$TENANT_A")"
assert_eq "3b: failover post-hooks run cluster-wide (one failover => one)" 1 "$posts"
assert_eq "3b: no post hook ran on a node that was not Leader at that moment (a zombie leader must not finish a recovery)" "" "$(hooks_non_leader "$TENANT_A")"
promoted "$TENANT_A" && pass "3b: a2 promoted" || fail "3b: a2 not promoted"
b="$(for n in "${NODES[@]}"; do node_logs "$n" | grep -E -i 'panic|goroutine [0-9]+ \[|nil pointer'; done | head -3)"; [ -z "$b" ] && pass "3b: no panic" || fail "3b: panic" "$b"
chaos_save_logs "03b-leader-pause-midrecovery"
for n in "${NODES[@]}"; do docker rm -f -v "$(cname $n)" >/dev/null 2>&1; node_remove_volume "$n"; done

# ---------------------------------------------------------------------------------------------------- 3c
if [ "${CHAOS_LONG:-0}" != 1 ] && [ "${CHAOS_SOAK:-0}" != 1 ]; then log "3c skipped (set CHAOS_LONG=1 to run the 60s freeze)"; scenario_verdict; summary; exit; fi
echo "== 3c: leader frozen for 60s (twice the 30s health-fatal threshold) ==" >&2
topo_ensure a b c
unset CHAOS_PRE_SLEEP_A
nodes_fresh
OLD="$LEADER"; REST=("${FOLLOWERS[@]}")
node_pause "$OLD"; TP=$SECONDS
wait_until "$T_RAFT_ELECTION" "new leader among ${REST[*]}" single_leader "${REST[@]}" && NL="$(single_leader "${REST[@]}")" || { fail "no new leader"; NL="${REST[0]}"; }
while [ $((SECONDS-TP)) -lt 60 ]; do sleep 1; done
node_unpause "$OLD"
sleep 8
if node_running "$OLD"; then log "RESULT-INFO 3c: $OLD survived a 60s freeze (state $(rstate "$OLD"))"; else log "RESULT-INFO 3c: $OLD EXITED after a 60s freeze (exit code $(docker inspect -f '{{.State.ExitCode}}' "$(cname $OLD)")): $(node_logs "$OLD" | grep FATAL | tail -1 | cut -c1-200)"; fi
assert_eq "3c: the woken-up node never acted as leader (no recovery/hook)" 0 "$(node_rec_count "$OLD")"
assert_eq "3c: leader stayed $NL" "$NL" "$(single_leader "${REST[@]}")"
node_running "$OLD" || { node_up "$OLD"; wait_until "$T_NODE_READY" "$OLD HTTP" node_http_ready "$OLD"; }
cluster_stable "${NODES[@]}" && pass "3c: after (re)starting $OLD the cluster is stable: $(states_line "${NODES[@]}")" || fail "3c: cluster not stable"
for t in "${TENANTS[@]}"; do wait_until 45 "$t converged" converged "$t" "${NODES[@]}" && pass "3c: $t identical on the three nodes" || fail "3c: $t diverges"; done
scenario_verdict; summary
