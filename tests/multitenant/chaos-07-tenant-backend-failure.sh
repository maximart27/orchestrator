#!/usr/bin/env bash
# Scenario 7 -- THE BACKEND OF ONE TENANT FAILS ON THE LEADER.
# Realistic fault: the SQLite backend of tenant mariadb-b lives on a small volume (6 MB tmpfs mounted at /bk on every
# node) that fills up ("database or disk is full") on the LEADER only. Expected (docs/multi-tenant.md "Health of a tenant"):
# the leader yields after 5s ("yielding leadership: backend of tenant mariadb-b unhealthy"), another node takes ALL the
# tenants (the masters of the three tenants are stopped to prove it), the affected process does NOT die, and once the
# space is back it is healthy again. Then the commands it could not apply meanwhile are checked.
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "07-tenant-backend-failure"
export CHAOS_TMPFS_B=1
chaos_setup a b c
nodes_fresh
OLD="$LEADER"; REST=("${FOLLOWERS[@]}")
started_before="$(docker inspect -f '{{.State.StartedAt}}' "$(cname $OLD)")"
log "leader=$OLD; db of b on it: $(docker exec "$(cname $OLD)" sh -c 'ls -l /bk; df -k /bk | tail -1')"
hb_fresh() { [ "$(sql "$1" "$(tdb "$TENANT_B")" "select count(*) from node_health where last_seen_active > datetime('now','-8 seconds')" 2>/dev/null)" -ge 1 ] 2>/dev/null; }
hb_fresh "$OLD" && pass "before the fault: the heartbeat reaches tenant b's backend on $OLD" || fail "no fresh heartbeat in tenant b's backend before the fault"

# ---- fault: fill the volume of tenant b on the leader
docker exec "$(cname $OLD)" sh -c 'dd if=/dev/zero of=/bk/fill bs=64k 2>&1 | tail -1' >&2
log "volume of b on $OLD: $(docker exec "$(cname $OLD)" sh -c 'df -k /bk | tail -1')"
TF=$SECONDS
yielded() { [ "$(rstate "$OLD")" != Leader ]; }
wait_until 60 "$OLD gives up the leadership" yielded && pass "the leader $OLD yielded $((SECONDS-TF))s after its tenant backend broke" || fail "leader $OLD never yielded"
wait_until "$T_RAFT_ELECTION" "another node takes over" single_leader "${REST[@]}" && NL="$(single_leader "${REST[@]}")" || { fail "no new leader"; NL="${REST[0]}"; }
pass "new leader $NL; old leader $OLD is now $(rstate "$OLD")"
node_logs "$OLD" | grep -q 'yielding leadership: backend of tenant mariadb-b' && pass "log line present: $(node_logs "$OLD" | grep 'yielding leadership' | head -1 | cut -c1-180)" || fail "no 'yielding leadership: backend of tenant mariadb-b' line in $OLD's log"
assert_eq "$OLD process alive, not restarted" "$started_before" "$(docker inspect -f '{{.State.StartedAt}}' "$(cname $OLD)")"
[ "$(api_http_code "$OLD" /api/raft-state)" = 200 ] && pass "$OLD still serves HTTP (raft-state=$(rstate "$OLD"))" || fail "$OLD does not answer HTTP"

# ---- the new leader runs ALL tenants: every master dies, NL recovers the three
for t in "${TENANTS[@]}"; do master_down "$t"; done
for t in "${TENANTS[@]}"; do
  wait_until "$T_FAILOVER" "$t: recovery on $NL" rec_rows_at_least "$NL" "$t" "$(tenant_master "$t")" 1 && pass "$t: new leader $NL recovered it" || fail "$t: no recovery on $NL"
  wait_until "$T_FAILOVER" "$t: promoted" promoted "$t" || fail "$t: replica not promoted"
done
sleep "$T_SETTLE"
for t in "${TENANTS[@]}"; do assert_eq "$t: the broken node $OLD ran no failover hook" 0 "$(hook_lines "$OLD" "$t")"; done
assert_eq "the broken node never started a recovery" 0 "$(node_rec_count "$OLD")"
assert_eq "the broken node is not the leader again" "false" "$([ "$(rstate "$OLD")" = Leader ] && echo true || echo false)"

# ---- commands published while its backend is full (they can only fail to apply on $OLD)
B2="$(tenant_replica "$TENANT_B")"
tapi_ok "$NL" "$TENANT_B" "tag/$B2/3306/during-fault/b-value" && pass "tag of tenant b accepted by the cluster while $OLD's backend is full" || fail "tag b rejected"
tapi_ok "$NL" "$TENANT_A" "tag/$(tenant_replica "$TENANT_A")/3306/during-fault/a-value" && pass "tag of tenant a accepted" || fail "tag a rejected"

# ---- repair
docker exec "$(cname $OLD)" rm -f /bk/fill
log "space released on $OLD: $(docker exec "$(cname $OLD)" sh -c 'df -k /bk | tail -1')"
wait_until 40 "heartbeat reaches tenant b's backend on $OLD again" hb_fresh "$OLD" && pass "$OLD's tenant b backend is healthy again (heartbeat resumed)" || fail "heartbeat of b on $OLD did not resume"
cluster_stable "${NODES[@]}" && pass "cluster stable with $LEADER as leader: $(states_line "${NODES[@]}")" || fail "cluster not stable after the repair"
assert_eq "$OLD still the same process" "$started_before" "$(docker inspect -f '{{.State.StartedAt}}' "$(cname $OLD)")"
for t in "$TENANT_A" "$TENANT_C"; do
  wait_until 45 "$t converged" converged "$t" "${NODES[@]}" && pass "$t: identical on the three nodes (not affected by b's failure)" || fail "$t diverges" "$(for n in "${NODES[@]}"; do echo "$n=$(tenant_fp $n $t)"; done | tr '\n' ' ')"
done
if wait_until 45 "b converged without a restart" converged "$TENANT_B" "${NODES[@]}"; then
  pass "tenant b converged on the three nodes without restarting $OLD"
else
  fail "tenant b: commands the broken node failed to apply are NOT re-applied once its backend is healthy again (no retry)" \
       "$(diverge_report "$TENANT_B" "${NODES[@]}")"
  log "workaround: restart $OLD (the Raft log is replayed from the last snapshot)"
  node_stop "$OLD" 15; node_up "$OLD"
  wait_until "$T_NODE_READY" "$OLD HTTP" node_http_ready "$OLD"; cluster_stable "${NODES[@]}" >/dev/null
  wait_until 60 "b converged after the restart of $OLD" converged "$TENANT_B" "${NODES[@]}" && pass "after restarting $OLD tenant b converges (log replay)" || fail "tenant b still diverges after restarting $OLD" "$(diverge_report "$TENANT_B" "${NODES[@]}")"
fi
assert_clean_logs "end" "${NODES[@]}"
scenario_verdict; summary
