#!/usr/bin/env bash
# Scenario 10 -- RECOVERY ROWS RESTORED FROM A RAFT SNAPSHOT STAY "ACTIVE" (regression found by the soak, scenario 9).
# A recovery row is in_active_period=1 for RecoveryPeriodBlockSeconds (anti-flapping), then ClearActiveRecoveries sets it to 0
# with `start_active_period < NOW() - INTERVAL n SECOND`. A node that restarts restores the latest snapshot, which rewrites the
# topology_recovery rows (sqlutils.WriteTable: REPLACE INTO) with the timestamps as RFC3339 text ("2026-10-08T23:03:39Z"); in
# SQLite that text sorts AFTER every "YYYY-MM-DD HH:MM:SS" of the same day, so the comparison is never true again that day:
# the row stays in_active_period=1 and the node refuses to fail over that cluster ("has recently experienced a failover")
# if it ever becomes the leader.
#   1. a master (tenant mariadb-b) fails; as soon as the recovery is registered a snapshot is taken on every node (row active)
#   2. both followers restart (they restore the snapshot); the block period passes
#   3. assert: the row is no longer active on any node (it is, on the restored ones)
#   4. consequence: the old master re-joins, the leader node is stopped, and the NEW master fails: the new leader (a restored
#      node) must recover the cluster again within the block period
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "10-restored-recovery-stuck"
export RECOVERY_BLOCK_SECONDS=30
chaos_setup a b c
nodes_fresh
L="$LEADER"; F1="${FOLLOWERS[0]}"; F2="${FOLLOWERS[1]}"
active() { sql "$1" "$(tdb "$TENANT_B")" "select count(*) from topology_recovery where in_active_period=1"; }
fmt() { sql "$1" "$(tdb "$TENANT_B")" "select group_concat(recovery_id||':'||start_active_period||':'||in_active_period,' ') from topology_recovery"; }
master_down "$TENANT_B"; T0=$SECONDS
wait_until "$T_FAILOVER" "recovery row registered on $L" rec_rows_at_least "$L" "$TENANT_B" "$(tenant_master "$TENANT_B")" 1 && pass "recovery of b registered on $L (row in its active period)" || fail "no recovery on $L"
wait_until 20 "row replicated" rec_rows_at_least "$F1" "$TENANT_B" "$(tenant_master "$TENANT_B")" 1
for n in "${NODES[@]}"; do api "$n" /api/raft-snapshot >/dev/null; done
log "snapshots taken on every node ($((SECONDS-T0))s after the crash, block=${RECOVERY_BLOCK_SECONDS}s); active rows: $(for n in "${NODES[@]}"; do echo -n "$n=$(active $n) "; done)"
for n in "$F1" "$F2"; do node_stop "$n" 10; node_up "$n"; wait_until "$T_NODE_READY" "$n HTTP" node_http_ready "$n"; cluster_stable "${NODES[@]}" >/dev/null; done
log "followers restarted (they restored the snapshot); waiting for the failover and for the block period to expire"
wait_until "$T_FAILOVER" "recovery completed (hook on $L)" hook_at_least "$L" "$TENANT_B" 1 && pass "recovery of b completed on $L" || fail "recovery of b did not complete on $L"
wait_until 30 "b2 promoted" promoted "$TENANT_B"
while [ $((SECONDS-T0)) -lt $((RECOVERY_BLOCK_SECONDS+40)) ]; do sleep 2; done
for n in "${NODES[@]}"; do log "$n rows (id:start_active_period:in_active_period): $(fmt "$n")"; done
assert_eq "leader $L: the recovery row left its active period" 0 "$(active "$L")"
for n in "$F1" "$F2"; do assert_eq "restored $n: the recovery row left its active period (ClearActiveRecoveries)" 0 "$(active "$n")"; done

echo "== consequence: the new leader is a restored node and the new master fails ==" >&2
mariadb_rejoin b1 b2 >/dev/null && pass "b1 re-attached as a replica of b2" || fail "b1 could not rejoin"
wait_until 40 "restored nodes see b1 under b2" reintegrated "$F1" "$TENANT_B" "$(ip_of b2)" "$(ip_of b1)"
node_stop "$L" 10
REST=("$F1" "$F2"); wait_until "$T_RAFT_ELECTION" "new leader among ${REST[*]}" single_leader "${REST[@]}" && NL="$(single_leader "${REST[@]}")" || NL="$F1"
log "leader is now $NL (restored); crashing the master b2"
docker stop -t 2 "$(cname b2)" >/dev/null
T1=$SECONDS
second() { mariadb_writable b1; }
wait_until 90 "b1 promoted by $NL" second && pass "the restored leader $NL failed over a second time (${SECONDS}s)" || fail "restored leader $NL does NOT fail over: blocked by a recovery row that never leaves its active period" "$(node_logs "$NL" | grep 'in active period' | tail -1 | cut -c1-230)"
scenario_verdict; summary
