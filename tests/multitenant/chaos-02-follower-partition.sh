#!/usr/bin/env bash
# Scenario 2 -- PARTITION OF A FOLLOWER (1 of 3).
# The majority keeps working. Tenant commands published during the partition (put-instance-tag on the three tenants,
# begin-downtime, ack-recovery of ONE tenant) reach the other follower at once and the isolated one after healing, and
# land ONLY in the tenant they were addressed to.
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "02-follower-partition"
chaos_setup a b c
nodes_fresh
L="$LEADER"; F="${FOLLOWERS[0]}"; G="${FOLLOWERS[1]}"
log "leader=$L isolated follower=$F other follower=$G"

# a recovery in tenants a and b, so that there is something to acknowledge (and something that must NOT be, in b)
master_down "$TENANT_A"; master_down "$TENANT_B"
for t in "$TENANT_A" "$TENANT_B"; do
  wait_until "$T_FAILOVER" "$t: recovery row on leader" rec_rows_at_least "$L" "$t" "$(tenant_master "$t")" 1 && pass "$t: leader registered the recovery" || fail "$t: no recovery on leader"
  wait_until "$T_FAILOVER" "$t: promoted" promoted "$t" || fail "$t: replica not promoted"
  wait_until 30 "$t: recovery row on follower $F" rec_rows_at_least "$F" "$t" "$(tenant_master "$t")" 1 && pass "$t: recovery row replicated to follower $F before the partition" || fail "$t: row not replicated to $F"
done
wait_until 60 "pre-partition convergence a" converged "$TENANT_A" "${NODES[@]}" || log "info: a not yet converged before the partition"
CLA="$(cluster_name_of "$L" "$TENANT_A")"; CLB="$(cluster_name_of "$L" "$TENANT_B")"
log "clusters: a='$CLA' b='$CLB'"

partition_raft "$F"
sleep 3
assert_eq "leader $L unchanged" Leader "$(rstate "$L")"
assert_eq "other follower $G still Follower" Follower "$(rstate "$G")"
assert_eq "isolated follower is not Leader" "false" "$([ "$(rstate "$F")" = Leader ] && echo true || echo false)"

A2="$(tenant_replica "$TENANT_A")"; B2="$(tenant_replica "$TENANT_B")"; C1="$(tenant_master "$TENANT_C")"
# commands during the partition: half through the leader, half through the follower G (which proxies to the leader)
tapi_ok "$L" "$TENANT_A" "tag/$A2/3306/chaos-a/value-a"            && pass "put-instance-tag a via leader" || fail "tag a rejected"
tapi_ok "$G" "$TENANT_B" "tag/$B2/3306/chaos-b/value-b"            && pass "put-instance-tag b via follower $G (proxied)" || fail "tag b rejected"
tapi_ok "$L" "$TENANT_C" "tag/$C1/5432/chaos-c/value-c"            && pass "put-instance-tag c via leader" || fail "tag c rejected"
tapi_ok "$G" "$TENANT_A" "begin-downtime/$A2/3306/chaos-owner/chaos-downtime-a/1h" && pass "begin-downtime a" || fail "downtime a rejected"
tapi_ok "$L" "$TENANT_A" "ack-recovery/cluster/$CLA?comment=chaos-ack"               && pass "ack-recovery of tenant a's cluster" || fail "ack a rejected"
sleep 3
assert_eq "tenant a on leader: recoveries acknowledged" 1 "$(ack_count "$L" "$TENANT_A")"
assert_eq "tenant b on leader: recoveries NOT acknowledged" 0 "$(ack_count "$L" "$TENANT_B")"
wait_until 20 "other follower $G has the commands" tag_present "$G" "$TENANT_C" chaos-c
for n in "$L" "$G"; do
  assert_eq "$n: tag chaos-a in tenant a" 1 "$(sql $n "$(tdb $TENANT_A)" "select count(*) from database_instance_tags where tag_name='chaos-a'")"
  assert_eq "$n: tag chaos-b in tenant b" 1 "$(sql $n "$(tdb $TENANT_B)" "select count(*) from database_instance_tags where tag_name='chaos-b'")"
  assert_eq "$n: tag chaos-c in tenant c" 1 "$(sql $n "$(tdb $TENANT_C)" "select count(*) from database_instance_tags where tag_name='chaos-c'")"
done
assert_eq "isolated $F has NOT received them (partition is real): tags a" 0 "$(sql $F "$(tdb $TENANT_A)" "select count(*) from database_instance_tags where tag_name='chaos-a'")"
assert_eq "isolated $F: tenant a recoveries still unacknowledged" 0 "$(ack_count "$F" "$TENANT_A")"
assert_eq "isolated $F: no change of leadership claims" "false" "$([ "$(rstate "$F")" = Leader ] && echo true || echo false)"

heal_raft "$F"
cluster_stable "${NODES[@]}" && pass "healed: single Leader $LEADER: $(states_line "${NODES[@]}")" || fail "cluster not stable after healing"
[ "$LEADER" = "$L" ] && pass "the partition of a follower did not change the leader" || fail "leader changed ($L -> $LEADER) because a follower was partitioned"
wait_until 60 "isolated $F received the commands" tag_present "$F" "$TENANT_C" chaos-c
for t in "${TENANTS[@]}"; do
  wait_until 45 "$t converged on ${NODES[*]}" converged "$t" "${NODES[@]}" && pass "$t: identical state on all 3 nodes" || fail "$t: nodes diverge" "$(for n in "${NODES[@]}"; do echo "$n=$(tenant_fp $n $t)"; done | tr '\n' ' ')"
done
assert_eq "$F: tag chaos-a in tenant a" 1 "$(sql $F "$(tdb $TENANT_A)" "select count(*) from database_instance_tags where tag_name='chaos-a'")"
assert_eq "$F: tag chaos-b in tenant b" 1 "$(sql $F "$(tdb $TENANT_B)" "select count(*) from database_instance_tags where tag_name='chaos-b'")"
assert_eq "$F: tag chaos-c in tenant c" 1 "$(sql $F "$(tdb $TENANT_C)" "select count(*) from database_instance_tags where tag_name='chaos-c'")"
assert_eq "$F: downtime of a landed in tenant a" 1 "$(sql $F "$(tdb $TENANT_A)" "select count(*) from database_instance_downtime where reason='chaos-downtime-a'")"
assert_eq "$F: acknowledgement landed in tenant a" 1 "$(ack_count "$F" "$TENANT_A")"
assert_eq "$F: tenant b recoveries stay unacknowledged (ack was for a only)" 0 "$(ack_count "$F" "$TENANT_B")"
# no cross-tenant leakage on ANY node
for n in "${NODES[@]}"; do
  leak=0
  for t in "${TENANTS[@]}"; do
    for tag in chaos-a chaos-b chaos-c; do
      own=chaos-${t##*-}; [ "$tag" = "$own" ] && continue
      leak=$((leak + $(sql $n "$(tdb $t)" "select count(*) from database_instance_tags where tag_name='$tag'")))
    done
    [ "$t" != "$TENANT_A" ] && leak=$((leak + $(sql $n "$(tdb $t)" "select count(*) from database_instance_downtime where reason='chaos-downtime-a'")))
  done
  assert_eq "$n: no command leaked into another tenant" 0 "$leak"
done
assert_clean_logs "end" "${NODES[@]}"
scenario_verdict; summary
