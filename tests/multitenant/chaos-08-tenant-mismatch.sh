#!/usr/bin/env bash
# Scenario 8 -- OPERATIONAL ERROR: A NODE WITH A DIFFERENT TENANTS FILE.
# n3 is started (as a late joiner, so that it is a follower) with a tenants.json that LACKS tenant mariadb-b.
#   * the commands of mariadb-b reach n3 through Raft and cannot be applied there: "command NOT applied on this node,
#     tenant unknown here" (log; the counter UnknownTenantCommands() exists but nothing exports it), n3 does not die and
#     keeps applying the commands of mariadb-a and postgres-c; no command is applied to another tenant.
#   * adding the tenant back and restarting n3 does NOT recover what was lost when a snapshot (which has no section for
#     b) was taken meanwhile; the reconstruction is: stop n3, delete its data (Raft dir + tenant DBs), start it again
#     with the right tenants file -> it re-joins and rebuilds b from the leader.
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "08-tenant-mismatch"
export CHAOS_DROP_TENANT_N3="$TENANT_B"
chaos_setup a b c
for n in "${NODES[@]}"; do docker rm -f -v "$(cname $n)" >/dev/null 2>&1; node_remove_volume "$n"; done
chaos_gen_config
log "n3 tenants: $(python3 -c "import json;print([t['ID'] for t in json.load(open('$ORCMT_WORK/conf/n3/tenants.json'))['Tenants']])") ; n1: $(python3 -c "import json;print([t['ID'] for t in json.load(open('$ORCMT_WORK/conf/n1/tenants.json'))['Tenants']])")"
cnode_start n1; cnode_start n2
for n in n1 n2; do wait_until "$T_NODE_READY" "$n HTTP" node_http_ready "$n" || die "$n did not start"; done
cluster_stable n1 n2 || die "n1/n2 do not form a quorum"
L="$LEADER"
wait_until "$T_DISCOVERY" "leader $L discovered every tenant" chaos_all_tenants_on "$L" || die "tenants not discovered on $L"
cnode_start n3
wait_until "$T_NODE_READY" "n3 HTTP" node_http_ready n3 || die "n3 did not start"
cluster_stable "${NODES[@]}" && pass "n3 (missing tenant b) joined as a follower: $(states_line "${NODES[@]}")" || fail "cluster not stable with the mismatching node"
[ "$LEADER" = n3 ] && die "n3 became the leader; run again (the scenario needs n3 as a follower)"
n3_start="$(docker inspect -f '{{.State.StartedAt}}' "$(cname n3)")"
assert_eq "n3 runs only 2 tenants" 2 "$(node_logs n3 | grep -c 'continuous discovery: starting')"
B2="$(tenant_replica "$TENANT_B")"; A2="$(tenant_replica "$TENANT_A")"; C2="$(tenant_replica "$TENANT_C")"
before_unknown="$(node_logs n3 | grep -c 'tenant unknown here')"
tapi_ok "$LEADER" "$TENANT_B" "tag/$B2/3306/lost-b1/v1" && tapi_ok "$LEADER" "$TENANT_B" "tag/$B2/3306/lost-b2/v2" && tapi_ok "$LEADER" "$TENANT_B" "begin-downtime/$B2/3306/chaos/lost-downtime/1h" \
  && pass "3 commands of tenant b accepted by the cluster (the leader applied them)" || fail "tenant b commands rejected by the leader"
tapi_ok "$LEADER" "$TENANT_A" "tag/$A2/3306/kept-a/va" && tapi_ok "$LEADER" "$TENANT_C" "tag/$C2/5432/kept-c/vc" && pass "commands of a and c accepted" || fail "commands of a/c rejected"
wait_until 30 "n3 applied the command of tenant c" tag_present n3 "$TENANT_C" kept-c && pass "n3 keeps applying the commands of the tenants it knows (a, c)" || fail "n3 did not apply tenant c's command"
tag_present n3 "$TENANT_A" kept-a && pass "n3: tenant a tag applied" || fail "n3: tenant a tag missing"
sleep 3
got=$(( $(node_logs n3 | grep -c 'tenant unknown here') - before_unknown ))
[ "$got" -ge 3 ] && pass "n3 logged $got 'command NOT applied on this node, tenant unknown here' lines (>=3 commands of b)" || fail "expected >=3 'tenant unknown here' lines on n3, got $got"
node_logs n3 | grep 'tenant unknown here' | tail -1 | cut -c1-260 >&2
assert_eq "n3: tag of b did not land in another tenant (a)" 0 "$(sql n3 "$(tdb $TENANT_A)" "select count(*) from database_instance_tags where tag_name like 'lost-b%'")"
assert_eq "n3: tag of b did not land in another tenant (c)" 0 "$(sql n3 "$(tdb $TENANT_C)" "select count(*) from database_instance_tags where tag_name like 'lost-b%'")"
assert_eq "n3 process alive, never restarted" "$n3_start" "$(docker inspect -f '{{.State.StartedAt}}' "$(cname n3)")"
assert_eq "leader $LEADER has the tags of b" 2 "$(sql "$LEADER" "$(tdb $TENANT_B)" "select count(*) from database_instance_tags where tag_name like 'lost-b%'")"
for n in n1 n2 n3; do b="$(node_logs $n | grep -E -i 'panic|fatal|goroutine [0-9]+ \[|nil pointer' | head -3)"; [ -z "$b" ] && pass "$n: no panic/fatal" || fail "$n: panic/fatal" "$b"; done
log "UnknownTenantCommands() counter: not observable from outside the process (no metric/API exports it; go/raft/tenant.go:78)"

# ---- adding the tenant afterwards does NOT recover the lost commands once a snapshot (without b) exists
echo "== adding the tenant back to n3 =====" >&2
api n3 /api/raft-snapshot >/dev/null && pass "n3 took a snapshot while b was unknown (its snapshot has no section for b)" || fail "snapshot on n3 failed"
node_stop n3 15
python3 - "$ORCMT_WORK/conf" <<'PY'
import json, shutil, sys
c = sys.argv[1]; shutil.copy(c + "/n1/tenants.json", c + "/n3/tenants.json")
PY
node_up n3
wait_until "$T_NODE_READY" "n3 HTTP" node_http_ready n3 || fail "n3 did not restart"
cluster_stable "${NODES[@]}" >/dev/null
[ "$(node_logs n3 | grep -c 'continuous discovery: starting')" -ge 5 ] && pass "n3 now runs 3 tenants (2 before the restart + 3 after)" || fail "n3 did not start the 3 tenants after the restart"
sleep 25
lost="$(sql n3 "$(tdb $TENANT_B)" "select count(*) from database_instance_tags where tag_name like 'lost-b%'" 2>/dev/null)"
log "n3 tenant b after adding it back (restart, no data wipe): lost-b tags = $lost (leader has 2)"
if [ "$lost" = 2 ]; then
  log "RESULT-INFO: the commands were re-applied by the log replay (no snapshot section for b hid them)"
else
  pass "adding the tenant back and restarting did NOT recover the lost commands (documented): n3 has $lost of 2 tags of b"
fi

# ---- reconstruction: wipe n3 completely and let it rejoin
echo "== reconstruction: wipe n3 and rejoin ==" >&2
node_stop n3 15; docker rm -f -v "$(cname n3)" >/dev/null 2>&1; node_remove_volume n3
cnode_start n3
wait_until "$T_NODE_READY" "n3 HTTP" node_http_ready n3 || fail "n3 did not start after the wipe"
cluster_stable "${NODES[@]}" >/dev/null
wait_until 90 "b converged with n3 rebuilt" converged "$TENANT_B" "${NODES[@]}" && pass "after deleting n3's data and re-joining, tenant b is identical on the three nodes (lost commands rebuilt)" || fail "tenant b still differs on n3" "$(for n in "${NODES[@]}"; do echo "$n=$(tenant_fp $n $TENANT_B)"; done | tr '\n' ' ')"
for t in "$TENANT_A" "$TENANT_C"; do converged "$t" "${NODES[@]}" && pass "$t converged" || fail "$t diverges after the rebuild"; done
scenario_verdict; summary
