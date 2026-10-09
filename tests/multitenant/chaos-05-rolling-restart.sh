#!/usr/bin/env bash
# Scenario 5 -- ROLLING RESTART OF THE 3 NODES (one at a time, waiting for readiness) under CONTINUOUS tenant-command
# traffic (put-instance-tag / untag on the three tenants, spread over the three nodes with retry on another node).
# No command that was ACCEPTED (HTTP 200 + Code OK) may be lost, and every tenant ends identical on the three nodes.
source "$(dirname "$0")/lib-chaos.sh"
scenario_begin "05-rolling-restart"
chaos_setup a b c
nodes_fresh
traffic_start r5 "$ORCMT_WORK/traffic-5.jsonl" 4 0.04
sleep 10
for n in n1 n2 n3; do
  was_leader=""; [ "$(rstate "$n")" = Leader ] && was_leader=" (it was the Leader)"
  log "restarting $n$was_leader"
  node_stop "$n" 15
  sleep 3
  node_up "$n"
  wait_until "$T_NODE_READY" "$n HTTP" node_http_ready "$n" || fail "$n did not come back"
  cluster_stable "${NODES[@]}" && pass "after restarting $n: single Leader $LEADER: $(states_line "${NODES[@]}")" || fail "cluster not stable after restarting $n"
  sleep 8
done
sleep 10
traffic_stop
traffic_stats | tee "$ORCMT_WORK/traffic-5.stats" >&2
acc="$(python3 -c "import json;print(json.load(open('$ORCMT_WORK/traffic-5.stats'))['tag_ok'])" 2>/dev/null || echo 0)"
[ "${acc:-0}" -ge 300 ] && pass "traffic was meaningful: $acc tag commands accepted during the rolling restart" || fail "too little traffic accepted ($acc)"
for t in "${TENANTS[@]}"; do wait_until 60 "$t converged on ${NODES[*]}" converged "$t" "${NODES[@]}" && pass "$t: identical state on all 3 nodes" || fail "$t: nodes diverge" "$(for n in "${NODES[@]}"; do echo "$n=$(tenant_fp $n $t)"; done | tr '\n' ' ')"; done
traffic_verify r5 "${NODES[@]}"
assert_eq "no accepted command missing on any node (3 tenants x 3 nodes)" 0 "$TRAFFIC_MISSING"
log "info: $TRAFFIC_EXTRA commands present but never acknowledged to the client (ambiguous outcomes)"
for n in "${NODES[@]}"; do [ "$(docker inspect -f '{{.State.Running}}' "$(cname $n)")" = true ] && pass "$n running" || fail "$n not running"; done
# 5b -- side check found while building the suite: /api/.../untag is applied on ONE node only (go/http/api.go: the Untag and
# UntagAll handlers call tenant.Store.Untag directly; nothing publishes the "delete-instance-tag" op that the applier
# knows). Pre-existing in the legacy single-tenant code, reported here because the tenant API inherits it.
UT="$(tenant_replica "$TENANT_A")"
tapi_ok "${NODES[0]}" "$TENANT_A" "tag/$UT/3306/untag-probe/x" || fail "untag probe: tag rejected"
sleep 3
tapi_ok "${NODES[1]}" "$TENANT_A" "untag/$UT/3306/untag-probe" || fail "untag probe: untag rejected"
sleep 8
left=""; for n in "${NODES[@]}"; do left="$left$n=$(sql "$n" "$(tdb "$TENANT_A")" "select count(*) from database_instance_tags where tag_name='untag-probe'") "; done
log "untag-probe rows left per node after an accepted untag: $left"
[ "$(echo "$left" | tr ' ' '\n' | grep -c '=0$')" = 3 ] && pass "untag replicated to every node" || fail "[pre-existing, not multi-tenant] an accepted untag is not replicated through Raft: $left"
assert_clean_logs "end" "${NODES[@]}"
scenario_verdict; summary
