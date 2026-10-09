#!/usr/bin/env bash
# Mixed-version / rolling-upgrade acceptance of the multi-tenant Raft: OLD = upstream orchestrator (master, no tenants),
# NEW = this tree, in ONE Raft group of three nodes (orcmtv-n1..n3) over one MariaDB topology (a1 -> a2).
#
#   1  rolling upgrade, legacy config (no tenants file): OLD x3 -> follower NEW -> 2nd follower NEW -> leader NEW,
#      one failover in the all-OLD, the OLD-leader/NEW-followers and the all-NEW phases
#   2  every legacy command crosses versions in both directions (NEW leader -> OLD follower, OLD leader -> NEW followers);
#      the legacy envelope of the Raft log carries no "tenant" field
#   3  snapshots across versions: a follower of the other version that starts EMPTY is brought up with InstallSnapshot
#      (3a NEW leader -> OLD follower, 3b OLD leader -> NEW follower)
#   4  DOCUMENTED HAZARD: second tenant active while one node is still OLD -> what the OLD node's backend ends up with
#   5  downgrade of a node NEW -> OLD in legacy mode (its Raft dir holds entries and snapshots written by NEW)
#
#   run-mixed.sh [1 2 3 4 5]      default: all.   ORCMT_KEEP=1 leaves the containers running.   ORCMT_REBUILD=1 rebuilds OLD.
# Needs ~1.5 GB free (waits up to 10 min for it). Everything is named orcmtv-*; nothing else is touched.
set -o pipefail
source "$(dirname "$0")/lib-mixed.sh"
orcmt_trap_cleanup
SCEN=("$@"); [ ${#SCEN[@]} -eq 0 ] && SCEN=(1 2 3 4 5)
T_LAST_FO=0
CUR_M=a1; CUR_R=a2

# ------------------------------------------------------------------------------------------------ shared checks
no_unknown_ops() { # LABEL: no node ever failed to understand a command of the other version
  local n bad=0 c
  for n in "${NODES[@]}"; do
    c="$(nlog_count "$n" 'Unknown command op|failed to unmarshal command|unknown tenant')"; bad=$((bad+${c:-0}))
  done
  assert_eq "$1: no 'Unknown command op' / unmarshal / unknown-tenant errors in any node log" 0 "$bad"
}
all_discovered() { local n; for n in "${NODES[@]}"; do discovered_pair "$n" "$CUR_M" "$CUR_R" || return 1; done; }
converged() { digests_equal "${NODES[@]}" >/dev/null; }
# phase_check LABEL: one leader everybody agrees on; every node discovered the topology; a fresh round of legacy commands
# applied identically on the three backends (OLD and NEW alike); no errors
phase_check() {
  local lab="$1" l d
  if wait_until 60 "one leader ($lab)" healthy_cluster; then
    l="$(leader_of)"; pass "$lab: exactly one leader, $l (${NODE_VER[$l]}), all nodes agree [$(versions)]"
  else fail "$lab: no single leader" "$(for n in "${NODES[@]}"; do echo -n "$n=$(raft_state "$n") "; done)"; return 1; fi
  wait_until "$T_DISCOVERY" "all nodes see $CUR_M master / $CUR_R replica" all_discovered && pass "$lab: every node (OLD and NEW) discovered the topology" || fail "$lab: topology not discovered on every node" "$(for n in "${NODES[@]}"; do echo "$n: $(topology_rows "$n" "$DB_A" | tr '\n' ';')"; done)"
  seed_state "$l" "$lab"
  if wait_until 40 "state converged on the 3 backends ($lab)" converged; then
    pass "$lab: replicated state identical on n1, n2, n3 ($(versions))"
  else
    fail "$lab: state diverged" "$(digests_equal "${NODES[@]}")"
    for n in "${NODES[@]}"; do digest "$n" > "$ORCMT_WORK/digest-$lab-$n.txt"; done
  fi
  for n in "${NODES[@]}"; do
    [ "$(sql "$n" "$DB_A" "select count(*) from database_instance_tags where tag_name='tag-$lab' and tag_value='value-$lab'")" = 1 ] || { fail "$lab: $n (${NODE_VER[$n]}) lacks the tag of this phase"; }
  done
  no_unknown_ops "$lab"
  # end-downtime, through the same path; a downtimed instance would also stop being recovered by the failover that follows
  end_downtimes "$l"
  wait_until 30 "downtimes ended everywhere ($lab)" bash -c "for n in n1 n2 n3; do [ \"\$(source $ORCMT_DIR/lib-mixed.sh; sql \$n $DB_A 'select count(*) from database_instance_downtime')\" = 0 ] || exit 1; done" \
    && pass "$lab: end-downtime applied on every node ($(versions))" || fail "$lab: end-downtime not applied everywhere"
}
# wait_block: the previous failover must be out of RecoveryPeriodBlockSeconds
wait_block() { local w=$((RECOVERY_BLOCK_SECONDS + 8 - (SECONDS - T_LAST_FO))); [ "$T_LAST_FO" -gt 0 ] && [ "$w" -gt 0 ] && { log "waiting ${w}s: recovery block of the previous failover"; sleep "$w"; }; return 0; }
# do_failover LABEL: kills the master; the leader (alone) recovers it, runs the hook once; the old master rejoins as replica
do_failover() {
  local lab="$1" l m mip n h0=() i=0 total0 hl rc0
  l="$(leader_of)"; m="$CUR_M"; mip="$(ip_of "$m")"
  wait_block
  rc0="$(recovery_for "$l" "$mip")"
  for n in "${NODES[@]}"; do h0[$i]="$(hook_lines "$n" "$TENANT_A")"; i=$((i+1)); done
  log "failover [$lab]: stopping master $m; leader is $l (${NODE_VER[$l]})"
  T_LAST_FO=$SECONDS
  docker stop -t 2 "$(cname "$m")" >/dev/null
  if wait_until "$T_FAILOVER" "leader $l registered the recovery of $mip" bash -c "[ \"\$(source $ORCMT_DIR/lib-mixed.sh; recovery_for $l $mip)\" -gt $rc0 ] 2>/dev/null"; then pass "$lab: leader $l (${NODE_VER[$l]}) registered the recovery of $m"; else fail "$lab: no recovery on leader $l" "$(node_logs "$l" | grep -i recover | tail -5)"; fi
  wait_until "$T_FAILOVER" "$CUR_R promoted" mariadb_writable "$CUR_R" && pass "$lab: $CUR_R promoted to master" || fail "$lab: $CUR_R not promoted"
  sleep 10
  i=0; total0=0
  for n in "${NODES[@]}"; do
    hl=$(( $(hook_lines "$n" "$TENANT_A") - h0[$i] )); i=$((i+1))
    if [ "$n" = "$l" ]; then assert_eq "$lab: failover hook ran exactly once on the leader $l (${NODE_VER[$n]})" 1 "$hl"
    else assert_eq "$lab: failover hook did not run on follower $n (${NODE_VER[$n]})" 0 "$hl"; fi
  done
  assert_eq "$lab: leader backend: exactly ONE new recovery row for the dead master" 1 "$(( $(recovery_for "$l" "$mip") - rc0 ))"
  mariadb_rejoin "$m" "$CUR_R" && pass "$lab: $m rejoined as replica of $CUR_R" || fail "$lab: rejoin of $m failed"
  local t="$CUR_M"; CUR_M="$CUR_R"; CUR_R="$t"
  wait_until "$T_DISCOVERY" "every node sees $CUR_M master / $CUR_R replica" all_discovered && pass "$lab: every node sees the new topology $CUR_M -> $CUR_R" || fail "$lab: new topology not seen on every node"
}
upgrade_node() { # NODE LABEL
  log "swapping $1 OLD -> NEW ($2)"
  node_swap "$1" new && pass "$2: $1 restarted with the NEW binary on its old data (Raft dir + SQLite)" || fail "$2: $1 did not come up as NEW"
}

# ================================================================================================ 1
scenario_1() {
  echo "== 1. ROLLING UPGRADE, legacy config: OLD x3 -> NEW x3 =="
  LOGDIR="$ORCMT_WORK/logs/s1"; rm -rf "$LOGDIR"; env_reset
  start_nodes old old old
  local l f1 f2
  phase_check P0-all-old
  do_failover P0-all-old
  l="$(leader_of)"; f1="$(other_nodes "$l" | head -1)"; f2="$(other_nodes "$l" | tail -1)"
  echo "-- upgrading follower $f1 (leader $l stays OLD)"
  upgrade_node "$f1" P1; phase_check P1-1new-2old
  echo "-- upgrading follower $f2"
  upgrade_node "$f2" P2; phase_check P2-2new-1old
  do_failover P2-OLD-leader-NEW-followers
  echo "-- upgrading the LEADER $l: forces a re-election among NEW nodes"
  upgrade_node "$l" P3
  phase_check P3-all-new
  [ "${NODE_VER[$(leader_of)]}" = new ] && pass "P3: the new leader $(leader_of) runs the NEW binary" || fail "P3: leader is not NEW"
  do_failover P3-all-new
  phase_check P4-after-failover
  assert_eq "no raft log entry of any node carries a tenant field (legacy envelope unchanged)" "0 0 0" "$(for n in "${NODES[@]}"; do echo -n "$(raft_entries_with_tenant "$n") "; done | sed 's/ $//')"
}

# ================================================================================================ 2
# battery_cross LEADER LABEL [instance|all]: every legacy command, published through the leader, applied identically by OLD and NEW
battery_cross() {
  local l="$1" lab="$2" ackmode="${3:-instance}" m r n
  m="$(ip_of "$CUR_M")"; r="$(ip_of "$CUR_R")"
  seed_state "$l" "$lab"
  wait_until 40 "converged ($lab)" converged && pass "$lab: begin-downtime, put-instance-tag, register-candidate, set-cluster-alias, register-hostname-unresolve applied identically on $(versions)" || fail "$lab: state diverged" "$(digests_equal "${NODES[@]}")"
  # explicit content, node by node (not just equality of the three digests)
  for n in "${NODES[@]}"; do
    local d; d="$(digest "$n")"
    echo "$d" | grep -q "^$r|3306|owner-$lab|reason-$lab" && echo "$d" | grep -q "^$r|3306|tag-$lab|value-$lab" \
      && echo "$d" | grep -q "^$r|3306|prefer" && echo "$d" | grep -q "alias-$lab" && echo "$d" | grep -q "^$r|virtual-$lab" \
      && pass "$lab: $n (${NODE_VER[$n]}) holds the downtime, the tag, the candidate, the alias override and the hostname-unresolve" \
      || fail "$lab: $n (${NODE_VER[$n]}) misses something" "$d"
  done
  # recoveries: disable / enable
  api_ok "$l" /api/disable-global-recoveries || fail "$lab: disable-global-recoveries"
  wait_until 20 "recovery disabled everywhere" bash -c "for n in n1 n2 n3; do [ \"\$(source $ORCMT_DIR/lib-mixed.sh; sql \$n $DB_A 'select count(*) from global_recovery_disable')\" = 1 ] || exit 1; done" && pass "$lab: disable-global-recoveries applied on every node" || fail "$lab: disable-global-recoveries not applied everywhere"
  api_ok "$l" /api/enable-global-recoveries || fail "$lab: enable-global-recoveries"
  wait_until 20 "recovery enabled everywhere" bash -c "for n in n1 n2 n3; do [ \"\$(source $ORCMT_DIR/lib-mixed.sh; sql \$n $DB_A 'select count(*) from global_recovery_disable')\" = 0 ] || exit 1; done" && pass "$lab: enable-global-recoveries applied on every node" || fail "$lab: enable-global-recoveries not applied everywhere"
  # end-downtime
  api_ok "$l" "/api/end-downtime/$r/3306" && wait_until 20 "downtime ended" bash -c "for n in n1 n2 n3; do [ \"\$(source $ORCMT_DIR/lib-mixed.sh; sql \$n $DB_A 'select count(*) from database_instance_downtime')\" = 0 ] || exit 1; done" && pass "$lab: end-downtime applied on every node" || fail "$lab: end-downtime not applied everywhere"
  # ack-recovery (there is at least one recovery row, from the failover that precedes). Not by cluster name: that variant
  # matches on the cluster alias each node derives from ITS OWN discovery state, so it can differ between nodes of any version.
  local ackpath="/api/ack-recovery/instance/$(ip_of "$CUR_R")/3306?comment=ack-$lab"; [ "$ackmode" = all ] && ackpath="/api/ack-all-recoveries?comment=ack-$lab"
  api_ok "$l" "$ackpath" || fail "$lab: ack-recovery ($ackmode)" "$(api_out "$l" "$ackpath")"
  wait_until 20 "recoveries acknowledged everywhere" bash -c "for n in n1 n2 n3; do [ \"\$(source $ORCMT_DIR/lib-mixed.sh; sql \$n $DB_A 'select count(*) from topology_recovery where acknowledged=0')\" = 0 ] || exit 1; done" && pass "$lab: ack-recovery ($ackmode) applied on every node (no unacknowledged recovery left)" || fail "$lab: ack-recovery ($ackmode) not applied everywhere" "$(for n in "${NODES[@]}"; do echo "$n: $(sql "$n" "$DB_A" "select hostname,acknowledged from topology_recovery" | tr '\n' ';')"; done)"
  # forget: the applier audits it ("auditType:forget" in the log; the audit table is only written with AuditToBackendDB)
  local f0=() i=0; for n in "${NODES[@]}"; do f0[$i]="$(nlog_count "$n" "auditType:forget instance:$r:3306")"; i=$((i+1)); done
  api_ok "$l" "/api/forget/$r/3306" || fail "$lab: forget" "$(api_out "$l" "/api/forget/$r/3306")"
  forgot_everywhere() { local j=0 x; for x in "${NODES[@]}"; do [ "$(nlog_count "$x" "auditType:forget instance:$r:3306")" -gt "${f0[$j]}" ] || return 1; j=$((j+1)); done; }
  wait_until 20 "forget applied everywhere" forgot_everywhere && pass "$lab: forget applied on every node ($(versions))" || fail "$lab: forget not applied everywhere"
  wait_until "$T_DISCOVERY" "forgotten replica rediscovered everywhere" all_discovered && pass "$lab: the forgotten replica is rediscovered on every node" || fail "$lab: replica not rediscovered"
  no_unknown_ops "$lab"
}
scenario_2() {
  echo "== 2. LEGACY COMMANDS ACROSS VERSIONS (n1 OLD, n2/n3 NEW) =="
  LOGDIR="$ORCMT_WORK/logs/s2"; rm -rf "$LOGDIR"; env_reset
  start_nodes old new new
  make_leader n2 && pass "leader is n2 (NEW); n1 is an OLD follower" || { fail "could not make n2 the leader"; return; }
  do_failover S2-NEW-leader                 # leaves one recovery row for ack-recovery
  battery_cross n2 S2a instance
  make_leader n1 && pass "leader is n1 (OLD); n2, n3 are NEW followers" || { fail "could not make n1 the leader"; return; }
  do_failover S2-OLD-leader
  battery_cross n1 S2b all
  assert_eq "no raft log entry of any node carries a tenant field" "0 0 0" "$(for n in "${NODES[@]}"; do echo -n "$(raft_entries_with_tenant "$n") "; done | sed 's/ $//')"
}

# ================================================================================================ 3
# snapshot_cross LABEL LEADER_VER FOLLOWER_VER
snapshot_cross() {
  local lab="$1" lv="$2" fv="$3" l f other
  env_reset
  if [ "$lv" = new ]; then start_nodes new new new; else start_nodes old old old; fi
  l="$(leader_of)"; f="$(other_nodes "$l" | head -1)"; other="$(other_nodes "$l" | tail -1)"
  seed_state "$l" "$lab"
  api_ok "$l" /api/disable-global-recoveries
  wait_until 40 "converged before the snapshot" converged || fail "$lab: no convergence before the test"
  log "leader $l ($lv); follower $f will start EMPTY as $fv"
  node_stop_save "$f"; wipe_node "$f"
  force_install_snapshot "$l" | sed "s/^/  [$lab] leader: /"
  node_start_ver "$f" "$fv"
  wait_until "$T_NODE_READY" "$f HTTP" node_http_ready "$f"
  wait_until 60 "$f is a follower of $l" bash -c "[ \"\$(source $ORCMT_DIR/lib-mixed.sh; raft_state $f)\" = Follower ]" || fail "$lab: $f did not join"
  if wait_until 60 "$f received an InstallSnapshot" snapshot_installed "$f"; then pass "$lab: $f ($fv) was brought up with an InstallSnapshot from $l ($lv)"; else fail "$lab: no snapshot installation seen on $f" "$(node_logs "$f" | grep -i snapshot | tail -3)"; fi
  sleep 8
  local dl df; dl="$(digest "$l")"; df="$(digest "$f")"
  if [ "$dl" = "$df" ]; then pass "$lab: $f ($fv) restored the snapshot of $l ($lv): identical state"
  else fail "$lab: $f ($fv) did NOT get the state of the snapshot written by $l ($lv)" "$(diff <(echo "$dl") <(echo "$df") | head -30)"; fi
  [ "$(sql "$f" "$DB_A" 'select count(*) from global_recovery_disable')" = 1 ] && pass "$lab: the disabled-recoveries flag survived the snapshot on $f" || fail "$lab: $f lost the disabled-recoveries flag (it would recover while the cluster says recoveries are off)"
  wait_until "$T_DISCOVERY" "$f discovered" discovered_pair "$f" "$CUR_M" "$CUR_R" && pass "$lab: $f still knows the instances" || fail "$lab: $f lost its instances"
  no_unknown_ops "$lab"
}
scenario_3() {
  echo "== 3. SNAPSHOTS ACROSS VERSIONS =="
  LOGDIR="$ORCMT_WORK/logs/s3"; rm -rf "$LOGDIR"
  echo "-- 3a NEW leader -> OLD follower"; snapshot_cross S3a-new-to-old new old
  echo "-- 3b OLD leader -> NEW follower"; snapshot_cross S3b-old-to-new old new
}

# ================================================================================================ 4
# A fake instance that exists only in tenant-b: whatever shows up with it in a backend came from a tenant-b command.
B_HOST=10.99.99.99
scenario_4() {
  echo "== 4. HAZARD: a second tenant is active while n1 is still OLD (n2, n3 NEW with a tenants file) =="
  LOGDIR="$ORCMT_WORK/logs/s4"; rm -rf "$LOGDIR"; env_reset
  gen_manifest_conf n2; gen_manifest_conf n3
  start_nodes old new new
  make_leader n2 && pass "leader n2 (NEW) runs tenants default and tenant-b; n1 is OLD and knows one backend only" || { fail "no NEW leader"; return; }
  local l=n2 n
  api_ok "$l" "/api/tag/$(ip_of a2)/3306/rack/legacy-marker" && pass "default tenant: legacy API still works" || fail "default tenant tag"
  api_ok "$l" "/api/tenants/tenant-b/tag/$B_HOST/3306/rack/tenant-b-marker"                || fail "tenant-b: tag" "$(api_out "$l" "/api/tenants/tenant-b/tag/$B_HOST/3306/rack/tenant-b-marker")"
  api_ok "$l" "/api/tenants/tenant-b/begin-downtime/$B_HOST/3306/owner-b/reason-b/1h"      || fail "tenant-b: begin-downtime"
  api_ok "$l" "/api/tenants/tenant-b/register-candidate/$B_HOST/3306/prefer"               || fail "tenant-b: register-candidate"
  sleep 5
  local q="select (select count(*) from database_instance_tags where hostname='$B_HOST')
         + (select count(*) from database_instance_downtime where hostname='$B_HOST')
         + (select count(*) from candidate_database_instance where hostname='$B_HOST')"
  for n in n2 n3; do
    assert_eq "$n (NEW): tenant-b has its 3 rows in ITS backend (tenant-b.db)" 3 "$(sql "$n" /data/tenant-b.db "$q")"
    assert_eq "$n (NEW): none of them leaked into the backend of tenant default" 0 "$(sql "$n" "$DB_A" "$q")"
  done
  assert_eq "n2/n3 (NEW): the default tenant got its legacy tag" "1 1" "$(sql n2 "$DB_A" "select count(*) from database_instance_tags where tag_name='rack' and tag_value='legacy-marker'") $(sql n3 "$DB_A" "select count(*) from database_instance_tags where tag_name='rack' and tag_value='legacy-marker'")"
  local leaked; leaked="$(sql n1 "$DB_A" "$q")"
  if [ "$leaked" = 3 ]; then
    pass "HAZARD REPRODUCED: the OLD node n1 applied the 3 commands of tenant-b to its single backend (tag, downtime and candidate of $B_HOST are in n1's default backend): $(sql n1 "$DB_A" "select 'tag='||tag_name||'='||tag_value from database_instance_tags where hostname='$B_HOST'" | tr '\n' ' ')"
  else
    fail "hazard NOT reproduced: n1 holds $leaked of the 3 tenant-b rows (the OLD node does not apply them?)"
  fi
  assert_eq "n1 (OLD): its log shows no error for those commands (it just applies them)" 0 "$(nlog_count n1 'Unknown command op|failed to unmarshal')"
}

# ================================================================================================ 5
scenario_5() {
  echo "== 5. DOWNGRADE of one node NEW -> OLD in legacy mode =="
  LOGDIR="$ORCMT_WORK/logs/s5"; rm -rf "$LOGDIR"; env_reset
  start_nodes new new new
  local l f
  l="$(leader_of)"; f="$(other_nodes "$l" | head -1)"
  seed_state "$l" S5a; api_ok "$l" /api/disable-global-recoveries
  wait_until 40 "converged" converged || fail "S5: no convergence before the snapshot"
  api "$f" /api/raft-snapshot | grep -q "snapshot created" && pass "S5: $f (NEW) wrote a snapshot in its own Raft dir: $(docker exec "$(cname "$f")" sh -c 'ls /data/raft/snapshots | tr "\n" " "')" || fail "S5: no snapshot on $f"
  seed_state "$l" S5b           # entries AFTER the snapshot: replayed from the Raft log written by the NEW leader
  wait_until 40 "converged" converged || fail "S5: no convergence after the snapshot"
  log "downgrading $f NEW -> OLD (leader $l stays NEW)"
  node_swap "$f" old && pass "S5: $f started with the OLD binary on the Raft dir and the backend written by NEW" || { fail "S5: $f (OLD) did not come up" "$(node_logs "$f" | tail -5)"; return; }
  wait_until 60 "$f follower again" bash -c "[ \"\$(source $ORCMT_DIR/lib-mixed.sh; raft_state $f)\" = Follower ]" && pass "S5: $f (OLD) is a follower of $l (NEW)" || fail "S5: $f did not rejoin"
  node_running "$f" && pass "S5: $f did not crash" || fail "S5: $f stopped"
  api_ok "$l" "/api/tag/$(ip_of "$CUR_R")/3306/after/downgrade" ; sleep 6
  if wait_until 40 "state identical after the downgrade" converged; then pass "S5: after the downgrade $f (OLD) holds the same state as $l (NEW) -- snapshot, replayed log and new commands"
  else fail "S5: $f (OLD) diverged from $l (NEW)" "$(digests_equal "${NODES[@]}")$(diff <(digest "$l") <(digest "$f") | head -20)"; fi
  [ "$(sql "$f" "$DB_A" 'select count(*) from global_recovery_disable')" = 1 ] && pass "S5: the disabled-recoveries flag survived the downgrade on $f" || fail "S5: $f lost the disabled-recoveries flag"
  no_unknown_ops S5
  wait_until "$T_DISCOVERY" "$f discovered" discovered_pair "$f" "$CUR_M" "$CUR_R" && pass "S5: $f still discovers the topology" || fail "S5: $f lost the topology"
}

# ================================================================================================ main
build_binaries
for s in "${SCEN[@]}"; do "scenario_$s"; done
summary
