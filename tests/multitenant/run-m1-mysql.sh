#!/usr/bin/env bash
# Milestone 1 with a MySQL/MariaDB BACKEND: ONE orchestrator process, THREE tenants (mariadb-a, mariadb-b, postgres-c),
# a Raft of one node, and ONE MariaDB backend server holding one database per tenant (+ orc_control for the node).
# Same assertions as run-m1.sh (re-pointed at the databases) plus the ones specific to MySQL (sections 2b, 2c, 6b).
# See README-mysql.md. Wrap in the docker lock when other agents share the host:
#   flock /tmp/orcmt-docker.lock tests/multitenant/run-m1-mysql.sh
#
#   M1_FAILOVER=0 run-m1-mysql.sh   skip the recovery-isolation stage
#   ORCMT_KEEP=1  run-m1-mysql.sh   leave the orcmtm-* containers running (then ORCMT_PREFIX=orcmtm ./cleanup.sh)
set -o pipefail
source "$(dirname "$0")/lib-mysql.sh"
orcmt_trap_cleanup
M1_FAILOVER="${M1_FAILOVER:-1}"
CADENCE_WINDOW="${CADENCE_WINDOW:-30}"
N=n1; NC="$(cname $N)"
T0=$SECONDS

avail=$(free -m | awk '/^Mem:/{print $7}'); [ "${avail:-0}" -ge 1500 ] || log "WARNING: only ${avail}MB available memory (suite wants >= 1500)"
[ -x "$ORCMT_WORK/bin/orchestrator" ] && [ -x "$ORCMT_WORK/bin/validate-tenants" ] && [ "${ORCMT_REBUILD:-0}" != 1 ] || build_orchestrator
orcmt_cleanup
net_up
( bk_up ) & bkpid=$!
"$ORCMT_DIR/topology.sh" up a b c || die "topologies failed"
wait $bkpid || die "backend server failed"
"$ORCMT_DIR/gen-config-mysql.sh" m1 >/dev/null || die "gen-config-mysql failed"
log "tenants file as the real loader (config.LoadTenantConfigs) sees it:"
"$ORCMT_DIR/validate-config.sh" $N 2>&1 | grep '^tenant=' | sed 's/^/    /' >&2 || die "tenants manifest rejected by config.LoadTenantConfigs"
log "infrastructure + config ready after $((SECONDS-T0))s"

node_start $N
wait_until "$T_NODE_READY" "$NC HTTP" node_http_ready $N || { node_logs $N | tail -40 >&2; die "orchestrator did not come up"; }
wait_until "$T_RAFT_ELECTION" "$NC is the raft Leader" bash -c "[ \"\$(source $ORCMT_DIR/lib-mysql.sh; raft_state $N)\" = Leader ]" \
  && pass "raft of one: $N is Leader" || fail "raft of one: $N never became Leader" "$(api $N /api/raft-status)"
T_UP=$SECONDS

echo "== 1. the three tenants discover their own topology, simultaneously (into their own MySQL database) =="
declare -A found
while [ $((SECONDS-T_UP)) -lt "$T_DISCOVERY" ] && [ ${#found[@]} -lt ${#TENANTS[@]} ]; do
  for t in "${TENANTS[@]}"; do
    [ -n "${found[$t]:-}" ] && continue
    topology_matches $N "$(tenant_db "$t")" "$(tenant_prov "$t")" "$(tenant_master "$t")" "$(tenant_replica "$t")" "$(tenant_port "$t")" \
      && found[$t]=$((SECONDS-T_UP))
  done
  sleep 1
done
for t in "${TENANTS[@]}"; do
  if [ -n "${found[$t]:-}" ]; then
    pass "$t: database $(be_db $N $t) holds exactly master+replica, provider_type=$(tenant_prov "$t"), one cluster ($(topology_cluster $N "$(tenant_db "$t")")) -- after ${found[$t]}s"
  else
    fail "$t: not discovered within ${T_DISCOVERY}s" "rows: $(topology_rows $N "$(tenant_db "$t")" 2>&1 | tr '\n' ';')"
  fi
done
mx=0; mn=999999; for t in "${TENANTS[@]}"; do v="${found[$t]:-$T_DISCOVERY}"; [ "$v" -gt "$mx" ] && mx=$v; [ "$v" -lt "$mn" ] && mn=$v; done
log "discovery times: $(for t in "${TENANTS[@]}"; do echo -n "$t=${found[$t]:-NA}s "; done) (spread $((mx-mn))s)"
[ $((mx-mn)) -le 30 ] && pass "tenants ran concurrently (discovery spread $((mx-mn))s <= 30s)" || fail "tenants discovered one after another (spread $((mx-mn))s)"

echo "== 2. each backend is its own DATABASE of the one server and sees ONLY its own instances =="
dbs="$(bk_root "show databases like 'orc\\_%'" | sort | tr '\n' ' ')"
assert_eq "the backend server holds exactly the control + tenant databases" "$(expected_dbs $N | tr '\n' ' ')" "$dbs"
for t in "${TENANTS[@]}"; do
  k="$(tenant_db "$t")"; oth="$(others_sql "$t")"
  assert_eq "$t: no database_instance row of another tenant" 0 "$(sql $N "$k" "select count(*) from database_instance where hostname in ($oth)")"
  assert_eq "$t: no audit row about another tenant's instances" 0 "$(sql $N "$k" "select count(*) from audit where hostname in ($oth)")"
  assert_eq "$t: no topology_recovery of another tenant" 0 "$(sql $N "$k" "select count(*) from topology_recovery where hostname in ($oth)")"
  assert_eq "$t: no instance of another provider" 0 "$(sql $N "$k" "select count(*) from database_instance where provider_type <> '$(tenant_prov "$t")'")"
  assert_eq "$t: database_instance has exactly its 2 instances" 2 "$(sql $N "$k" "select count(*) from database_instance")"
done
for t in "$TENANT_A" "$TENANT_B"; do
  assert_eq "$t: DetectClusterAliasQuery applied" "$(tenant_alias "$t")" "$(sql $N "$(tenant_db "$t")" "select distinct suggested_cluster_alias from database_instance where suggested_cluster_alias <> ''")"
done
assert_eq "control backend (orc_control) holds no tenant instances" 0 "$(sql $N control "select count(*) from database_instance")"
assert_eq "control backend holds no audit of tenant instances" 0 "$(sql $N control "select count(*) from audit")"

echo "== 2b. the schema (EnsureSchema per tenant) is complete in every database =="
ref="$(tables_of $N control)"
[ "$(echo "$ref" | grep -c .)" -ge 20 ] && pass "orc_control has $(echo "$ref" | grep -c .) tables" || fail "orc_control has too few tables" "$ref"
echo "$ref" | grep -qx orchestrator_db_deployments && pass "orc_control: orchestrator_db_deployments exists" || fail "orc_control: no orchestrator_db_deployments"
for t in "${TENANTS[@]}"; do
  k="$(tenant_db "$t")"
  [ "$(tables_of $N "$k")" = "$ref" ] && pass "$t: same $(echo "$ref" | grep -c .) tables as orc_control" || fail "$t: table set differs from orc_control" "$(diff <(echo "$ref") <(tables_of $N "$k") | head -5)"
  [ "$(sql $N "$k" "select count(*) from orchestrator_db_deployments")" -ge 1 ] && pass "$t: orchestrator_db_deployments has a deployed version" || fail "$t: orchestrator_db_deployments empty"
done

echo "== 2c. node_health is mirrored (heartbeat) into every tenant database and the control one =="
for k in control "${TENANTS[@]}"; do
  c="$(sql $N "$(tenant_db "$k")" "select count(*) from node_health where hostname='$NC' and last_seen_active > now() - interval 60 second")"
  assert_eq "$k: node_health has a fresh heartbeat of $NC" 1 "$c"
done

echo "== 3. tenant-scoped config really is per tenant =="
declare -A cad
for t in "${TENANTS[@]}"; do cad[$t]=""; done
for i in $(seq 1 "$CADENCE_WINDOW"); do
  for t in "${TENANTS[@]}"; do
    cad[$t]="${cad[$t]}$(sql $N "$(tenant_db "$t")" "select last_checked from database_instance where hostname='$(tenant_master "$t")'")"$'\n'
  done
  sleep 1
done
declare -A cnt
for t in "${TENANTS[@]}"; do cnt[$t]="$(printf '%s' "${cad[$t]}" | sort -u | grep -c .)"; done
log "distinct polls of the master in ${CADENCE_WINDOW}s: $(for t in "${TENANTS[@]}"; do echo -n "$t(poll $(tenant_poll "$t")s)=${cnt[$t]} "; done)"
[ "${cnt[$TENANT_A]}" -gt "${cnt[$TENANT_B]}" ] && pass "InstancePollSeconds is per tenant (a=3s polled ${cnt[$TENANT_A]}x > b=5s polled ${cnt[$TENANT_B]}x)" \
  || fail "poll cadence does not follow per-tenant InstancePollSeconds" "a=${cnt[$TENANT_A]} b=${cnt[$TENANT_B]} c=${cnt[$TENANT_C]}"

echo "== 4. API =="
if [ "$(api_http_code $N /api/tenants)" = 200 ]; then
  ids="$(api $N /api/tenants | python3 -c 'import json,sys; d=json.load(sys.stdin); d=d.get("Details",d) if isinstance(d,dict) else d; print(" ".join(sorted(x if isinstance(x,str) else x.get("ID") or x.get("id") or x.get("tenantId","") for x in d)))' 2>/dev/null)"
  assert_eq "GET /api/tenants lists the tenants" "$(printf '%s\n' "${TENANTS[@]}" | sort | tr '\n' ' ' | sed 's/ $//')" "$ids"
  for t in "${TENANTS[@]}"; do
    c="$(api $N "/api/tenants/$t/clusters")"
    echo "$c" | grep -q "$(tenant_master "$t")" && pass "GET /api/tenants/$t/clusters lists $(tenant_master "$t")" || fail "GET /api/tenants/$t/clusters" "$c"
    others="$(others_sql "$t" | tr -d "'" | tr ',' ' ')"; leak=0
    for o in $others; do echo "$c" | grep -q "$o" && leak=1; done
    assert_eq "GET /api/tenants/$t/clusters leaks nothing from other tenants" 0 "$leak"
  done
  h="$(api $N /api/health)"
  for t in "${TENANTS[@]}"; do echo "$h" | grep -q "\"$t\"" && pass "GET /api/health reports tenant $t" || fail "GET /api/health does not mention tenant $t" "$h"; done
  assert_eq "legacy GET /api/clusters is ambiguous with 3 tenants" 400 "$(api_http_code $N /api/clusters)"
else
  fail "/api/tenants not available"
fi

echo "== 5. process health =="
[ "$(docker inspect -f '{{.State.Running}} {{.RestartCount}}' $NC)" = "true 0" ] && pass "$NC still running, never restarted" || fail "$NC died or restarted" "$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}' $NC)"
bad="$(node_logs $N | grep -E -i 'panic|fatal|unknown tenant|goroutine [0-9]+ \[|nil pointer' | head -5)"
[ -z "$bad" ] && pass "log has no panic / fatal / 'unknown tenant'" || fail "suspicious log lines" "$bad"
denied="$(node_logs $N | grep -c -i 'access denied\|password authentication failed')"
assert_eq "no authentication errors (topology credentials not mixed up, backend user accepted)" 0 "$denied"
errs="$(node_logs $N | grep -E 'Error 1[0-9]{3}|Error 2[0-9]{3}|sql: |driver: bad connection|Too many connections' | head -5)"
[ -z "$errs" ] && pass "log has no MySQL driver/server errors" || fail "MySQL errors in the orchestrator log" "$errs"
for t in "${TENANTS[@]}"; do
  [ "$(log_count $N "tenant $t: continuous discovery: starting")" -ge 1 ] && pass "log: tenant $t started its continuous discovery" || fail "log: no start line for tenant $t"
done
pw="$(node_logs $N | grep -c -F "$BK_PW")"
assert_eq "the backend password never appears in the node log" 0 "$pw"
log "backend connections on the server: $(bk_root "show status like 'Threads_connected'" | awk '{print $2}') (max_connections=$BK_MAX_CONN)"

if [ "$M1_FAILOVER" = 1 ]; then
  echo "== 6. recoveries stay inside their tenant (stop a1 and c1, b untouched) =="
  log "stopping $(cname a1) and $(cname c1)"; docker stop -t 2 "$(cname a1)" "$(cname c1)" >/dev/null
  for t in "$TENANT_A" "$TENANT_C"; do
    if wait_until "$T_FAILOVER" "$t: recovery registered" bash -c "[ \"\$(source $ORCMT_DIR/lib-mysql.sh; recovery_count $N $(tenant_db "$t"))\" -ge 1 ]"; then
      rows="$(recovery_detail $N "$(tenant_db "$t")")"
      pass "$t: topology_recovery -> $(echo "$rows" | tr '\n' ';')"
      echo "$rows" | awk -F'|' -v m="$(tenant_master "$t")" '$1!=m {bad=1} END{exit bad}' && pass "$t: recoveries are about its own master only" || fail "$t: recovery of a foreign host" "$rows"
    else
      fail "$t: no recovery registered" "$(node_logs $N | grep -i -E 'recover' | tail -5)"
    fi
  done
  assert_eq "$TENANT_B (healthy): no recovery" 0 "$(recovery_count $N "$(tenant_db "$TENANT_B")")"
  wait_until "$T_FAILOVER" "hook file of $TENANT_A" docker exec $NC test -s "$(tenant_hook "$TENANT_A")" \
    && pass "$TENANT_A: PostMasterFailoverProcesses ran: $(docker exec $NC tail -1 "$(tenant_hook "$TENANT_A")")" || fail "$TENANT_A: its hook never ran"
  hl="$(hook_last $N $TENANT_A)"
  case "$hl" in *"tid=$TENANT_A env=$TENANT_A "*) pass "hook knows its tenant ({tenantId} and ORC_TENANT_ID = $TENANT_A)";; *) fail "hook does not carry the tenant id" "$hl";; esac
  docker exec $NC test -e "$(tenant_hook "$TENANT_B")" && fail "$TENANT_B: hook ran although b had no failover" || pass "$TENANT_B: hook did not run"
  wait_until "$T_FAILOVER" "a2 promoted (read_only=0)" mariadb_writable a2 && pass "a2 promoted" || fail "a2 not promoted"
  wait_until "$T_FAILOVER" "c2 promoted" pg_promoted c2 && pass "c2 promoted" || fail "c2 not promoted"

  echo "== 6b. the recovery data are ONLY in the database of the tenant that failed over =="
  for t in "${TENANTS[@]}"; do
    k="$(tenant_db "$t")"; oth="$(others_sql "$t")"
    assert_eq "$t: no topology_recovery of another tenant (after failover)" 0 "$(sql $N "$k" "select count(*) from topology_recovery where hostname in ($oth)")"
    # steps belong to a recovery of this database, or to a failure detection (their hook step is written before the recovery registers)
    assert_eq "$t: every recovery step is of its own recovery or failure detection" 0 "$(sql $N "$k" "select count(*) from topology_recovery_steps s where s.message not like '%OnFailureDetectionProcesses%' and not exists (select 1 from topology_recovery r where r.uid = s.recovery_uid)")"
    assert_eq "$t: no topology_failure_detection of another tenant" 0 "$(sql $N "$k" "select count(*) from topology_failure_detection where hostname in ($oth)")"
  done
  for t in "$TENANT_A" "$TENANT_C"; do
    k="$(tenant_db "$t")"
    [ "$(sql $N "$k" "select count(*) from topology_recovery_steps")" -ge 1 ] && pass "$t: topology_recovery_steps written ($(sql $N "$k" "select count(*) from topology_recovery_steps") steps)" || fail "$t: no recovery steps"
    [ "$(sql $N "$k" "select count(*) from topology_failure_detection")" -ge 1 ] && pass "$t: topology_failure_detection written" || fail "$t: no failure detection row"
  done
  assert_eq "$TENANT_B: no failure detection / steps (untouched)" 0 "$(sql $N "$(tenant_db "$TENANT_B")" "select (select count(*) from topology_failure_detection)+(select count(*) from topology_recovery_steps)")"
  assert_eq "orc_control: no recovery data" 0 "$(sql $N control "select (select count(*) from topology_recovery)+(select count(*) from topology_recovery_steps)+(select count(*) from topology_failure_detection)")"
  bad="$(node_logs $N | grep -E -i 'panic|fatal|unknown tenant|goroutine [0-9]+ \[|nil pointer' | head -5)"
  [ -z "$bad" ] && pass "log still clean after the recoveries" || fail "suspicious log lines after recoveries" "$bad"
  errs="$(node_logs $N | grep -E 'Error 1[0-9]{3}|Error 2[0-9]{3}|driver: bad connection|Too many connections' | head -5)"
  [ -z "$errs" ] && pass "no MySQL errors after the recoveries" || fail "MySQL errors after the recoveries" "$errs"
fi

[ "$(docker inspect -f '{{.State.Running}} {{.State.OOMKilled}} {{.RestartCount}}' "$(cname bk)" 2>/dev/null)" = "true false 0" ] && pass "the backend server never stopped (not OOM-killed)" || fail "the backend server stopped" "$(docker inspect -f '{{.State.Status}} OOM={{.State.OOMKilled}} exit={{.State.ExitCode}}' "$(cname bk)" 2>&1)"
echo "== 7. fail-fast: unusable backend settings stop the process with a message that names the tenant =="
docker rm -f -v "$NC" >/dev/null 2>&1
ff() { # NAME EXPECT_REGEX python-mutation-of-tenants.json
  local name="$1" expect="$2" mut="$3" d="$ORCMT_WORK/conf/ff-$1"
  rm -rf "$d"; cp -r "$ORCMT_WORK/conf/$N" "$d"
  python3 - "$d/tenants.json" "$d/orchestrator.conf.json" <<PY
import json, sys
t = json.load(open(sys.argv[1])); c = json.load(open(sys.argv[2]))
$mut
json.dump(t, open(sys.argv[1], "w")); json.dump(c, open(sys.argv[2], "w"))
PY
  docker rm -f "$(cname ff)" >/dev/null 2>&1
  docker run --name "$(cname ff)" --network "$ORCMT_NET" --ip "$(ip_of n1)" --memory "$MEM_NODE" \
    -v "$d":/conf:ro -v "$ORCMT_WORK/bin/orchestrator":/usr/local/bin/orchestrator:ro -v "$ORCMT_REPO/resources":/orchestrator/resources:ro \
    -e ORC_A_TOPOLOGY_PASSWORD="$TOPO_PW_A" -w /orchestrator "$IMG_GO" \
    timeout 60 orchestrator -config /conf/orchestrator.conf.json http >"$ORCMT_WORK/ff-$name.log" 2>&1
  local rc=$?; docker rm -f "$(cname ff)" >/dev/null 2>&1
  if [ $rc -ne 0 ] && [ $rc -ne 124 ] && grep -qE "$expect" "$ORCMT_WORK/ff-$name.log"; then
    pass "fail-fast ($name): exit $rc, message matches /$expect/: $(grep -E "$expect" "$ORCMT_WORK/ff-$name.log" | head -1 | cut -c1-230)"
  else
    fail "fail-fast ($name): rc=$rc, no message matching /$expect/" "$(tail -3 "$ORCMT_WORK/ff-$name.log" | cut -c1-300)"
  fi
  if grep -qF "$BK_PW" "$ORCMT_WORK/ff-$name.log"; then fail "fail-fast ($name): the backend PASSWORD is in the log"; else pass "fail-fast ($name): no password in the log"; fi
}
ff nopriv "mariadb-a" 'for x in t["Tenants"]:
    if x["ID"]=="mariadb-a": x["Backend"]["MySQLOrchestratorDatabase"]="forbidden_db"'
ff badpw "postgres-c" 'for x in t["Tenants"]:
    if x["ID"]=="postgres-c": x["Backend"]["MySQLOrchestratorPassword"]="wrong-password"'
ff dupdb "mariadb-b.*postgres-c|postgres-c.*mariadb-b|share" 'for x in t["Tenants"]:
    if x["ID"]=="postgres-c": x["Backend"]["MySQLOrchestratorDatabase"]="orc_mariadb_b"'
ff ctldb "control|mariadb-a" 'for x in t["Tenants"]:
    if x["ID"]=="mariadb-a": x["Backend"]["MySQLOrchestratorDatabase"]=c["MySQLOrchestratorDatabase"]'

log "total $((SECONDS-T0))s"
summary
