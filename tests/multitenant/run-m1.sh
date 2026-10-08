#!/usr/bin/env bash
# Milestone 1 acceptance: ONE orchestrator process, THREE tenants at once (mariadb-a, mariadb-b, postgres-c),
# a Raft group of one node. Real MariaDB 11.4 / PostgreSQL 16 topologies (topology.sh), real binary of this tree.
#
# Needs the multi-tenant runtime (go/logic TenantManager + Tenant.Run). Until it lands the process will not
# start tenants and the assertions fail -- use validate-legacy.sh for what is runnable today.
#
#   run-m1.sh                 everything
#   M1_FAILOVER=0 run-m1.sh   skip the recovery-isolation stage (discovery only)
#   ORCMT_KEEP=1 run-m1.sh    leave the orcmt-* containers running for debugging (then ./cleanup.sh)
set -o pipefail
source "$(dirname "$0")/lib.sh"
orcmt_trap_cleanup
M1_FAILOVER="${M1_FAILOVER:-1}"
EMU="${ORCMT_M1_EMULATE:-0}"             # 1 = self-test of THIS script against 3 legacy processes (see gen-config.sh)
CADENCE_WINDOW="${CADENCE_WINDOW:-30}"   # seconds sampled to compare per-tenant InstancePollSeconds (3/5/4)
N=n1; NC="$(cname $N)"
T0=$SECONDS

[ -x "$ORCMT_WORK/bin/orchestrator" ] && [ -x "$ORCMT_WORK/bin/validate-tenants" ] && [ "${ORCMT_REBUILD:-0}" != 1 ] || build_orchestrator
"$ORCMT_DIR/topology.sh" up a b c || die "topologies failed"
# the tenants manifest is always validated with the real loader (also in emulation mode)
"$ORCMT_DIR/gen-config.sh" m1 >/dev/null || die "gen-config failed"
log "tenants file as the real loader (config.LoadTenantConfigs) sees it:"
"$ORCMT_DIR/validate-config.sh" $N 2>&1 | grep '^tenant=' | sed 's/^/    /' >&2 || die "tenants manifest rejected by config.LoadTenantConfigs"
[ "$EMU" = 1 ] && { "$ORCMT_DIR/gen-config.sh" m1-emulated >/dev/null || die "gen-config failed"; }
log "infrastructure + config ready after $((SECONDS-T0))s"

if [ "$EMU" = 1 ]; then
  node_start_emulated
else
  node_start $N
fi
wait_until "$T_NODE_READY" "$NC HTTP" node_http_ready $N || { node_logs $N | tail -40 >&2; die "orchestrator did not come up"; }
if [ "$EMU" = 1 ]; then
  wait_until 20 "emulated c process" docker exec $NC curl -s -o /dev/null http://127.0.0.1:$((ORC_HTTP_PORT+2))/api/clusters
  docker exec $NC curl -s "http://127.0.0.1:$((ORC_HTTP_PORT+2))/api/discover/$(ip_of c1)/5432" >/dev/null
  docker exec $NC curl -s "http://127.0.0.1:$((ORC_HTTP_PORT+2))/api/discover/$(ip_of c2)/5432" >/dev/null
  skip "raft leader check (emulation has no Raft)"
else
wait_until "$T_RAFT_ELECTION" "$NC is the raft Leader" bash -c "[ \"\$(source $ORCMT_DIR/lib.sh; raft_state $N)\" = Leader ]" \
  && pass "raft of one: $N is Leader" || fail "raft of one: $N never became Leader" "$(api $N /api/raft-status)"
fi
T_UP=$SECONDS

echo "== 1. the three tenants discover their own topology, simultaneously =="
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
    pass "$t: backend $(tenant_db "$t") holds exactly master+replica, provider_type=$(tenant_prov "$t"), one cluster ($(topology_cluster $N "$(tenant_db "$t")")) -- after ${found[$t]}s"
  else
    fail "$t: not discovered within ${T_DISCOVERY}s" "rows: $(topology_rows $N "$(tenant_db "$t")" 2>&1 | tr '\n' ';')"
  fi
done
# simultaneity: nobody had to wait for somebody else (spread of discovery times, informational + loose bound)
mx=0; mn=999999; for t in "${TENANTS[@]}"; do v="${found[$t]:-$T_DISCOVERY}"; [ "$v" -gt "$mx" ] && mx=$v; [ "$v" -lt "$mn" ] && mn=$v; done
log "discovery times: $(for t in "${TENANTS[@]}"; do echo -n "$t=${found[$t]:-NA}s "; done) (spread $((mx-mn))s)"
[ $((mx-mn)) -le 30 ] && pass "tenants ran concurrently (discovery spread $((mx-mn))s <= 30s)" || fail "tenants discovered one after another (spread $((mx-mn))s)"

echo "== 2. each backend is its own file and sees ONLY its own instances =="
files="$(docker exec $NC sh -c 'ls /data/*.db' | xargs -n1 basename | tr '\n' ' ')"
for t in "${TENANTS[@]}"; do
  case " $files" in *" $t.db "*) pass "$t: own backend file $t.db exists";; *) fail "$t: backend file missing" "have: $files";; esac
done
for t in "${TENANTS[@]}"; do
  db="$(tenant_db "$t")"; oth="$(others_sql "$t")"
  assert_eq "$t: no database_instance row of another tenant" 0 "$(sql $N "$db" "select count(*) from database_instance where hostname in ($oth)")"
  assert_eq "$t: no audit row about another tenant's instances" 0 "$(sql $N "$db" "select count(*) from audit where hostname in ($oth)")"
  assert_eq "$t: no topology_recovery of another tenant" 0 "$(sql $N "$db" "select count(*) from topology_recovery where hostname in ($oth)")"
  assert_eq "$t: no instance of another provider" 0 "$(sql $N "$db" "select count(*) from database_instance where provider_type <> '$(tenant_prov "$t")'")"
done
for t in "$TENANT_A" "$TENANT_B"; do   # tenant-scoped DetectClusterAliasQuery differs per tenant
  assert_eq "$t: DetectClusterAliasQuery applied" "$(tenant_alias "$t")" "$(sql $N "$(tenant_db "$t")" "select distinct suggested_cluster_alias from database_instance where suggested_cluster_alias <> ''")"
done
# control backend must not have become a dumping ground for tenant instances
ctl="$(sql $N /data/control.db "select count(*) from database_instance" 2>/dev/null || echo ERR)"
log "control backend (/data/control.db) database_instance rows: $ctl (informational; expected 0)"
if [ "$EMU" = 1 ]; then skip "control backend check (emulation)"
elif [ "$ctl" = 0 ]; then pass "control backend holds no tenant instances"
else fail "control backend holds tenant instances" "rows=$ctl"; fi

echo "== 3. tenant-scoped config really is per tenant =="
# credentials: users orcmt_a/_b/_c only exist in their own topology, so a tenant using another tenant's
# credentials could not have discovered anything (section 1). Cadence: InstancePollSeconds 3 / 5 / 4.
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
  skip "/api/tenants not available (emulation or runtime without the tenant API)"
fi

echo "== 5. process health =="
[ "$(docker inspect -f '{{.State.Running}} {{.RestartCount}}' $NC)" = "true 0" ] && pass "$NC still running, never restarted" || fail "$NC died or restarted" "$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}' $NC)"
bad="$(node_logs $N | grep -E -i 'panic|fatal|unknown tenant|goroutine [0-9]+ \[|nil pointer' | head -5)"
[ -z "$bad" ] && pass "log has no panic / fatal / 'unknown tenant'" || fail "suspicious log lines" "$bad"
denied="$(node_logs $N | grep -c -i 'access denied\|password authentication failed')"
assert_eq "no topology authentication errors (tenant credentials not mixed up)" 0 "$denied"
for t in "${TENANTS[@]}"; do   # runtime logs "tenant <id>: continuous discovery: starting (provider=...)"
  if [ "$EMU" = 1 ]; then skip "tenant start line (emulation)"; continue; fi
  [ "$(log_count $N "tenant $t: continuous discovery: starting")" -ge 1 ] && pass "log: tenant $t started its continuous discovery" || fail "log: no start line for tenant $t"
done

if [ "$M1_FAILOVER" = 1 ]; then
  echo "== 6. recoveries stay inside their tenant (stop a1 and c1, b untouched) =="
  log "stopping $(cname a1) and $(cname c1)"; docker stop -t 2 "$(cname a1)" "$(cname c1)" >/dev/null
  for t in "$TENANT_A" "$TENANT_C"; do
    if wait_until "$T_FAILOVER" "$t: recovery registered" bash -c "[ \"\$(source $ORCMT_DIR/lib.sh; recovery_count $N $(tenant_db "$t"))\" -ge 1 ]"; then
      rows="$(recovery_detail $N "$(tenant_db "$t")")"
      pass "$t: topology_recovery -> $(echo "$rows" | tr '\n' ';')"
      echo "$rows" | awk -F'|' -v m="$(tenant_master "$t")" '$1!=m {bad=1} END{exit bad}' && pass "$t: recoveries are about its own master only" || fail "$t: recovery of a foreign host" "$rows"
    else
      fail "$t: no recovery registered" "$(node_logs $N | grep -i -E 'recover' | tail -5)"
    fi
  done
  assert_eq "$TENANT_B (healthy): no recovery" 0 "$(recovery_count $N "$(tenant_db "$TENANT_B")")"
  # hooks are tenant-scoped config: a's hook ran for a, never for b/c
  wait_until "$T_FAILOVER" "hook file of $TENANT_A" docker exec $NC test -s "$(tenant_hook "$TENANT_A")" \
    && pass "$TENANT_A: PostMasterFailoverProcesses ran: $(docker exec $NC tail -1 "$(tenant_hook "$TENANT_A")")" || fail "$TENANT_A: its hook never ran"
  if [ "$EMU" != 1 ]; then   # {tenantId} placeholder and ORC_TENANT_ID env of the hooks
    hl="$(hook_last $N $TENANT_A)"
    case "$hl" in *"tid=$TENANT_A env=$TENANT_A "*) pass "hook knows its tenant ({tenantId} and ORC_TENANT_ID = $TENANT_A)";; *) fail "hook does not carry the tenant id" "$hl";; esac
  fi
  docker exec $NC test -e "$(tenant_hook "$TENANT_B")" && fail "$TENANT_B: hook ran although b had no failover" || pass "$TENANT_B: hook did not run"
  # promoted replicas
  wait_until "$T_FAILOVER" "a2 promoted (read_only=0)" bash -c "[ \"\$(docker exec $(cname a2) mariadb -uroot -p$MARIADB_ROOT_PW -N -B -e 'select @@read_only' 2>/dev/null)\" = 0 ]" && pass "a2 promoted" || fail "a2 not promoted"
  wait_until "$T_FAILOVER" "c2 promoted" bash -c "[ \"\$(docker exec $(cname c2) psql -U postgres -At -c 'select pg_is_in_recovery()' 2>/dev/null)\" = f ]" && pass "c2 promoted" || fail "c2 not promoted"
  bad="$(node_logs $N | grep -E -i 'panic|fatal|unknown tenant|goroutine [0-9]+ \[|nil pointer' | head -5)"
  [ -z "$bad" ] && pass "log still clean after the recoveries" || fail "suspicious log lines after recoveries" "$bad"
fi

log "total $((SECONDS-T0))s"
summary
