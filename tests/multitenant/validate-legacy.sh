#!/usr/bin/env bash
# Validates the container infrastructure and the wiring against the CURRENT orchestrator binary in
# single-tenant legacy mode (no TenantsConfigFile). It is the only script that is fully runnable before the
# multi-tenant runtime (go/logic TenantManager) lands, and it keeps being useful afterwards as a regression of
# the legacy path ("tenant default").
#
#   validate-legacy.sh [stage...]       stages: mysql pg raft failover   (default: all)
#
#   mysql     legacy MariaDB cluster (tenant-a settings inline in orchestrator.conf.json): discovery
#   pg        legacy PostgreSQL (ProviderType=postgresql, tenant-c settings inline): discovery
#   raft      legacy MariaDB + Raft of one node: node is Leader, discovery
#   failover  legacy MariaDB and PostgreSQL: stop the master, expect a registered recovery and a promoted replica
set -o pipefail
source "$(dirname "$0")/lib.sh"
orcmt_trap_cleanup

stages=("$@"); [ ${#stages[@]} -eq 0 ] && stages=(mysql pg raft failover)
has() { local s; for s in "${stages[@]}"; do [ "$s" = "$1" ] && return 0; done; return 1; }
DB=/data/legacy.db
A1="$(ip_of a1)"; A2="$(ip_of a2)"; C1="$(ip_of c1)"; C2="$(ip_of c2)"

T0=$SECONDS
[ -x "$ORCMT_WORK/bin/orchestrator" ] && [ "${ORCMT_REBUILD:-0}" != 1 ] || build_orchestrator
net_up
need=(); has mysql && need+=(a); has raft && need+=(a); has failover && need+=(a c); has pg && need+=(c)
need=($(printf '%s\n' "${need[@]}" | sort -u))
log "bringing up topologies: ${need[*]}"
"$ORCMT_DIR/topology.sh" up "${need[@]}" || die "topologies failed"
log "infrastructure ready after $((SECONDS-T0))s"

# fresh node: drop container+data volume, regenerate config, start, wait for HTTP
fresh_node() {   # fresh_node PROFILE
  docker rm -f -v "$(cname n1)" >/dev/null 2>&1; docker volume rm -f "$(cname n1)-data" >/dev/null 2>&1
  "$ORCMT_DIR/gen-config.sh" "$1" >/dev/null
  node_start n1
  wait_until "$T_NODE_READY" "orchestrator n1 HTTP ($1)" node_http_ready n1 || { node_logs n1 | tail -30 >&2; return 1; }
}
no_errors_in_log() {  # fatal/panic lines in the node log
  local bad; bad="$(node_logs "$1" | grep -E -i 'panic|fatal|unknown tenant|goroutine [0-9]+ \[' | head -5)"
  [ -z "$bad" ] && pass "$1 log has no panic/fatal" || fail "$1 log has panic/fatal lines" "$bad"
}

if has mysql; then
  echo "== legacy mysql (MariaDB $IMG_MARIADB) =="
  fresh_node legacy-mysql || die "node did not start"
  assert_topology "legacy mysql" n1 $DB mysql "$A1" "$A2" 3306
  assert_eq "legacy mysql: DetectClusterAliasQuery -> suggested_cluster_alias" "alias-a" "$(sql n1 $DB "select distinct suggested_cluster_alias from database_instance where suggested_cluster_alias <> ''")"
  api n1 /api/clusters | grep -q "$A1:3306" && pass "legacy mysql: /api/clusters lists $A1:3306" || fail "legacy mysql: /api/clusters" "$(api n1 /api/clusters)"
  assert_eq "legacy mysql: /api/cluster/<name> instances" 2 "$(api n1 "/api/cluster/$A1:3306" | json_len)"
  no_errors_in_log n1
fi
if has pg; then
  echo "== legacy postgresql =="
  fresh_node legacy-pg || die "node did not start"
  api n1 "/api/discover/$C1/5432" >/dev/null; api n1 "/api/discover/$C2/5432" >/dev/null
  assert_topology "legacy pg" n1 $DB postgresql "$C1" "$C2" 5432
  api n1 /api/clusters | grep -q "$C1:5432" && pass "legacy pg: /api/clusters lists $C1:5432" || fail "legacy pg: /api/clusters" "$(api n1 /api/clusters)"
  no_errors_in_log n1
fi
if has raft; then
  echo "== legacy + raft of one node =="
  fresh_node legacy-raft || die "node did not start"
  wait_until "$T_RAFT_ELECTION" "n1 is raft Leader" bash -c "[ \"\$(docker exec $(cname n1) curl -s http://127.0.0.1:$ORC_HTTP_PORT/api/raft-state)\" = '\"Leader\"' ]" \
    && pass "raft: n1 state Leader ($(api n1 /api/raft-state))" || fail "raft: n1 never became Leader" "$(api n1 /api/raft-status)"
  assert_topology "legacy mysql under raft" n1 $DB mysql "$A1" "$A2" 3306
  no_errors_in_log n1
fi
if has failover; then
  echo "== legacy failover: MariaDB =="
  fresh_node legacy-mysql || die "node did not start"
  assert_topology "failover/mysql pre" n1 $DB mysql "$A1" "$A2" 3306
  log "stopping $(cname a1)"; docker stop -t 2 "$(cname a1)" >/dev/null
  if wait_until "$T_FAILOVER" "recovery registered for $A1" bash -c "[ \"\$(source $ORCMT_DIR/lib.sh; recovery_count n1 $DB)\" -ge 1 ]"; then
    pass "failover/mysql: topology_recovery has $(recovery_count n1 $DB) row(s): $(recovery_rows n1 $DB | tr '\n' ';')"
  else
    fail "failover/mysql: no recovery registered" "$(node_logs n1 | grep -i -E 'recover|analysis' | tail -8)"
  fi
  wait_until 60 "$(cname a2) promoted (read_only=0)" bash -c "[ \"\$(docker exec $(cname a2) mariadb -uroot -p$MARIADB_ROOT_PW -N -B -e 'select @@read_only' 2>/dev/null)\" = 0 ]" \
    && pass "failover/mysql: $(cname a2) is now writable" || fail "failover/mysql: $(cname a2) still read-only"
  no_errors_in_log n1

  echo "== legacy failover: PostgreSQL =="
  fresh_node legacy-pg || die "node did not start"
  api n1 "/api/discover/$C1/5432" >/dev/null; api n1 "/api/discover/$C2/5432" >/dev/null
  assert_topology "failover/pg pre" n1 $DB postgresql "$C1" "$C2" 5432
  log "stopping $(cname c1)"; docker stop -t 2 "$(cname c1)" >/dev/null
  if wait_until "$T_FAILOVER" "recovery registered for $C1" bash -c "[ \"\$(source $ORCMT_DIR/lib.sh; recovery_count n1 $DB)\" -ge 1 ]"; then
    pass "failover/pg: topology_recovery has $(recovery_count n1 $DB) row(s): $(recovery_rows n1 $DB | tr '\n' ';')"
  else
    fail "failover/pg: no recovery registered" "$(node_logs n1 | grep -i -E 'recover|analysis' | tail -8)"
  fi
  wait_until 60 "$(cname c2) promoted" bash -c "[ \"\$(docker exec $(cname c2) psql -U postgres -At -c 'select pg_is_in_recovery()' 2>/dev/null)\" = f ]" \
    && pass "failover/pg: $(cname c2) left recovery (promoted)" || fail "failover/pg: $(cname c2) still a standby"
  no_errors_in_log n1
fi

log "total ${SECONDS}s"
summary
