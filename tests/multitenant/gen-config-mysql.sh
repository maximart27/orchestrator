#!/usr/bin/env bash
# Generates the configuration of the MySQL-backend variant: the SQLite configuration of gen-config.sh (single source
# of truth for the tenant-scoped settings) with the backend rewritten to databases of the ONE backend server:
#
#   gen-config-mysql.sh m1    n1:        orc_control + orc_<tenant>
#   gen-config-mysql.sh m2    n1,n2,n3:  orc_<node>_control + orc_<node>_<tenant>      (BK_PER_NODE=1 is implied)
#
# Global (node-local) config: BackendDB=mysql, MySQLOrchestrator{Host,Port,Database,User,Password}: the database is
# the CONTROL backend of the node. Tenants: mariadb-a and mariadb-b spell the whole Backend section out, postgres-c
# only gives its Database and inherits host/port/user/password from the global config.
set -euo pipefail
profile="${1:?usage: gen-config-mysql.sh <m1|m2>}"
case "$profile" in m2) export BK_PER_NODE=1;; esac
source "$(dirname "$0")/lib-mysql.sh"
"$ORCMT_DIR/gen-config.sh" "$profile" >/dev/null

for conf in "$ORCMT_WORK"/conf/*/; do
  n="$(basename "$conf")"
  python3 - "$conf" "$n" "$BK_PER_NODE" "$BK_IP" "$BK_USER" "$BK_PW" "${BK_MAX_POOL:-0}" <<'PY'
import json, sys
conf, node, per_node, host, user, pw, pool = sys.argv[1:8]
per_node = per_node == "1"
def db(t):
    t = t.replace("-", "_")
    return "orc_%s_%s" % (node, t) if per_node else "orc_%s" % t
p = conf + "orchestrator.conf.json"
c = json.load(open(p))
c.pop("SQLite3DataFile", None)
c.update({"BackendDB": "mysql", "MySQLOrchestratorHost": host, "MySQLOrchestratorPort": 3306,
          "MySQLOrchestratorDatabase": db("control"), "MySQLOrchestratorUser": user, "MySQLOrchestratorPassword": pw})
if pool != "0":
    c["MySQLOrchestratorMaxPoolConnections"] = int(pool)
json.dump(c, open(p, "w"), indent=2)
tp = conf + "tenants.json"
try:
    t = json.load(open(tp))
except FileNotFoundError:
    sys.exit(0)
for tenant in t["Tenants"]:
    tid = tenant["ID"]
    if tid in ("mariadb-a", "mariadb-b"):
        tenant["Backend"] = {"BackendDB": "mysql", "MySQLOrchestratorHost": host, "MySQLOrchestratorPort": 3306,
                             "MySQLOrchestratorDatabase": db(tid), "MySQLOrchestratorUser": user, "MySQLOrchestratorPassword": pw}
    else:
        tenant["Backend"] = {"MySQLOrchestratorDatabase": db(tid)}
json.dump(t, open(tp, "w"), indent=2)
PY
done
log "MySQL backend: rewrote $(ls "$ORCMT_WORK/conf" | tr '\n' ' ')(profile $profile, databases per node: $BK_PER_NODE)"
