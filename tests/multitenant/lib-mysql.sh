#!/usr/bin/env bash
# Helpers of the MySQL-BACKEND variant of the multi-tenant suite (run-m1-mysql.sh, run-m2-mysql.sh).
# Sources lib.sh (does not modify it) and overrides the few helpers that read the orchestrator backends,
# which in lib.sh are SQLite files: here the backends are DATABASES of ONE MariaDB server (orcmtm-bk).
#
# Layout of the backend server (see README-mysql.md):
#   M1 (one node):  orc_control, orc_mariadb_a, orc_mariadb_b, orc_postgres_c
#   M2 (3 nodes):   orc_n1_control, orc_n1_mariadb_a, ..., orc_n3_postgres_c   (one set per node: every Raft node
#                   needs its own backend, raft replicates commands and each node applies them to ITS backend)
# One user (orcbk) with ALL on `orc\_%`.*  -- the tenants inherit Host/Port/User/Password from the node's global
# config and override only the database, except mariadb-a/mariadb-b which spell everything out.
#
# Isolation from the other suites: prefix orcmtm, subnet 172.31.90.0/24 (override with ORCMT_PREFIX/ORCMT_SUBNET).
# The orcmt-/orcmtc-/orcmtv- resources are never touched: every filter is the anchored regex ^orcmtm-.

export ORCMT_PREFIX="${ORCMT_PREFIX:-orcmtm}"
export ORCMT_SUBNET="${ORCMT_SUBNET:-172.31.90.0/24}"
export MEM_MARIADB="${MEM_MARIADB:-256m}"
export MEM_NODE="${MEM_NODE:-384m}"
case "$ORCMT_PREFIX" in orcmt|orcmtc|orcmtv) echo "lib-mysql.sh: refusing prefix $ORCMT_PREFIX (belongs to another suite)" >&2; exit 2;; esac
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

BK_NAME=bk
BK_IP="$ORCMT_PFX.200"
BK_USER="${BK_USER:-orcbk}"
BK_PW="${BK_PW:-orcbk-secret}"
MEM_BK="${MEM_BK:-384m}"
BK_MAX_CONN="${BK_MAX_CONN:-400}"
# 1 = databases per node (M2), 0 = one set (M1)
BK_PER_NODE="${BK_PER_NODE:-0}"

# ------------------------------------------------------------------- the backend server
bk_start() {
  docker run -d --name "$(cname $BK_NAME)" --network "$ORCMT_NET" --ip "$BK_IP" --memory "$MEM_BK" \
    -v "$(cname $BK_NAME)-data:/var/lib/mysql" -e MARIADB_ROOT_PASSWORD="$MARIADB_ROOT_PW" \
    "$IMG_MARIADB" --skip-name-resolve --innodb-buffer-pool-size=64M --innodb-log-file-size=32M \
    --performance-schema=OFF --max-connections="$BK_MAX_CONN" >/dev/null || die "cannot start $(cname $BK_NAME)"
}
bk_users() {
  docker exec -i "$(cname $BK_NAME)" mariadb -uroot -p"$MARIADB_ROOT_PW" >/dev/null <<SQL || return 1
CREATE USER IF NOT EXISTS '$BK_USER'@'%' IDENTIFIED BY '$BK_PW';
GRANT ALL PRIVILEGES ON \`orc\_%\`.* TO '$BK_USER'@'%';
FLUSH PRIVILEGES;
SQL
}
bk_up() {
  net_up
  bk_start
  wait_until "$T_DB_READY" "$(cname $BK_NAME) accepting TCP" mariadb_ready "$BK_NAME" || die "backend server not ready"
  bk_users || die "cannot create the backend user"
}
# root query on the backend server: bk_root SQL
bk_root() { docker exec "$(cname $BK_NAME)" mariadb -uroot -p"$MARIADB_ROOT_PW" -N -B -e "$1"; }

# ------------------------------------------------------------------- database names
db_safe() { echo "${1//-/_}"; }
# be_db NODE TENANT|control -> database of that backend
be_db() {
  local x; x="$(db_safe "$2")"
  if [ "$BK_PER_NODE" = 1 ]; then echo "orc_${1}_${x}"; else echo "orc_${x}"; fi
}
# every database that must exist on the server for the given nodes / tenants
expected_dbs() { # NODE... ; tenants from $TENANTS
  local n t
  for n in "$@"; do
    be_db "$n" control
    for t in "${TENANTS[@]}"; do be_db "$n" "$t"; done
  done | sort
}

# ------------------------------------------------------------------- overrides of lib.sh
# "tenant_db" is the logical key; sql() maps (node, key) to the database.
tenant_db() { echo "$1"; }
# sql NODE TENANT|control SQL  -> rows, columns separated by '|', NULL as empty (same contract as lib.sh's SQLite sql())
sql() {
  local n="$1" t="$2" q="$3"
  docker exec "$(cname $BK_NAME)" mariadb -uroot -p"$MARIADB_ROOT_PW" -N -B "$(be_db "$n" "$t")" -e "$q" \
    | awk -F'\t' 'BEGIN{OFS="|"}{for(i=1;i<=NF;i++) if($i=="NULL")$i=""; $1=$1; print}'
}
reintegrated() {
  local n="$1" t="$2"
  [ "$(sql "$n" "$t" "select count(*) from database_instance where hostname='$3' and master_host='' and last_seen > now() - interval 30 second" 2>/dev/null)" = 1 ] &&
  [ "$(sql "$n" "$t" "select count(*) from database_instance where hostname='$4' and master_host='$3'" 2>/dev/null)" = 1 ]
}

# ------------------------------------------------------------------- content checks specific to MySQL
# tenant tables that hold per-tenant data (all must stay inside the tenant's database)
BK_DATA_TABLES=(database_instance audit topology_recovery topology_recovery_steps database_instance_tags cluster_alias)
# tables_of NODE KEY -> sorted list of base tables of that database
tables_of() { sql "$1" "$2" "select table_name from information_schema.tables where table_schema=database() and table_type='BASE TABLE' order by 1"; }
# table_hash NODE KEY TABLE COLUMNS  -> md5 of the rows (order independent) over the listed columns
table_hash() {
  local n="$1" k="$2" tbl="$3" cols="$4"
  sql "$n" "$k" "set session group_concat_max_len=100000000; select md5(ifnull(group_concat(h order by h separator ','),'')) from (select md5(concat_ws('|',$cols)) h from $tbl) x"
}
