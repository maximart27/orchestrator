#!/usr/bin/env bash
# Brings up the real database topologies used by the tests, with real health checks and a replication probe.
#
#   topology.sh up [a] [b] [c]    default: a b c   (a=tenant mariadb-a, b=tenant mariadb-b, c=tenant postgres-c)
#   topology.sh down              removes every orcmt-* resource
#
# Topologies (fixed IPs on orcmt-net, see lib.sh):
#   a  orcmt-a1 (master, server_id 1001, gtid domain 11) -> orcmt-a2 (replica, 1002)     MariaDB 11.4, GTID slave_pos
#   b  orcmt-b1 (master, server_id 2001, gtid domain 22) -> orcmt-b2 (replica, 2002)     MariaDB 11.4, GTID slave_pos
#   c  orcmt-c1 (primary)                                -> orcmt-c2 (streaming standby) PostgreSQL 16
# Containers are started in parallel (one background job per topology).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

cmd="${1:-up}"; shift || true
case "$cmd" in
  down) orcmt_cleanup; exit 0;;
  up) ;;
  *) echo "usage: $0 up [a] [b] [c] | down" >&2; exit 2;;
esac
want=("$@"); [ ${#want[@]} -eq 0 ] && want=(a b c)
net_up
pids=()
for t in "${want[@]}"; do
  case "$t" in
    a) ( topology_a_up ) & pids+=($!);;
    b) ( topology_b_up ) & pids+=($!);;
    c) ( topology_c_up ) & pids+=($!);;
    *) die "unknown topology '$t'";;
  esac
done
rc=0; for p in "${pids[@]}"; do wait "$p" || rc=1; done
[ $rc -eq 0 ] && log "topologies up: ${want[*]}" || log "some topology failed"
exit $rc
