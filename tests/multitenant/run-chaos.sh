#!/usr/bin/env bash
# Runs the chaos suite (see tests/multitenant/README-chaos.md).
#   run-chaos.sh                 scenarios 1-8 sharing one set of databases (the soak only with CHAOS_SOAK=1)
#   run-chaos.sh 3 5             only scenarios 3 and 5
#   run-chaos.sh cleanup         removes every orcmtc-* container/volume/network (and nothing else)
#   CHAOS_SOAK=1 [CHAOS_SOAK_SECONDS=600] [CHAOS_SEED=N] run-chaos.sh 9
set -u
D="$(cd "$(dirname "$0")" && pwd)"
source "$D/lib-chaos.sh"
if [ "${1:-}" = cleanup ]; then
  chaos_cleanup
  echo "containers: $(docker ps -aq --filter "name=^${ORCMT_PREFIX}-" | wc -l)  volumes: $(docker volume ls -q --filter "name=^${ORCMT_PREFIX}-" | wc -l)  networks: $(docker network ls -q --filter "name=^${ORCMT_PREFIX}-" | wc -l)"
  exit 0
fi
declare -A SC=( [1]=chaos-01-leader-partition.sh [2]=chaos-02-follower-partition.sh [3]=chaos-03-leader-pause.sh
  [4]=chaos-04-kill-leader-mid-failover.sh [5]=chaos-05-rolling-restart.sh [6]=chaos-06-snapshot-restore.sh
  [7]=chaos-07-tenant-backend-failure.sh [8]=chaos-08-tenant-mismatch.sh [9]=chaos-09-soak.sh )
want=("$@"); [ ${#want[@]} -eq 0 ] && want=(1 2 3 4 5 6 7 8)
[ "${CHAOS_SOAK:-0}" = 1 ] && [[ " ${want[*]} " != *" 9 "* ]] && want+=(9)
export CHAOS_SHARED=1
T0=$SECONDS; results=()
chaos_build_if_needed
mkdir -p "$ORCMT_WORK/run-logs"
for i in "${want[@]}"; do
  s="${SC[$i]:-}"; [ -z "$s" ] && { echo "unknown scenario $i" >&2; continue; }
  if [ "$i" = 9 ] && [ "${CHAOS_SOAK:-0}" != 1 ]; then echo "scenario 9 (soak) needs CHAOS_SOAK=1" >&2; continue; fi
  t=$SECONDS; "$D/$s" 2>&1 | tee "$ORCMT_WORK/run-logs/scenario-$i.txt" | grep -E '^(  (PASS|FAIL|SKIP)|        |########|SCENARIO-RESULT|==)' >&2; rc=${PIPESTATUS[0]}
  results+=("$i:$rc:$((SECONDS-t))s")
done
echo "" >&2; echo "=== chaos suite: ${results[*]} total $((SECONDS-T0))s ===" >&2
if [ "$ORCMT_KEEP" != 1 ]; then chaos_cleanup; fi
fail=0; for r in "${results[@]}"; do [ "$(echo "$r" | cut -d: -f2)" != 0 ] && fail=1; done
exit $fail
