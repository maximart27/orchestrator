#!/usr/bin/env bash
# validate-config.sh NODE -- loads the generated config of NODE (and its tenants file) with the real
# config.LoadTenantConfigs code (offline, in Docker) and prints the effective tenants.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
n="${1:-n1}"
docker run --rm --network none -e ORC_A_TOPOLOGY_PASSWORD="$TOPO_PW_A" \
  -v "$ORCMT_WORK/bin/validate-tenants":/validate-tenants:ro -v "$ORCMT_WORK/conf/$n":/conf:ro \
  "$IMG_GO" /validate-tenants /conf/orchestrator.conf.json
