#!/usr/bin/env bash
# Removes every orcmt-* container, volume and network (and nothing else).
set -u
source "$(dirname "$0")/lib.sh"
orcmt_cleanup
echo "containers: $(docker ps -aq --filter "name=^${ORCMT_PREFIX}-" | wc -l)  volumes: $(docker volume ls -q --filter "name=^${ORCMT_PREFIX}-" | wc -l)  networks: $(docker network ls -q --filter "name=^${ORCMT_NET}\$" | wc -l)"
