#!/usr/bin/env bash
# Tear down the NBA POC compose stack.  ./down.sh        (stop, keep data)
#                                       ./down.sh -v     (also wipe the redpanda/postgres volumes)
set -euo pipefail
cd "$(dirname "$0")"
COMPOSE="${NBA_COMPOSE:-docker compose}"
$COMPOSE down "$@"
