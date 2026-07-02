#!/usr/bin/env bash
# One-command bring-up of the NBA POC flywheel on macOS / Linux — Docker by default, podman also works.
# Wraps `compose up --build --wait` (which health-gates the boot + runs the one-shot seed) and adds a smoke check.
#   ./up.sh                       # build + boot + seed 200 members + smoke
#   NBA_SEED_MEMBERS=500 ./up.sh  # more demo members
#   NBA_COMPOSE="podman compose" ./up.sh
set -euo pipefail
cd "$(dirname "$0")"
COMPOSE="${NBA_COMPOSE:-docker compose}"

echo "== NBA POC — bringing up the local flywheel ($COMPOSE) =="
$COMPOSE up -d --build --wait --wait-timeout "${NBA_WAIT:-900}"

echo "== smoke: facts -> snapshots =="
sleep 6
snaps=$($COMPOSE exec -T nba-redis redis-cli --scan --pattern 'nba:snapshot:*' 2>/dev/null | wc -l | tr -d ' ')
echo "  Redis snapshots: ${snaps:-0}"
if [ "${snaps:-0}" -gt 0 ]; then
  echo "  [ok] the flywheel turned — member facts folded into snapshots"
else
  echo "  [warn] no snapshots yet — give it a few seconds, then: $COMPOSE logs nba-snapshot-builder"
fi

cat <<EOF

  NBA stack is up.
    Temporal UI : http://localhost:8233
    Action API  : http://localhost:7001
    Watch loop  : $COMPOSE logs -f nba-temporal-worker
    Tear down   : ./down.sh        (add -v to wipe the data volumes)
EOF
