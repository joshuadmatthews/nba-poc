#!/usr/bin/env bash
# NBA POC unit-test runner — the FAST, hermetic gate (no live stack required).
#
# Runs `gradle test` for every Java service under nba/services/* inside the gradle container,
# reusing a shared dependency-cache volume. These are PURE unit tests (no Kafka / Redis / Temporal /
# Postgres) — the counterpart to the live integration suite in nba-tests.sh. The image builds run the
# SAME `gradle test` (Containerfile: `gradle --no-daemon test shadowJar`), so a red test fails the
# build there too; this runner just catches it in seconds, before the slow image build does.
#
# Usage:
#   bash nba/test/run-unit-tests.sh                       # every Java service
#   bash nba/test/run-unit-tests.sh action-library rules-engine   # only the named services
#
# Exit code is non-zero if any service's tests fail — wire it into CI / a pre-commit hook.
set -uo pipefail
export MSYS_NO_PATHCONV=1   # keep podman volume paths intact under Git Bash on Windows

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICES_DIR="$ROOT/nba/services"
CACHE_VOL="nba-gradle-cache"
IMAGE="docker.io/library/gradle:8.7.0-jdk21"

# Targets: explicit args, else every dir with a build.gradle.
if [ "$#" -gt 0 ]; then
  TARGETS=("$@")
else
  TARGETS=()
  for d in "$SERVICES_DIR"/*/build.gradle; do TARGETS+=("$(basename "$(dirname "$d")")"); done
fi

PASS=(); FAIL=(); SKIP=(); TOTAL=0
for s in "${TARGETS[@]}"; do
  svc="$SERVICES_DIR/$s"
  if [ ! -f "$svc/build.gradle" ]; then echo "  ?? $s — no build.gradle, skipping"; SKIP+=("$s"); continue; fi
  if [ -z "$(find "$svc/src/test" -name '*.java' 2>/dev/null | head -1)" ]; then
    echo "  -- $s — no test sources (coverage gap)"; SKIP+=("$s"); continue
  fi
  echo "==> $s : gradle test"
  log="$(mktemp 2>/dev/null || echo /tmp/nba-ut-$s.log)"
  if podman run --rm -v "$svc":/app:Z -v "$CACHE_VOL":/home/gradle/.gradle -w /app "$IMAGE" \
       gradle --no-daemon test --console=plain >"$log" 2>&1; then
    n=$(grep -rho '<testcase ' "$svc"/build/test-results/test/*.xml 2>/dev/null | wc -l | tr -d ' ')
    echo "    PASS (${n:-0} tests)"
    PASS+=("$s:${n:-0}"); TOTAL=$((TOTAL + ${n:-0}))
  else
    echo "    FAIL — last lines:"; tail -18 "$log" | sed 's/^/      /'
    FAIL+=("$s")
  fi
done

echo ""
echo "================ NBA unit-test summary ================"
for p in "${PASS[@]}"; do echo "  PASS  ${p%%:*}  (${p##*:} tests)"; done
for s in "${SKIP[@]}"; do echo "  skip  $s"; done
for f in "${FAIL[@]}"; do echo "  FAIL  $f"; done
echo "  ------------------------------------------------------"
echo "  ${#PASS[@]} service(s) green, $TOTAL tests; ${#FAIL[@]} failed; ${#SKIP[@]} skipped"
[ "${#FAIL[@]}" -eq 0 ] || exit 1
