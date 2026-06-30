# AGENTS.md — operating the NBA stack

A runbook for an agent (or human) working this module: how to bring it up, test it, and debug the local infra.
For *what it is*, start at [README.md](README.md); for architecture, [docs/](docs/).

## Ground rules
- **podman, not docker** — every script calls `podman`. Containers are `ais-nba-*` on the `aiservices_default`
  network. POC containers are **safe to recreate**.
- **Don't commit to `main`** — branch → PR → approve & ship.
- **Secrets are in vault / gitignored `.env`**, never in code. The public mirror (`nba-poc`) is scrubbed; this
  working repo keeps the real Databricks identifiers.
- Shells: the `.ps1` are PowerShell (pwsh), the `.sh`/`.py` are bash + python3 (Git Bash/WSL on Windows). Use
  `MSYS_NO_PATHCONV=1` when a podman arg looks like a path (e.g. `rpk` topic args).

## Bring it up (one command)
```powershell
pwsh nba/up.ps1 -Build      # first time: builds all images, boots infra+services in wave order, seeds, smoke-tests
pwsh nba/up.ps1             # subsequent (reuse images, ~2-3 min)
pwsh nba/up.ps1 -Engines    # also start the KStreams + Flink reference engines (shadow mode)
pwsh nba/down.ps1           # teardown (add -Volumes to wipe data)
```
This is the **local-only** flywheel (no Databricks). It boots: Redpanda + Postgres + Redis + Temporal → topics →
snapshot-builder → rules-engine → journey-scorer → conversion-sim → action-router → temporal-worker →
action-layer, then seeds definitions (`infra/seed/`) + ~200 members and confirms facts → snapshots →
evaluations. Boot order is the `ais.boot.wave=N` labels (15 infra → 16 → … → 21 action-layer).

## Run the tests
```bash
bash nba/test/nba-tests.sh          # integration suite (drives the live stack, asserts at each layer)
bash nba/test/nba-tests.sh fast     # skip the slow Temporal-lifecycle tests
bash nba/infra/run-loadtests.sh     # per-stage throughput matrix (classic spine)
bash nba/infra/run-loadtests.sh --engines   # also the Flink shadow rules stage
```
Tests use a fresh member id per run (no reset needed). Lifecycle tests need the temporal worker on a short
debounce: `pwsh nba/services/nba-temporal/run.ps1 -DebounceSeconds 10`.

## Debug the local infra
```bash
podman ps --filter name=ais-nba- --format '{{.Names}}\t{{.Status}}'   # what's up
podman logs -f ais-nba-temporal-worker        # the loop: 'activate' / dispatch lines
podman logs --tail 50 ais-nba-rules-engine    # eval errors
# Kafka:
podman exec ais-nba-redpanda rpk group list                          # consumer groups + lag
podman exec ais-nba-redpanda rpk group describe <group>              # per-topic lag (drain health)
podman exec ais-nba-redpanda rpk topic consume nba.member.facts -o end -n 5 -f '%k | %v\n'
# Redis:
podman exec ais-nba-redis redis-cli --scan --pattern 'nba:snapshot:*' | wc -l
podman exec ais-nba-redis redis-cli hgetall nba:snapshot:<nbaId>
# Temporal UI: http://localhost:8233
```
Trace a member: facts (`nba.member.facts`) → `nba:snapshot:{nbaId}` → `nba.evaluations` → `nba.activations` →
Temporal workflow `nba-ca:{nbaId}:{actionId}:{channel}` → dispositions back on `nba.member.facts`.

## Gotchas (these have bitten before)
- **Temporal: all workflow starts fail / 0 workflows** → the custom search attributes aren't registered.
  `run-nba-temporal.ps1` registers `NbaActionId`/`NbaChannel` on boot (in-memory `start-dev` loses them on
  restart). Re-run it.
- **`rpk topic produce -f '%k|%v'` writes only ONE record from multi-line stdin** — `%v` is greedy. Always add
  the newline: `-f '%k|%v\n'`.
- **`MESSAGE_TOO_LARGE` bulk-producing** — don't pipe a big stream into `rpk produce`. Use the bounded
  `infra/loadgen.py` / `infra/gen.py` (kafka-python) producers.
- **Redis `OOM command not allowed`** — the snapshot store hit `maxmemory` (256 MB, `noeviction`). For a big
  local run: `redis-cli config set maxmemory 2gb`. (This is the measured RAM wall — see PERFORMANCE.md §3.)
- **A consumer-group stage shows 0 rec/s in a load test** — it rejoined mid-window, or the topic is compacted
  and `trim-prefix` was policy-rejected. Keep engines warm; for compacted topics delete+recreate to empty them.
- **`rl_qnet.json` shows dirty in git after running nba-model** — `nba-model/run.ps1` stages the model file over
  the committed copy. It's a build artifact; don't commit the change.
- **Engines write `.shadow` topics by default** — `-Mode authoritative` makes them write the real topics + Redis.
  Authoritative engines **self-loop** on `member.facts` and can't be throughput-measured via Kafka lag.

## Reset
```bash
bash nba/infra/nba-clean-reset.sh all     # full (Databricks) loop: stop/wipe/seed/start  — DESTRUCTIVE
pwsh  nba/down.ps1 -Volumes; pwsh nba/up.ps1 -Build   # local-only clean slate
```
`nba-clean-reset.sh` is Databricks-coupled (cancels cloud jobs, truncates medallion). For local-only, prefer
`down -Volumes` + `up`.

## Where things are
- `up.ps1` / `down.ps1` — bring-up / teardown. `infra/` — run-nba-*.ps1 (infra), create-topics, seed/, the
  load-test harness. `services/<svc>/run.ps1` — per-service build+run. `test/nba-tests.sh` — the suite.
- Docs: [docs/](docs/) (00-13). Performance study: [PERFORMANCE.md](PERFORMANCE.md) +
  [infra/loadtest-results.md](infra/loadtest-results.md). Data stores: [docs/13-data-stores-and-schemas.md](docs/13-data-stores-and-schemas.md).
- Three spine impls: classic (default), `nba-decision-engine` (KStreams), `nba-flink-engine` (Flink). The Flink
  path is **not yet wired for Databricks** (writes Kafka directly, no outbox) — see its README.
