# Running the NBA POC with Docker (macOS / Linux / Windows)

The whole local flywheel — Redpanda + Postgres + Redis + Temporal, the snapshot → rules → score → route →
state-machine → action-layer spine, and the local stand-ins that replace Databricks — boots as **one Docker
Compose stack**. No PowerShell, no cloud, no Databricks, no tunnel.

> This is the cross-platform path. The PowerShell scripts (`up.ps1` / `run-*.ps1`) still work on Windows+podman;
> `compose.yaml` is the portable equivalent and the recommended way for everyone else.

## Prerequisites
- **Docker Desktop** (macOS/Windows) or **Docker Engine** (Linux) — that's it.
  Podman works too: use `podman compose …` or set `NBA_COMPOSE="podman compose"`.
- ~6 GB of RAM free for the stack. On Docker Desktop, give the VM **≥ 8 GB** (Settings → Resources).

## Quick start
```bash
cd nba
docker compose up --build          # first run builds the images (~10-15 min), then boots + seeds
```
That brings up infra, creates the topics, registers the Temporal search attributes, starts the 9 services, and
runs a one-shot **seed** (action/rule definitions + `nba:rulefacts` + **200 demo members**). Within a few seconds
the snapshot-builder folds those members into `nba:snapshot:*` and the loop turns.

Prefer one command with a built-in readiness wait + smoke check:
```bash
cd nba
./up.sh            # == docker compose up --build --wait  + a "did snapshots appear?" check
./down.sh          # stop (keep data);   ./down.sh -v  to wipe the volumes too
```

## What you get
| URL / command | what |
|---|---|
| http://localhost:8233 | Temporal Web UI (the `ChannelActionWorkflow` lifecycles) |
| http://localhost:7001 | action-library API (authoring + inbound-serve / fast-path) |
| `docker compose logs -f nba-temporal-worker` | watch activate / dispatch as the loop runs |
| `docker compose exec nba-redis redis-cli --scan --pattern 'nba:snapshot:*' \| wc -l` | how many member snapshots exist |

## Knobs
| env | default | effect |
|---|---|---|
| `NBA_SEED_MEMBERS` | `200` | demo members seeded |
| `NBA_PARTITIONS` | `1` | Kafka partitions per topic |
| `NBA_COMPOSE` | `docker compose` | set to `podman compose` to use podman |

```bash
NBA_SEED_MEMBERS=1000 docker compose up --build
```

## Troubleshooting

**`toomanyrequests: You have reached your unauthenticated pull rate limit`** — Docker Hub throttles anonymous
image pulls. Any of these fixes it:
- `docker login` with a free Docker Hub account (raises the limit), or
- route Docker Hub through Google's rate-limit-free mirror. **Docker Desktop:** Settings → Docker Engine, add
  `"registry-mirrors": ["https://mirror.gcr.io"]`, Apply & Restart. **podman:** drop this in
  `/etc/containers/registries.conf.d/99-docker-mirror.conf` (inside `podman machine ssh`):
  ```
  [[registry]]
  location = "docker.io"
  [[registry.mirror]]
  location = "mirror.gcr.io"
  ```
  (The Redpanda image already pulls from `mirror.gcr.io` to avoid this.)

**A service won't start / build fails** — `docker compose logs <service>` (e.g. `nba-snapshot-builder`). Rebuild
one service with `docker compose up --build -d <service>`.

**Out of memory / containers killed** — give the Docker/podman VM more RAM (≥ 8 GB). Docker Desktop: Settings →
Resources. podman: `podman machine stop && podman machine set --memory 12288 && podman machine start`.

## Notes
- **Local-only by design.** The Databricks / lakebase env is intentionally not set — the local `nba-model`,
  `nba-journey-scorer`, and `nba-conversion-sim` close the loop instead (same as `up.ps1`).
- **The seeder is network-native** (`infra/seed/compose-seed.py`): it talks to Redpanda and Redis directly, so
  it needs no `docker exec` and is runtime-agnostic. It reads the same canonical `definitions.jsonl` /
  `redis-defs.sh` the PowerShell path uses — no drift.
- **Rebuild after code changes:** `docker compose up --build -d <service>` (or the whole stack).
- **Reference engines** (`nba-decision-engine`, `nba-flink-engine`) used by the load tests are not in the
  default compose — they're opt-in and covered by `PERFORMANCE.md` / `infra/loadtest-results.md`.
