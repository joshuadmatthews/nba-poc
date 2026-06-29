# NBA POC — a fact-centric Next-Best-Action engine

A Kafka/fact-based **Next-Best-Action** platform: member facts stream in → a per-member **snapshot** →
**rules** (eligibility) → an RL **score** → a **router** → a **Temporal** state machine that dispatches the
action on the right channel with debounce + throttle. Self-contained local stack (Redpanda + Redis + Postgres +
Temporal on podman), plus a **three-architecture throughput study** (classic / Kafka Streams / Flink).

**Everything lives under [`nba/`](nba/) — start there: [`nba/README.md`](nba/README.md).**

## Quickstart
```powershell
pwsh nba/up.ps1 -Build     # build images, boot the whole local flywheel, seed + smoke-test (no cloud needed)
pwsh nba/down.ps1          # tear down
```
Prereqs: **podman** (or docker), **PowerShell 7+** (`pwsh`), and **python3** + **bash** for the seed/load-test
helpers. ~8 GB RAM for the stack.

## What's here
- **[`nba/`](nba/)** — the platform: `services/`, `infra/`, the design docs in [`nba/docs/`](nba/docs/), and the
  load-test study ([`nba/PERFORMANCE.md`](nba/PERFORMANCE.md), [`nba/infra/loadtest-results.md`](nba/infra/loadtest-results.md)).
- **[`nba/databricks/`](nba/databricks/)** — the optional Databricks ML/lake integration (medallion DLT, datalake
  stream, RL scoring jobs, the asset bundle). The **code and architecture are included**; the local flywheel runs
  **without** it (local stand-ins close the loop).

## Using the Databricks tier (bring your own workspace)
This is a **public** repo, so the Databricks **secrets and live endpoints are not here**:
- Secret values live only in **gitignored** `nba/databricks/databricks.env` and `nba/databricks/ml/ml.env` —
  create these locally from your own workspace. **Never commit them.**
- The workspace host, SQL warehouse id, Lakebase host, service-principal client id, job ids, and tunnel
  endpoints appear as **placeholders** (`<your-databricks-workspace>`, `<warehouse-id>`, `<job-id>`, …) — fill in
  your own. The integration code reads everything from env/config; nothing about it is hardcoded to one workspace.

## License
[MIT](LICENSE).
