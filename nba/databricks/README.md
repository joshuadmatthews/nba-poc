# NBA datalake on Databricks — no-bridge PoC

The NBA fact-source **medallion on Databricks**, as a faithful reference for the real
high-volume NBA work environment. Databricks **serverless reads and writes our Kafka
directly** (no local bridge processes): it consumes raw activity, runs a Lakeflow
Declarative (DLT) medallion bronze → silver → gold with **CDC**, and produces the NBA
fact contract back onto `nba.facts` — where the live NBA engine picks it up and acts.

```
 local Redpanda                         DATABRICKS (serverless)                      local NBA engine
 ───────────────                        ───────────────────────                     ────────────────
 nba.member.activity.raw  ──IN job──▶   bronze ─DLT▶ silver ─DLT/APPLY CHANGES▶ gold
                          (pandaproxy)                          (CDF: delta-only)
                                          gold ──OUT job──▶  nba.facts + nba.member.facts ──▶ ML→Temporal→action
                                        (pandaproxy)
 gold row = {entityType, entityId, key, value, valueType, eventTs, source=databricks-gold}
```

**Proven end-to-end:** seeded `dax`/`rhea` raw → serverless IN → DLT gold → serverless OUT →
`nba.facts` → the live engine scored next-best-actions and **sent a push action for `dax`**
(`nba.disposition…push = sent`). No bridges.

## Why it's shaped this way

This is a **demo for real work**: demo volume is low, the real NBA volume is huge, so the
*architecture* is the production one even though the data is tiny. Enterprise Unity Catalog,
declarative DLT, native CDC — the shape that holds at scale.

## The pieces

### Medallion — Lakeflow Declarative Pipeline (DLT)
- `nba_medallion_dlt.py` — `silver_member_activity` (streaming table) + `gold_facts` via
  **`APPLY CHANGES`** (SCD type 1, keyed by `entityType,entityId,key`, latest-by-`eventTs`).
  Gold has **Change Data Feed** on. NOTE: when a member's full snapshot re-arrives, APPLY CHANGES
  rewrites all of that member's facts (eventTs advances), so the feed contains no-op updates
  (`update_preimage.value == update_postimage.value`). True fact-level delta-only is enforced in
  the OUT job, which compares pre/post and emits only genuine value changes (see below).
- **Do NOT drop the DLT-managed silver/gold tables out-of-band** — it corrupts APPLY CHANGES
  incremental state (`Staging Table does not exist`). Use `reset` (one-time) or `run --full-refresh`.
- `run_dlt_pipeline.py` — manage it, **cheap by default**: `up` (create, triggered), `run`
  (one triggered update, process-and-stop), `run --full-refresh`, `continuous on|off` (always-on
  real-time for a live demo — costs while running), `stop`, `status`, `delete`.
- Cost is **lifecycle, not architecture**: triggered/stopped = pennies on this volume; continuous
  is the real-time shape when you want to show it.

### Serverless ↔ Kafka, directly (replaces both bridges)
Serverless can't speak native Kafka (9092) and its egress firewall only carries **HTTPS to
allowlisted FQDNs**. So we expose Redpanda's **HTTP Proxy (pandaproxy = the Kafka REST API)**
over a stable, authed HTTPS tunnel and drive it from serverless:
- `nba_in_consume.py` — pandaproxy **consumer** (binary format; keys are plain strings, so JSON
  format fails with err 40801) → appends raw to bronze. Reconstructs instance URLs against the
  tunnel (pandaproxy's returned `base_uri` is internal). Explicit offset commit (no auto-commit) —
  the commit body uses the **`partitions`** key (the `offsets` key 422s and silently never commits,
  causing a full re-read every run).
- `nba_out_produce.py` — reads gold **CDF** (`readChangeFeed`), joins `update_preimage` to
  `update_postimage` per (fact, commit version) and **emits inserts + only the updates whose value
  actually changed** → true fact-level delta-only ("5 facts re-asserted, 1 changed, 1 emitted").
  Watermark in a Delta table; first run / post-refresh emits the full snapshot, then incremental.
- `run_kafka_jobs.py in|out` — run either as a serverless job (proxy URL/token from `databricks.env`).
- Tunnel: `../infra/run-nba-pandaproxy-tunnel.ps1` → `ais-nba-pandaproxy-tunnel` (external
  `tunnel http --to nba-redpanda:8082 --reserved-domain <proxy-tunnel-endpoint> --key-auth …`). Standalone
  netns — independent of the LiveKit external tunnels. Auth = `X-Token` header.

## Networking notes (the hard-won part)
- **Serverless egress is allowlist-only.** Bind a Network Connectivity Config, then add the target
  **FQDN** to the workspace network policy. Proven: `<proxy-tunnel-endpoint>` reachable, `google.com` blocked.
- **Cloudflare blocks serverless at the edge** (error `1010`, bot-signature ban) — so the public
  `davidagents.ai` `/webhook` path is *not* usable from serverless. external (not behind Cloudflare)
  sidesteps it; that's why the proxy tunnel is external, not the CF tunnel.
- **NCC private endpoints / native 9092** need Kafka in a cloud VPC (MSK / Kafka-on-EC2) + a
  customer-VPC workspace. Not needed here — pandaproxy-over-HTTPS is direct Kafka topic I/O that
  fits the egress firewall.

## Unity Catalog
Three-level `workspace.nba_poc.*`; managed Delta tables; DLT owns `silver_member_activity` +
`gold_facts`; bronze is the IN job's append target. Managed UC Volume for any checkpoints.

## Run a full cycle (cheap)
```bash
# creds + proxy token in the gitignored databricks.env (ROTATE the SP secret after the PoC)
python run_kafka_jobs.py in            # raw -> bronze (serverless via pandaproxy)
python run_dlt_pipeline.py run         # bronze -> silver -> gold (DLT, triggered)
python run_kafka_jobs.py out           # gold CDF -> nba.facts (serverless via pandaproxy)
```

## Validated
- **Incremental DLT is reliable** — the earlier `Staging Table does not exist` was self-inflicted
  (the run script dropped DLT-managed tables out-of-band each run); removed → incremental completes.
- **Fact-level delta-only** — change 1 of a member's 5 facts → CDF carries 5 (no-op re-asserts) →
  OUT's pre/post filter emits exactly **1**. Verified end-to-end into `nba.facts`.
- **IN is incremental** — fixed the offset-commit (`partitions` key); a second run consumes 0.

## Online feature store — gold-direct today, Lakebase blocked
The inbound hot path reads its ~30 rich model features **straight from gold** (`{LAKE_NS}.gold_member_snapshot`)
via the serverless SQL warehouse — `goldFeatures(entityId)`, no Redis feature cache (~1s warm; the hot path wears
the latency). The intended online store is **Lakebase** (managed Postgres `nba-lakebase`, catalog `nba_pg`) fed by a
**CONTINUOUS synced table** from gold (CDF already enabled), giving ~ms point-reads by `nbaId`. That is **BLOCKED**:
the synced-table API creates the resource but its backing DLT pipeline fails in a retry loop with
`UNITY_CATALOG_INITIALIZATION_FAILED` / "Metastore storage root URL does not exist" — this POC account's UC metastore
has **no storage root**. FIX: an account admin sets the metastore storage root, then re-create the synced table and
flip the hot path from `goldFeatures` back to `lakebaseFeatures` (left dormant for exactly this).

## Operational — Databricks parked (minimum spend)
All Databricks compute is currently **parked**: SQL warehouse STOPPED, Lakebase instance STOPPED (parked, data kept),
custom serving endpoints (nba-cql, nba-propensity) DELETED, retrain schedules PAUSED, source-sim run cancelled.
Foundation-model APIs (databricks-*) left (pay-per-token, no idle cost). To resume: un-park Lakebase → run
`nba-ml-rl-serve` (re-creates nba-cql + `@champion`) → un-pause the retrains; the warehouse auto-starts on the first
gold query.

## Known follow-ups
- Secrets (`databricks.env`, the pandaproxy key-auth) belong in **OpenBao**, not a file, for production.
- For true always-on real-time, flip the DLT pipeline to `continuous on` (accepts the always-on cost).
