# NBA lake — Infrastructure-as-Code (Databricks Asset Bundle)

`databricks.yml` defines the entire NBA medallion lakehouse declaratively, so it can be **recreated on a
fresh Databricks workspace** with a couple of commands instead of running the imperative `run_*.py`
scripts in sequence. The notebooks (`nba_*.py`) hold the Spark logic and are synced by the bundle; the
bundle is the infrastructure around them — the DLT pipeline, every serverless job, and the nightly
schedule.

## What it provisions

| Resource | Type | Notebook | Notes |
|---|---|---|---|
| `nba-medallion-dlt` | DLT pipeline | `nba_medallion_dlt.py` | serverless, triggered (cheap), CDC bronze→silver→gold |
| `nba-setup` | job | `nba_setup.py` | one-time: schema + `ckpt` volume + `bronze_member_activity` |
| `nba-datalake-ingest` | job | `nba_datalake_stream.py` | Kafka → silver/gold (+ event-path counts) |
| `nba-out-produce` | job | `nba_out_produce.py` | gold CDF → re-emit delta facts to Kafka |
| `nba-fact-reconcile` | job | `nba_fact_reconcile.py` | re-emit newly-referenced facts |
| `nba-kafka-rollover` | **scheduled** job | `nba_throttle_emit.py` + `nba_comms_count.py` | daily 00:05 UTC boundary recompute |

Everything else (the silver/gold tables, `dim_definitions`, views, state tables) is created by the
notebooks on first run — so the only bootstrap surface is `nba-setup`.

## Prerequisites

- Databricks CLI v0.218+ (`databricks --version`) — the one with bundle support.
- A workspace with **Unity Catalog** and **serverless** jobs + DLT enabled.
- Auth: a profile or env vars for the target workspace:
  ```
  export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
  export DATABRICKS_CLIENT_ID=<service-principal-id>
  export DATABRICKS_CLIENT_SECRET=<service-principal-secret>   # OAuth M2M
  ```
- The Kafka bridge reachable from Databricks (the external SASL/SCRAM tunnel), and its creds.

## Deploy to a fresh workspace

1. Point the `work` target at the new host — edit `targets.work.workspace.host` in `databricks.yml`
   (or pass `--profile <name>`).

2. Supply the Kafka bridge creds as bundle variables (never commit them). Either inline:
   ```
   export BUNDLE_VAR_kafka_bootstrap=<tunnel-endpoint>
   export BUNDLE_VAR_kafka_sasl_user=dbx-ingest
   export BUNDLE_VAR_kafka_sasl_pass=<scram-pass>
   ```
   …or, for production, put them in a **Databricks secret scope** and change the `base_parameters` in
   `databricks.yml` to `{{secrets/nba/kafka_sasl_pass}}` etc. (the upgrade path off plaintext params).

3. Deploy + bring up the lake:
   ```
   databricks bundle validate -t work
   databricks bundle deploy   -t work          # creates the pipeline, jobs, and rollover schedule
   databricks bundle run nba_setup           -t work   # schema + volume + bronze  (once)
   databricks bundle run nba_datalake_ingest -t work   # drain Kafka -> silver/gold
   databricks bundle run nba_medallion_dlt   -t work   # one triggered DLT update
   databricks bundle run nba_out_produce     -t work   # emit gold deltas back to Kafka
   databricks bundle run nba_fact_reconcile  -t work
   ```
   `nba-kafka-rollover` is created by `deploy` and runs itself daily at 00:05 UTC.

4. Validate invariants (optional): `test_lake.py` still works as a standalone check.

## Day-2

- **Live demo (always-on):** flip `pipelines.nba_medallion_dlt.continuous: true` and/or run
  `nba_datalake_ingest` with `trigger=continuous` (`databricks bundle run nba_datalake_ingest -t work
  --params trigger=continuous`). Both cost while running; revert for cheap triggered mode.
- **Rebuild gold from scratch:** run `nba_datalake_ingest` with `reset=true`, or a DLT full-refresh.
- **Tear down:** `databricks bundle destroy -t work` removes the pipeline + jobs (DLT-managed tables go
  with the pipeline; the standalone Delta tables in the schema remain — drop the schema to fully reset).

## Relationship to the `run_*.py` scripts

`run_dlt_pipeline.py` and `run_kafka_jobs.py` remain as imperative helpers for ad-hoc operations against
the local PoC workspace. The bundle is the reproducible source of truth for the infrastructure; prefer it
for any fresh setup. They are kept out of the workspace sync (gitignored state files like `databricks.env`,
`.dlt_pipeline_id` are never synced).
