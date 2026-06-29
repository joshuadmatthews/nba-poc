# Databricks notebook source
# NBA lake bootstrap — make a FRESH Unity Catalog workspace ready for the medallion bundle.
#
# Idempotent. Creates the only things the pipeline/jobs assume already exist:
#   - the schema (catalog.schema)            — nothing else creates it
#   - the checkpoint volume (.ckpt)          — the streaming job also self-creates this; here for a clean first run
#   - bronze_member_activity                 — the DLT pipeline's append target (NOT DLT-managed), formerly
#                                              created imperatively by run_dlt_pipeline.py's ensure_bronze()
# Everything else (silver_*, gold_*, dim_definitions, state tables, views) is created by the
# notebooks on first run, so this is the whole bootstrap surface for a brand-new workspace.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "nba_poc")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ns = f"{catalog}.{schema}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ns}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ns}.ckpt")
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {ns}.bronze_member_activity ("
    "  memberId STRING, daysSinceLogin BIGINT, completedTasks BIGINT,"
    "  viewedDashboard BOOLEAN, usedChat BOOLEAN, isDNC BOOLEAN,"
    "  eventTs BIGINT, ingestTs TIMESTAMP) USING DELTA"
)
dbutils.notebook.exit(f"ready: {ns} (schema + ckpt volume + bronze_member_activity)")
