# Databricks notebook source
# Lakeflow Declarative Pipeline (DLT) — the work-faithful NBA medallion.
#
# This is the shape the REAL (high-volume) NBA work env would run: a declarative
# bronze -> silver -> gold pipeline with native CDC, not a hand-rolled loop. The demo
# feeds it low volume; the architecture is the production one.
#
#   bronze_member_activity  (EXTERNAL Delta, fed by the local IN bridge — NOT DLT-managed)
#        |  spark.readStream  (append-only source)
#        v
#   silver_member_activity  (DLT streaming table: typed + deduped)
#        |  explode wide row -> per-attribute NBA facts
#        v
#   gold_facts              (DLT streaming table via APPLY CHANGES, SCD type 1 =
#                            current value per fact; Change Data Feed on -> delta-only out)
#
# Gold row = the NBA fact contract {entityType, entityId, key, value, valueType, eventTs, source}.
# CDF means the OUT bridge reads table_changes(...) and emits ONLY real deltas.
import dlt
from pyspark.sql import functions as F

# bronze is external (the bridge owns it); passed in via pipeline configuration
BRONZE = spark.conf.get("nba.bronze", "workspace.nba_poc.bronze_member_activity")


@dlt.table(comment="typed + deduped member activity (DLT streaming table)")
def silver_member_activity():
    return (spark.readStream.table(BRONZE)
            .where(F.col("memberId").isNotNull())
            .select("memberId", "daysSinceLogin", "completedTasks",
                    "viewedDashboard", "usedChat", "isDNC", "eventTs")
            .dropDuplicates(["memberId", "eventTs"]))


FACTS = [("operator.activity.daysSinceLogin", "daysSinceLogin", "LONG"),
         ("operator.activity.completedTasks", "completedTasks", "LONG"),
         ("operator.activity.viewedDashboard", "viewedDashboard", "BOOL"),
         ("operator.activity.usedChat", "usedChat", "BOOL"),
         ("operator.profile.isDNC", "isDNC", "BOOL")]


@dlt.view
def facts_stream():
    # melt the wide silver row into one row per NBA fact
    cols = [F.struct(F.lit(k).alias("key"),
                     F.col(c).cast("string").alias("value"),
                     F.lit(vt).alias("valueType")) for k, c, vt in FACTS]
    return (dlt.read_stream("silver_member_activity")
            .select("memberId", "eventTs", F.explode(F.array(*cols)).alias("f"))
            .select(F.lit("OPERATOR").alias("entityType"),
                    F.col("memberId").alias("entityId"),
                    F.col("f.key").alias("key"),
                    F.col("f.value").alias("value"),
                    F.col("f.valueType").alias("valueType"),
                    F.col("eventTs"),
                    F.lit("databricks-gold").alias("source")))


# gold = current value per fact; CDF on so the OUT bridge emits only real deltas
dlt.create_streaming_table(
    name="gold_facts",
    comment="current value per NBA fact; CDC delta-only via Change Data Feed",
    table_properties={"delta.enableChangeDataFeed": "true"})

# APPLY CHANGES = declarative upsert keyed by the fact identity, latest-by-eventTs wins.
# SCD type 1 keeps just the current value; a re-arriving identical fact is a no-op (no CDF row).
dlt.apply_changes(
    target="gold_facts",
    source="facts_stream",
    keys=["entityType", "entityId", "key"],
    sequence_by=F.col("eventTs"),
    stored_as_scd_type=1)
