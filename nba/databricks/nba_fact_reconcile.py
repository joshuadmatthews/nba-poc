# Databricks notebook source
# FACT RECONCILE — adapt the whole population's snapshots when the action->fact map changes.
#
# When an action starts using a fact that no action used before, that fact was previously ONLY an ML
# feature (it went down nba.facts, never nba.member.facts), so it isn't in anyone's eligibility snapshot.
# This job detects newly-referenced facts (action_fact_map vs the last known set) and RE-EMITS each one's
# latest value for EVERY member from gold_member_snapshot onto nba.member.facts via NATIVE spark-kafka —
# the snapshot-builder then adds it to every snapshot. The re-emits carry origin=lake (header) so the
# streaming ingest doesn't re-store them (they already live in gold). De-referenced facts are reported;
# the snapshot-builder prunes them.
import json
from pyspark.sql import functions as F
dbutils.widgets.text("catalog", "workspace"); dbutils.widgets.text("schema", "nba_poc")
dbutils.widgets.text("bootstrap", ""); dbutils.widgets.text("sasl_user", ""); dbutils.widgets.text("sasl_pass", "")
CATALOG = dbutils.widgets.get("catalog"); SCHEMA = dbutils.widgets.get("schema"); NS = f"{CATALOG}.{SCHEMA}"
BOOTSTRAP = dbutils.widgets.get("bootstrap"); USER = dbutils.widgets.get("sasl_user"); PASS = dbutils.widgets.get("sasl_pass")
jaas = f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="{USER}" password="{PASS}";'
SASL = {"kafka.bootstrap.servers": BOOTSTRAP, "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "SCRAM-SHA-256", "kafka.sasl.jaas.config": jaas}

spark.sql(f"CREATE TABLE IF NOT EXISTS {NS}.gold_rulefacts_state (fact STRING) USING DELTA")

def jstr(c):
    return f"substring(to_json(array({c})), 2, length(to_json(array({c}))) - 2)"

current = {r["fact"] for r in spark.sql(f"SELECT DISTINCT fact FROM {NS}.action_fact_map WHERE fact LIKE 'operator.%'").collect()}
known = {r["fact"] for r in spark.sql(f"SELECT fact FROM {NS}.gold_rulefacts_state").collect()}
added = current - known
removed = known - current

reemitted = 0
if added:
    inlist = ", ".join("'" + f.replace("'", "''") + "'" for f in added)
    src = spark.sql(f"""SELECT entityType, entityId, key, value, valueType, eventTs, coalesce(source,'reconcile') AS source
        FROM {NS}.gold_member_snapshot WHERE key IN ({inlist})""")
    reemitted = src.count()
    if reemitted:
        kdf = (src.selectExpr("concat_ws(':', entityType, entityId) AS key",   # memberId key: a member's facts co-locate on one partition
                "concat('{',"
                f"'\"entityType\":', {jstr('entityType')}, ',',"
                f"'\"entityId\":', {jstr('entityId')}, ',',"
                f"'\"key\":', {jstr('key')}, ',',"
                "'\"value\":', CASE WHEN value IS NULL THEN 'null' "
                "WHEN valueType='LONG' THEN cast(cast(value AS bigint) AS string) "
                "WHEN valueType='DOUBLE' THEN cast(cast(value AS double) AS string) "
                "WHEN valueType IN ('BOOL','BOOLEAN') THEN CASE WHEN lower(value)='true' THEN 'true' ELSE 'false' END "
                f"ELSE {jstr('value')} END, ',',"
                f"'\"valueType\":', {jstr('valueType')}, ',',"
                "'\"eventTs\":', cast(eventTs AS string), ',',"
                f"'\"source\":', {jstr('source')}, ',',"
                "'\"origin\":\"lake\"}') AS value")
               .withColumn("headers", F.array(F.struct(F.lit("origin").alias("key"), F.lit("lake").cast("binary").alias("value")))))
        kdf.write.format("kafka").options(**SASL).option("topic", "nba.member.facts").option("includeHeaders", "true").save()

spark.sql(f"DELETE FROM {NS}.gold_rulefacts_state")
if current:
    spark.createDataFrame([(f,) for f in current], ["fact"]).write.format("delta").mode("append").saveAsTable(f"{NS}.gold_rulefacts_state")
dbutils.notebook.exit(json.dumps({"added": sorted(added), "removed": sorted(removed), "reemitted": reemitted, "native_kafka": True}))
