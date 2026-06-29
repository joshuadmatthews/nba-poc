# Databricks notebook source
# COMMS COUNT — per-member weekly comms frequency, owned by the LAKE (replaces the in-pod datalake Redis
# counter). Delta is transactional and already holds every send, so there is ZERO reason for a Redis counter:
# we count sends straight from silver_fact_history (the single transactional source of truth) and PRODUCE the
# frequency facts back via NATIVE spark-kafka (SASL/SCRAM), carrying origin=lake as a HEADER so the streaming
# ingest never re-stores its own telemetry.
#
# A "send" = an nba.actionstate.{actionId}.{channel} workflow-state fact with value='sent' (exactly what the
# retired in-pod datalake counted). Over a rolling 7-day window, per member:
#   operator.comms.totalThisWeek        (all channels)
#   operator.comms.{channel}sThisWeek   (e.g. emailsThisWeek)
# These are the rulefacts the global + channel frequency-cap rules read, so a send tightens eligibility on the
# next pass. count(DISTINCT actionId, eventTs) makes it idempotent — no dedup set needed (the lake is the dedup).
#
# mode=continuous loops every interval_seconds (tight, for testing); mode=triggered emits once.
import json, time
from pyspark.sql import functions as F
dbutils.widgets.text("catalog", "workspace"); dbutils.widgets.text("schema", "nba_poc")
dbutils.widgets.text("bootstrap", ""); dbutils.widgets.text("sasl_user", ""); dbutils.widgets.text("sasl_pass", "")
dbutils.widgets.text("mode", "triggered"); dbutils.widgets.text("interval_seconds", "20")
dbutils.widgets.text("max_seconds", "3300"); dbutils.widgets.text("window_seconds", str(7 * 24 * 3600))
CATALOG = dbutils.widgets.get("catalog"); SCHEMA = dbutils.widgets.get("schema"); NS = f"{CATALOG}.{SCHEMA}"
BOOTSTRAP = dbutils.widgets.get("bootstrap"); USER = dbutils.widgets.get("sasl_user"); PASS = dbutils.widgets.get("sasl_pass")
MODE = dbutils.widgets.get("mode"); INTERVAL = int(dbutils.widgets.get("interval_seconds"))
MAX_SECONDS = int(dbutils.widgets.get("max_seconds")); WINDOW_SECONDS = int(dbutils.widgets.get("window_seconds"))
jaas = f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="{USER}" password="{PASS}";'
SASL = {"kafka.bootstrap.servers": BOOTSTRAP, "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "SCRAM-SHA-256", "kafka.sasl.jaas.config": jaas}

def jstr(c):   # JSON-encode a STRING scalar (to_json only takes complex types) -> "[v]" minus the brackets
    return f"substring(to_json(array({c})), 2, length(to_json(array({c}))) - 2)"

def emit_once():
    now = int(time.time() * 1000)
    since = now - WINDOW_SECONDS * 1000
    # Per (member, channel) send count over the window — DISTINCT (actionId, eventTs) so a redelivered fact
    # can't double-count (the transactional table IS the dedup; no Redis set).
    sends = spark.sql(f"""
        SELECT entityType, entityId, channel, count(*) AS n FROM (
          SELECT DISTINCT entityType, entityId, actionId, channel, eventTs
          FROM {NS}.silver_fact_history
          WHERE key LIKE 'nba.actionstate.%' AND value='sent' AND channel IS NOT NULL AND eventTs >= {since}
        ) GROUP BY entityType, entityId, channel""")

    idm = spark.table(f"{NS}.gold_member_idmap").select("entityId", "nbaId")
    sends = sends.join(idm, "entityId", "left")
    # per-channel facts (operator.comms.{channel}sThisWeek) + per-member total (operator.comms.totalThisWeek)
    perchan = sends.select("entityType", "entityId", "nbaId",
                           F.concat(F.lit("operator.comms."), F.col("channel"), F.lit("sThisWeek")).alias("fkey"),
                           F.col("n"))
    totals = (sends.groupBy("entityType", "entityId", "nbaId").agg(F.sum("n").alias("n"))
              .withColumn("fkey", F.lit("operator.comms.totalThisWeek")))
    allc = perchan.unionByName(totals.select("entityType", "entityId", "nbaId", "fkey", "n"))

    VALUE = (f"""concat('{{',
        '"entityType":', {jstr('entityType')}, ',',
        '"entityId":', {jstr('entityId')}, ',',
        CASE WHEN nbaId IS NULL THEN '' ELSE concat('"nbaId":', {jstr('nbaId')}, ',') END,
        '"key":', {jstr('fkey')}, ',',
        '"value":', cast(n AS string), ',',
        '"valueType":"LONG",',
        '"eventTs":{now},',
        '"source":"comms-lake",',
        '"origin":"lake"}}')""")
    # keyed by memberId (entityType:entityId) so a member's facts co-locate on one partition (matches the
    # snapshot-builder's per-member ownership); origin=lake header so the ingest drops its own re-emission.
    kdf = (allc.selectExpr("concat_ws(':', entityType, entityId) AS key", f"{VALUE} AS value")
           .withColumn("headers", F.array(F.struct(F.lit("origin").alias("key"), F.lit("lake").cast("binary").alias("value")))))
    n_facts = kdf.count()
    kdf.write.format("kafka").options(**SASL).option("topic", "nba.member.facts").option("includeHeaders", "true").save()
    n_members = sends.select("entityId").distinct().count()
    return {"members": n_members, "facts": n_facts, "windowSeconds": WINDOW_SECONDS}

if MODE == "continuous":
    deadline = time.time() + MAX_SECONDS
    cycles = 0; last = {}
    while time.time() < deadline:
        last = emit_once(); cycles += 1
        time.sleep(INTERVAL)
    dbutils.notebook.exit(json.dumps({"mode": "continuous", "cycles": cycles, "last": last, "native_kafka": True}))
else:
    dbutils.notebook.exit(json.dumps({"mode": "triggered", "result": emit_once(), "native_kafka": True}))
