# Databricks notebook source
# THROTTLE EMIT — the lake is the ONE place that sees every send, so it owns the channel-throttle count.
# Serverless counts the GLOBAL per-channel daily send level from silver and PRODUCES it back as facts
# (nba.throttle.{channel}.{daily|rate}) via NATIVE spark-kafka (SASL/SCRAM), carrying origin=lake as a
# HEADER so the streaming ingest never re-stores its own telemetry.
#
# It counts disposition=sent by channel (NBA's own sends + external, normalized on ingest). The level
# rides nba.member.facts; the snapshot-builder forwards nba.throttle.* to nba.definitions, which the rules
# engine + Temporal worker read as a population-wide channel cap.
#
# mode=continuous loops every interval_seconds (tight, for testing); mode=triggered emits once.
import json, time
from pyspark.sql import functions as F
dbutils.widgets.text("catalog", "workspace"); dbutils.widgets.text("schema", "nba_poc")
dbutils.widgets.text("bootstrap", ""); dbutils.widgets.text("sasl_user", ""); dbutils.widgets.text("sasl_pass", "")
dbutils.widgets.text("mode", "triggered"); dbutils.widgets.text("interval_seconds", "20")
dbutils.widgets.text("max_seconds", "3300"); dbutils.widgets.text("window_seconds", "300")
CATALOG = dbutils.widgets.get("catalog"); SCHEMA = dbutils.widgets.get("schema"); NS = f"{CATALOG}.{SCHEMA}"
BOOTSTRAP = dbutils.widgets.get("bootstrap"); USER = dbutils.widgets.get("sasl_user"); PASS = dbutils.widgets.get("sasl_pass")
MODE = dbutils.widgets.get("mode"); INTERVAL = int(dbutils.widgets.get("interval_seconds"))
MAX_SECONDS = int(dbutils.widgets.get("max_seconds")); WINDOW_SECONDS = int(dbutils.widgets.get("window_seconds"))
jaas = f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="{USER}" password="{PASS}";'
SASL = {"kafka.bootstrap.servers": BOOTSTRAP, "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "SCRAM-SHA-256", "kafka.sasl.jaas.config": jaas}
CHANNELS = ["email", "sms", "push", "mail", "voice"]

def counts(since_ms):
    rows = spark.sql(f"""
        SELECT channel, count(distinct entityId, actionId, channel, eventTs) AS sent
        FROM {NS}.silver_fact_history
        WHERE factClass='disposition' AND value='sent' AND channel IS NOT NULL AND eventTs >= {since_ms}
        GROUP BY channel""").collect()
    return {r["channel"]: int(r["sent"]) for r in rows}

def emit_once():
    now = int(time.time() * 1000)
    day_start = (now // 86400000) * 86400000
    daily = counts(day_start); rate = counts(now - WINDOW_SECONDS * 1000)
    facts = []   # (kafka key, fact key, value)
    for ch in sorted(set(CHANNELS) | set(daily.keys()) | set(rate.keys())):
        for metric, val in (("daily", daily.get(ch, 0)), ("rate", rate.get(ch, 0))):
            k = f"nba.throttle.{ch}.{metric}"
            facts.append((f"SYSTEM:__throttle:{k}", k, int(val)))
    df = spark.createDataFrame(facts, "kkey string, fkey string, val long")
    kdf = (df.selectExpr("kkey AS key",
            f"""concat('{{',
              '"entityType":"SYSTEM",', '"entityId":"__throttle",',
              '"key":', substring(to_json(array(fkey)), 2, length(to_json(array(fkey))) - 2), ',',
              '"value":', cast(val AS string), ',',
              '"valueType":"LONG",',
              '"eventTs":{now},',
              '"source":"throttle-lake",', '"origin":"lake",',
              '"windowSeconds":{WINDOW_SECONDS}}}') AS value""")
           .withColumn("headers", F.array(F.struct(F.lit("origin").alias("key"), F.lit("lake").cast("binary").alias("value")))))
    kdf.write.format("kafka").options(**SASL).option("topic", "nba.member.facts").option("includeHeaders", "true").save()
    return {"daily": daily, "rate": rate, "window": WINDOW_SECONDS}

if MODE == "continuous":
    deadline = time.time() + MAX_SECONDS
    cycles = 0; last = {}
    while time.time() < deadline:
        last = emit_once(); cycles += 1
        time.sleep(INTERVAL)
    dbutils.notebook.exit(json.dumps({"mode": "continuous", "cycles": cycles, "last": last}))
else:
    dbutils.notebook.exit(json.dumps({"mode": "triggered", "counts": emit_once()}))
