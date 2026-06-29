# Databricks notebook source
# OUT — STREAM the live member facts (gold_member_snapshot CDF) and PRODUCE deltas to Kafka via native
# spark-kafka. This is the gold -> member.facts BRIDGE that closes the loop, so it flows "as data arrives".
#
# SOURCE = gold_member_snapshot: the DENORMALIZED, key-value (EAV) current-value-per-(member,key) table the
# streaming ingest MERGE-upserts as facts arrive (entityType/entityId/key/value/valueType/eventTs/source, CDF
# on). Denormalized = dynamic schema: 160+ distinct fact keys coexist as ROWS, no per-fact columns, no schema
# migration when a new key appears. The snapshot IS the facts (the latest-per-key MERGE of silver_fact_history,
# the append-only event log). The old DLT `gold_facts` twin (+ its empty bronze_member_activity source) is a
# redundant second copy and is retired — do NOT point this at it.
#
# Structured Streaming, mirroring nba_datalake_stream: readStream.readChangeFeed + foreachBatch + a UC-volume
# CHECKPOINT (exactly-once CDF offsets — no hand-rolled watermark). BOOTSTRAP on first start (no checkpoint):
# emit the FULL current snapshot once to re-sync the local loop (it goes stale whenever this bridge is down),
# then stream deltas from the current head. Emit inserts + value-changed updates only (pre/post-image join),
# origin=lake so the ingest drops the echo pre-deserialize (no lake re-ingest; local is LWW by eventTs).
#
# Serverless forbids an infinite (processingTime) trigger, so "continuous" is a LOOP of availableNow drains
# (each drain processes what arrived since the checkpoint, then sleeps) — identical to the ingest + source-sim.
# trigger=availableNow (no loop) = a single drain + stop (backfill).
import json, time
from pyspark.sql import functions as F
dbutils.widgets.text("catalog", "workspace"); dbutils.widgets.text("schema", "nba_poc")
dbutils.widgets.text("bootstrap", ""); dbutils.widgets.text("sasl_user", ""); dbutils.widgets.text("sasl_pass", "")
dbutils.widgets.text("trigger", "continuous"); dbutils.widgets.text("interval_seconds", "10")
dbutils.widgets.text("reset", "false")     # true (post clean-reset): clear the checkpoint so the bridge re-bootstraps
CATALOG = dbutils.widgets.get("catalog"); SCHEMA = dbutils.widgets.get("schema"); NS = f"{CATALOG}.{SCHEMA}"
BOOTSTRAP = dbutils.widgets.get("bootstrap"); USER = dbutils.widgets.get("sasl_user"); PASS = dbutils.widgets.get("sasl_pass")
TRIGGER = (dbutils.widgets.get("trigger") or "continuous").strip()
INTERVAL = int(dbutils.widgets.get("interval_seconds") or "10")
RESET = dbutils.widgets.get("reset").strip().lower() == "true"
jaas = f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="{USER}" password="{PASS}";'
SASL = {"kafka.bootstrap.servers": BOOTSTRAP, "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "SCRAM-SHA-256", "kafka.sasl.jaas.config": jaas}
SRC = f"{NS}.gold_member_snapshot"
cols = ["entityType", "entityId", "key", "value", "valueType", "eventTs", "source"]

# UC-volume checkpoint (no DBFS) — exactly-once CDF offset tracking across triggered drains.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {NS}.ckpt")
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/ckpt/out_produce_snap"
if RESET:   # clean-reset truncated gold_member_snapshot -> the stored CDF offset is stale; drop it to force re-bootstrap
    try: dbutils.fs.rm(CKPT, True); print("reset=true: cleared out_produce_snap checkpoint -> will re-bootstrap")
    except Exception as _e: print(f"reset=true: checkpoint clear skipped ({str(_e)[:80]})")

# Render each fact to the SAME JSON shape the bridges produced, with the VALUE typed per valueType (so a
# LONG/BOOL/DOUBLE stays a JSON number/bool, not a quoted string), plus origin=lake. One spark-kafka write
# per topic carries the whole batch + the origin header.
def jstr(c):   # JSON-encode a STRING scalar (to_json only takes complex types) -> "[v]" minus the brackets
    return f"substring(to_json(array({c})), 2, length(to_json(array({c}))) - 2)"
VALUE_JSON = ("concat('{',"
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
  "'\"origin\":\"lake\"}')")

def emit_df(df):
    n = df.count()
    if n == 0: return 0
    # keyed by memberId (entityType:entityId) so a member's facts co-locate on one partition (matches the
    # snapshot-builder's per-member ownership); the fact key rides the body. Topics are transport, not a
    # per-fact store — compaction keeps the latest message per member, full per-fact state lives in gold.
    kdf = (df.selectExpr("concat_ws(':', entityType, entityId) AS key", f"{VALUE_JSON} AS value")
           .withColumn("headers", F.array(F.struct(F.lit("origin").alias("key"), F.lit("lake").cast("binary").alias("value")))))
    for topic in ("nba.facts", "nba.member.facts"):
        (kdf.write.format("kafka").options(**SASL).option("topic", topic).option("includeHeaders", "true").save())
    return n

# foreachBatch: each CDF micro-batch -> emit inserts + value-changed updates (drop MERGE no-op re-writes via
# the pre/post-image join on the commit version), origin=lake.
def emit_changed(cdf_df, batch_id):
    cdf_df = cdf_df.persist()
    try:
        post = cdf_df.filter("_change_type IN ('insert','update_postimage')")
        pre = (cdf_df.filter("_change_type = 'update_preimage'")
               .select("entityType", "entityId", "key", "_commit_version", F.col("value").alias("_preval")))
        changed = (post.join(pre, ["entityType", "entityId", "key", "_commit_version"], "left")
                   .where("_preval IS NULL OR _preval <> value").select(*cols))
        n = emit_df(changed)
        if n: print(f"batch {batch_id}: emitted {n} delta facts -> nba.facts + nba.member.facts")
    finally:
        cdf_df.unpersist()

def ckpt_exists():
    try: return len(dbutils.fs.ls(CKPT)) > 0
    except Exception: return False

# BOOTSTRAP (first start, no checkpoint): the local loop goes stale while this bridge is down, so emit the
# FULL current snapshot once to re-sync every member, and start the CDF stream from the current head (overlap
# at that version is idempotent — local is LWW by eventTs). Once the checkpoint exists, it owns offsets.
START_VER = 0
if not ckpt_exists():
    cur = spark.sql(f"SELECT max(version) AS v FROM (DESCRIBE HISTORY {SRC})").collect()[0]["v"]
    START_VER = int(cur) if cur is not None else 0
    n = emit_df(spark.table(SRC).select(*cols))
    print(f"bootstrap: emitted full snapshot ({n} facts) -> member.facts; CDF stream from version {START_VER}")

qb = (spark.readStream.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", START_VER)        # ignored once the checkpoint exists
      .option("maxFilesPerTrigger", 2000)          # bound micro-batch size
      .table(SRC)
      .writeStream.foreachBatch(emit_changed)
      .option("checkpointLocation", CKPT))

if TRIGGER == "continuous":
    while True:
        try:
            qb.trigger(availableNow=True).start().awaitTermination()
        except Exception as e:
            # SELF-HEAL: out-produce writes over the same external tunnel as the ingest; a mid-stream reconnect
            # throws and — without this — the run DIES (no auto-restart) and the loop freezes. Catch + resume
            # from the SAME checkpoint within the run.
            print(f"[out-produce] drain failed ({type(e).__name__}: {str(e)[:180]}); resuming from checkpoint...")
        time.sleep(INTERVAL)
else:
    qb.trigger(availableNow=True).start().awaitTermination()
    dbutils.notebook.exit(json.dumps({"native_kafka": True, "start_version": START_VER}))
