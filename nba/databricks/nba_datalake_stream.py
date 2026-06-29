# Databricks notebook source
# NBA DATALAKE INGEST — UC-native STRUCTURED STREAMING over NATIVE Kafka.
#
# Replaces the pandaproxy REST poll/drain with spark.readStream.format("kafka") (SASL/SCRAM) +
# includeHeaders, trigger(availableNow) + a UC-VOLUME checkpoint (exactly-once offsets, no manual
# consumer-group/max_bytes heuristics). The lake's OWN re-emissions (origin=lake) are dropped by
# HEADER, pre-deserialize. Same typed silver + gold + idmap + defs as the batch ingest.
#
#   reset=true  -> truncate silver/gold + clear the checkpoint, rebuild from earliest (cutover handoff)
#   reset=false -> resume from the checkpoint (incremental)
import json, time
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

def _s(*names_types):
    return StructType([StructField(n, t, True) for n, t in names_types])
S, L, D = StringType(), LongType(), DoubleType()
FACTS_SCHEMA = _s(("entityType",S),("entityId",S),("key",S),("value",S),("valueType",S),("eventTs",L),
                  ("source",S),("topic",S),("factClass",S),("actionId",S),("channel",S),("scoreVal",D),("contentKey",S))
EVALS_SCHEMA = _s(("nbaId",S),("actionId",S),("channel",S),("name",S),("evaluatedAt",L),("correlationId",S))
ACTS_SCHEMA  = _s(("nbaId",S),("entityType",S),("entityId",S),("op",S),("actionId",S),("channel",S),
                  ("name",S),("score",D),("source",S),("eventTs",L),("correlationId",S),("contentKey",S))
SNAPS_SCHEMA = _s(("nbaId",S),("entityType",S),("entityId",S),("updatedTs",L),("factCount",L),("correlationId",S),("factsJson",S))
MILES_SCHEMA = _s(("nbaId",S),("milestoneId",S),("name",S),("completedAt",L))
DEFS_SCHEMA  = _s(("id",S),("defType",S),("name",S),("channel",S),("ttlSeconds",L),("channelsJson",S),
                  ("inclusionJson",S),("exclusionJson",S),("logicJson",S),("factsUsedJson",S),("updatedTs",L))
IDMAP_SCHEMA = _s(("entityType",S),("entityId",S),("nbaId",S))

dbutils.widgets.text("catalog", "workspace"); dbutils.widgets.text("schema", "nba_poc")
dbutils.widgets.text("bootstrap", ""); dbutils.widgets.text("sasl_user", ""); dbutils.widgets.text("sasl_pass", "")
# INPUT topics: the external source stream (datalake.streaming-inbound) is the lake's raw ingress; member.facts
# is tailed for ANALYTICS (internal scores/states/dispositions -> silver/gold + comms/throttle counts). We do
# NOT read nba.facts — the lake EMITS nba.facts now (the all-facts feature stream for a future ML layer).
dbutils.widgets.text("topics", "datalake.streaming-inbound,nba.member.facts,nba.snapshots,nba.evaluations,nba.activations,nba.definitions,nba.model.card")
dbutils.widgets.text("reset", "false")
dbutils.widgets.text("start_offsets", "earliest")   # "latest" + reset=true => fresh start at the live edge (clean slate)
CATALOG = dbutils.widgets.get("catalog"); SCHEMA = dbutils.widgets.get("schema"); NS = f"{CATALOG}.{SCHEMA}"
BOOTSTRAP = dbutils.widgets.get("bootstrap"); USER = dbutils.widgets.get("sasl_user"); PASS = dbutils.widgets.get("sasl_pass")
TOPICS = ",".join(t.strip() for t in dbutils.widgets.get("topics").split(",") if t.strip())
RESET = dbutils.widgets.get("reset").lower() == "true"
# trigger=availableNow (drain + stop, for backfill/triggered) | continuous (processingTime — live, the EVENT
# path emits counts as sends stream in). Boundary rollovers (midnight/week reset) are a separate scheduled job.
dbutils.widgets.text("trigger", "availableNow"); dbutils.widgets.text("interval_seconds", "20")
dbutils.widgets.text("rate_window_seconds", "300"); dbutils.widgets.text("comms_window_seconds", str(7 * 24 * 3600))
TRIGGER = dbutils.widgets.get("trigger"); INTERVAL = int(dbutils.widgets.get("interval_seconds"))
RATE_WINDOW = int(dbutils.widgets.get("rate_window_seconds")); COMMS_WINDOW = int(dbutils.widgets.get("comms_window_seconds"))
jaas = f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="{USER}" password="{PASS}";'
SASL = {"kafka.bootstrap.servers": BOOTSTRAP, "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "SCRAM-SHA-256", "kafka.sasl.jaas.config": jaas}
AOPT = "TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)"

# --- silver / gold DDL (identical to the batch ingest) ---
spark.sql(f"""CREATE TABLE IF NOT EXISTS {NS}.silver_fact_history (
  entityType STRING, entityId STRING, key STRING, value STRING, valueType STRING,
  eventTs BIGINT, source STRING, topic STRING,
  factClass STRING, actionId STRING, channel STRING, scoreVal DOUBLE, contentKey STRING, ingestTs TIMESTAMP)
  USING DELTA CLUSTER BY (factClass, entityId, eventTs) {AOPT}""")
for _t in ("silver_fact_history", "silver_activations"):
    try: spark.sql(f"ALTER TABLE {NS}.{_t} ADD COLUMNS (contentKey STRING)")
    except Exception: pass
spark.sql(f"""CREATE TABLE IF NOT EXISTS {NS}.silver_eval_eligible (
  nbaId STRING, actionId STRING, channel STRING, name STRING, evaluatedAt BIGINT,
  correlationId STRING, ingestTs TIMESTAMP) USING DELTA CLUSTER BY (actionId, channel) {AOPT}""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {NS}.silver_activations (
  nbaId STRING, entityType STRING, entityId STRING, op STRING, actionId STRING, channel STRING,
  name STRING, score DOUBLE, source STRING, eventTs BIGINT, correlationId STRING, contentKey STRING, ingestTs TIMESTAMP)
  USING DELTA CLUSTER BY (actionId, op) {AOPT}""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {NS}.silver_snapshots (
  nbaId STRING, entityType STRING, entityId STRING, updatedTs BIGINT, factCount BIGINT,
  correlationId STRING, factsJson STRING, ingestTs TIMESTAMP) USING DELTA CLUSTER BY (correlationId) {AOPT}""")
# milestone COMPLETIONS (permanent) — one row per (member, milestone), the first completedAt the eval carried.
spark.sql(f"""CREATE TABLE IF NOT EXISTS {NS}.silver_milestones (
  nbaId STRING, milestoneId STRING, name STRING, completedAt BIGINT, ingestTs TIMESTAMP)
  USING DELTA CLUSTER BY (nbaId) {AOPT}""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {NS}.dim_definitions (
  id STRING, defType STRING, name STRING, channel STRING, ttlSeconds BIGINT,
  channelsJson STRING, inclusionJson STRING, exclusionJson STRING, logicJson STRING,
  factsUsedJson STRING, updatedTs BIGINT) USING DELTA CLUSTER BY (defType, id) {AOPT}""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {NS}.gold_member_idmap (
  entityType STRING, entityId STRING, nbaId STRING, updatedTs TIMESTAMP)
  USING DELTA CLUSTER BY (entityId) {AOPT}""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {NS}.gold_member_snapshot (
  entityType STRING, entityId STRING, nbaId STRING, key STRING, value STRING, valueType STRING,
  eventTs BIGINT, source STRING) USING DELTA CLUSTER BY (nbaId, key)
  TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true)""")
# gold_model_card: the ML model card (champion/challenger versions + AUC, per-archetype model_p vs f*, learning
# curve, adaptation status) emitted by the Databricks ML job to Kafka nba.model.card. One JSON row = the latest
# card; gold_model_card_history keeps every emission for the version time-series. The command-center BFF queries
# the latest at page load — the usual gold→BFF analytics path (no cross-workspace reads).
spark.sql(f"CREATE TABLE IF NOT EXISTS {NS}.gold_model_card (card STRING, ts TIMESTAMP) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {NS}.gold_model_card_history (card STRING, ts TIMESTAMP) USING DELTA")

# UC-volume checkpoint (no DBFS) — exactly-once offset tracking across triggered runs.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {NS}.ckpt")
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/ckpt/datalake_ingest"

if RESET:
    for t in ("silver_fact_history", "silver_eval_eligible", "silver_activations", "silver_snapshots",
              "silver_milestones", "dim_definitions", "gold_member_snapshot", "gold_member_idmap"):
        spark.sql(f"TRUNCATE TABLE {NS}.{t}")
    try: dbutils.fs.rm(CKPT, True)
    except Exception: pass

# --- per-record parse helpers (identical semantics to the batch ingest) ---
def fact_dims(key):
    if key.startswith("operator.activity"): return "activity", None, None
    if key.startswith("operator.comms"): return "comms", None, None
    if key.startswith("operator.profile"): return "profile", None, None
    if key.startswith(("nba.score", "nba.actionstate", "nba.disposition")):
        p = key.split("."); return p[1], (p[2] if len(p) > 2 else None), (p[3] if len(p) > 3 else None)
    if key.startswith("comms.sent."):
        p = key.split("."); return "disposition", "external", (p[2] if len(p) > 2 else None)
    return "other", None, None

def normalize_send_value(key, sval):
    return "sent" if key.startswith("comms.sent.") else sval

# ---- MEDALLION bronze->silver normalization: heterogeneous raw source records -> canonical NBA facts. ----
# datalake.streaming-inbound carries whatever shape each source system speaks (a CRM export blob, a flat billing
# row, a product-event envelope, a support ticket). This is the WHOLE POINT of the lake: it reconciles those
# shapes into one canonical fact vocabulary ({entityType,entityId,key,value,valueType,eventTs,source}). Add a
# branch here when you onboard a new source system. Unknown shapes are dropped (logged via the batch counter).
def _vt(v):
    if isinstance(v, bool): return "BOOLEAN"
    if isinstance(v, int): return "LONG"
    if isinstance(v, float): return "DOUBLE"
    return "STRING"

def normalize_inbound(raw, default_ts):
    if not isinstance(raw, dict): return []
    src = str(raw.get("_src") or raw.get("system") or raw.get("source") or "unknown").lower()
    ts = int(raw.get("ts") or raw.get("eventTs") or raw.get("timestamp") or default_ts)
    out = []
    def fact(eid, key, val):
        if eid is None or val is None: return
        out.append({"entityType": "OPERATOR", "entityId": str(eid), "key": key, "value": val,
                    "valueType": _vt(val), "eventTs": ts, "source": "lake:" + src})
    # --- CRM export: a contactId with a nested fields{} blob (snake/camel keys vary by export) ---
    if raw.get("contactId") or raw.get("contact_id") or src.startswith("crm"):
        eid = raw.get("contactId") or raw.get("contact_id") or raw.get("id")
        for k, v in (raw.get("fields") or raw.get("attributes") or {}).items():
            fact(eid, _crm_key(k) or ("operator.activity." + _camel(k)), v)
    # --- billing: a flat account row (plan / mrr / seats / status) ---
    elif raw.get("account") or raw.get("customer") or src == "billing":
        eid = raw.get("account") or raw.get("customer") or raw.get("customerId")
        for k in ("plan", "mrr", "seats", "status", "trialDaysLeft"):
            if raw.get(k) is not None: fact(eid, "operator." + k, raw[k])
    # --- product events: an event verb + props bag keyed by user ---
    elif raw.get("event") or src.startswith("product") or src.startswith("events"):
        eid = raw.get("user") or raw.get("uid") or raw.get("userId")
        ev, props = raw.get("event"), (raw.get("props") or raw.get("properties") or {})
        if ev: fact(eid, "operator.lastEvent", str(ev))
        for k, v in props.items(): fact(eid, "operator." + _camel(k), v)
    # --- support: a ticket with account + csat/priority ---
    elif raw.get("ticket") or src == "support":
        t = raw.get("ticket") or raw
        eid = t.get("account") or t.get("accountId")
        for k in ("csat", "priority", "openTickets"):
            if t.get(k) is not None: fact(eid, "operator." + k, t[k])
    # --- already-canonical fact (a source that speaks our vocabulary natively) ---
    elif raw.get("entityId") and raw.get("key"):
        v = raw.get("value")
        out.append({"entityType": raw.get("entityType", "OPERATOR"), "entityId": str(raw["entityId"]),
                    "key": raw["key"], "value": v, "valueType": raw.get("valueType") or _vt(v),
                    "eventTs": ts, "source": "lake:" + src})
    return out

def _camel(k):
    parts = str(k).replace("-", "_").split("_")
    return parts[0] + "".join(p[:1].upper() + p[1:] for p in parts[1:]) if len(parts) > 1 else parts[0]

# A source field (in whatever case the source speaks) -> the canonical NBA fact key the rules actually use.
# This map IS the medallion's reconciliation: source dialect -> one governed vocabulary.
CRM_MAP = {
    "dayssincelogin": "operator.activity.daysSinceLogin",
    "completedtasks": "operator.activity.completedTasks",
    "vieweddashboard": "operator.activity.viewedDashboard",
    "usedchat": "operator.activity.usedChat",
    "isdnc": "operator.profile.isDNC",
    "smsconsent": "operator.profile.smsConsent",
    "emailsthisweek": "operator.comms.emailsThisWeek",
    "totalcommsthisweek": "operator.comms.totalThisWeek",
}

def _crm_key(field):
    return CRM_MAP.get(str(field).replace("_", "").replace("-", "").lower())

fcols = ["entityType","entityId","key","value","valueType","eventTs","source","topic","factClass","actionId","channel","scoreVal","contentKey"]

# ---- EVENT-PATH frequency counts: a send that lands in a micro-batch immediately re-emits the affected
# throttle level / member comms count, straight from the transactional silver table (origin=lake header so
# the ingest never re-stores its own emission). Boundary ROLLOVERS (daily reset at midnight, weekly reset)
# are the scheduled rollover job's job — a stream fires on new data, not on the wall clock advancing. ----
def _jstr(c):
    return f"substring(to_json(array({c})), 2, length(to_json(array({c}))) - 2)"

def _produce(kdf, topic="nba.member.facts"):
    kdf.write.format("kafka").options(**SASL).option("topic", topic).option("includeHeaders", "true").save()

THROTTLE_CHANNELS = ["email", "sms", "push", "mail", "voice"]

def emit_throttle(now):
    """Population channel level: today's sends (daily ceiling) + last-window sends (rate gate), per channel.
    Counts at SEND TIME — the IN_PROCESS actionstate the workflow emits on DISPATCH ("activation sent, no
    response yet"). We do NOT wait for a downstream delivery disposition ('sent'/Delivered): rate-limiting
    can't depend on the provider confirmation coming back in time, or we overshoot the cap before it lands.
    A send that falls back to FAILED (bounce/no-answer) or SUPPRESSED (pulled) gives its token back."""
    day_start = (now // 86400000) * 86400000
    def counts(since):
        # net = IN_PROCESS (attempted sends) minus the ones that didn't really go out (FAILED / SUPPRESSED).
        return {r["channel"]: int(r["sent"]) for r in spark.sql(f"""
            SELECT channel, greatest(0, count_if(value='IN_PROCESS') - count_if(value IN ('FAILED','SUPPRESSED'))) AS sent
            FROM (SELECT DISTINCT entityId, actionId, channel, eventTs, value FROM {NS}.silver_fact_history
                  WHERE factClass='actionstate' AND value IN ('IN_PROCESS','FAILED','SUPPRESSED')
                    AND channel IS NOT NULL AND eventTs >= {since})
            GROUP BY channel""").collect()}
    daily, rate = counts(day_start), counts(now - RATE_WINDOW * 1000)
    # BROADCAST the population-global level STRAIGHT onto nba.definitions — the topic every rules-engine pod
    # and the Temporal gate tail — keyed THROTTLE:{ch}.{metric}, EXACTLY as the snapshot-builder would route it
    # (SnapshotBuilder.classify -> "THROTTLE:" + key-suffix, raw body as value; RulesEngine.applyThrottle parses
    # it). It must NOT ride member.facts: only rule-USED facts get re-emitted there, and a level smuggled onto
    # one member's snapshot wouldn't reach the other pods. Same body the consumers already expect.
    rows = [(f"THROTTLE:{ch}.{m}", f"nba.throttle.{ch}.{m}", int(v))
            for ch in sorted(set(THROTTLE_CHANNELS) | set(daily) | set(rate))
            for m, v in (("daily", daily.get(ch, 0)), ("rate", rate.get(ch, 0)))]
    df = spark.createDataFrame(rows, "kkey string, fkey string, val long")
    kdf = (df.selectExpr("kkey AS key",
            f"""concat('{{', '"entityType":"SYSTEM",', '"entityId":"__throttle",',
              '"key":', {_jstr('fkey')}, ',', '"value":', cast(val AS string), ',',
              '"valueType":"LONG",', '"eventTs":{now},', '"source":"throttle-lake",', '"origin":"lake",',
              '"windowSeconds":{RATE_WINDOW}}}') AS value""")
           .withColumn("headers", F.array(F.struct(F.lit("origin").alias("key"), F.lit("lake").cast("binary").alias("value")))))
    _produce(kdf, "nba.definitions")

def emit_comms(members, now):
    """Per-member rolling-week comms count (total + per channel), scoped to the members touched this batch."""
    since = now - COMMS_WINDOW * 1000
    inlist = ", ".join("'" + m.replace("'", "''") + "'" for m in members)
    sends = spark.sql(f"""SELECT entityType, entityId, channel, count(*) AS n FROM (
        SELECT DISTINCT entityType, entityId, actionId, channel, eventTs FROM {NS}.silver_fact_history
        WHERE key LIKE 'nba.actionstate.%' AND value='sent' AND channel IS NOT NULL AND eventTs >= {since}
          AND entityId IN ({inlist})) GROUP BY entityType, entityId, channel""")
    idm = spark.table(f"{NS}.gold_member_idmap").select("entityId", "nbaId")
    sends = sends.join(idm, "entityId", "left")
    perchan = sends.select("entityType", "entityId", "nbaId",
                           F.concat(F.lit("operator.comms."), F.col("channel"), F.lit("sThisWeek")).alias("fkey"), F.col("n"))
    totals = (sends.groupBy("entityType", "entityId", "nbaId").agg(F.sum("n").alias("n"))
              .withColumn("fkey", F.lit("operator.comms.totalThisWeek")))
    allc = perchan.unionByName(totals.select("entityType", "entityId", "nbaId", "fkey", "n"))
    VALUE = (f"""concat('{{', '"entityType":', {_jstr('entityType')}, ',', '"entityId":', {_jstr('entityId')}, ',',
        CASE WHEN nbaId IS NULL THEN '' ELSE concat('"nbaId":', {_jstr('nbaId')}, ',') END,
        '"key":', {_jstr('fkey')}, ',', '"value":', cast(n AS string), ',', '"valueType":"LONG",',
        '"eventTs":{now},', '"source":"comms-lake",', '"origin":"lake"}}')""")
    kdf = (allc.selectExpr("concat_ws(':', entityType, entityId) AS key", f"{VALUE} AS value")
           .withColumn("headers", F.array(F.struct(F.lit("origin").alias("key"), F.lit("lake").cast("binary").alias("value")))))
    _produce(kdf)

def _origin_hdr():
    return F.array(F.struct(F.lit("origin").alias("key"), F.lit("lake").cast("binary").alias("value")))

def emit_inbound(canon):
    """Re-emit the action-mapped subset of medallion-normalized facts -> nba.member.facts (the snapshot-builder's
    only input). 'Action-mapped' = the fact key appears in some action/rule's factsUsed. On a cold start (no
    definitions ingested yet) we emit all to member.facts so the pipeline still flows. The origin=lake header
    keeps the lake from re-ingesting its own emissions.
    NOTE: we used to ALSO re-emit every fact to nba.facts (the "all-facts feature stream for a future ML layer").
    That's RETIRED — the ML layer reads features from Unity Catalog now, nothing consumes nba.facts, and the
    firehose just burned disk + starved consumers. Member-facts (the live pipeline input) is unaffected."""
    try:
        mapped = {r["fact"] for r in spark.sql(f"""
            SELECT DISTINCT explode(from_json(factsUsedJson, 'array<string>')) AS fact
            FROM {NS}.dim_definitions WHERE factsUsedJson IS NOT NULL""").collect()}
    except Exception:
        mapped = set()
    def kv(cf):
        body = {k: cf[k] for k in ("entityType", "entityId", "key", "value", "valueType", "eventTs", "source")}
        return (cf["entityType"] + ":" + cf["entityId"], json.dumps(body))
    member = [kv(cf) for cf in canon if (not mapped) or (cf["key"] in mapped)]
    if member:
        _produce(spark.createDataFrame(member, "key string, value string").withColumn("headers", _origin_hdr()), "nba.member.facts")
    return len(canon), len(member)

def process(batch_df, batch_id):
    # HEADER ROUTING: drop the lake's own re-emissions (any record carrying an 'origin' header) BEFORE
    # deserializing. This is the native-Kafka win the REST proxy couldn't do.
    rows = (batch_df
            .where("headers IS NULL OR NOT exists(headers, h -> string(h.key) = 'origin')")
            .selectExpr("topic", "CAST(key AS STRING) k", "CAST(value AS STRING) v")
            .collect())
    facts, evals, acts, snaps, mstones = [], [], [], [], []
    inbound = []   # canonical facts normalized from datalake.streaming-inbound, to re-emit on nba.facts/member.facts
    cards = []     # ML model-card JSON blobs from nba.model.card (latest -> gold_model_card)
    defs = {}
    def add_act(d):
        r = d["actions"] if isinstance(d.get("actions"), list) and d["actions"] else [d]
        for ba in r:
            sc = ba.get("score")
            acts.append((d.get("nbaId"), d.get("entityType"), d.get("entityId"), d.get("op"),
                         ba.get("actionId"), d.get("channel"), ba.get("name"),
                         float(sc) if isinstance(sc, (int, float)) else None, d.get("source"),
                         int(d.get("eventTs", 0)), d.get("correlationId"), ba.get("contentKey")))
    for row in rows:
        topic = row["topic"]; mkey = row["k"] or ""
        try: d = json.loads(row["v"])
        except Exception: continue
        if not isinstance(d, dict): continue
        if d.get("origin") == "lake": continue   # belt-and-suspenders if a producer set origin in the body only
        if topic == "nba.model.card":            # ML model card (one JSON blob per emission) -> gold_model_card
            cards.append(row["v"]); continue
        if topic == "datalake.streaming-inbound":
            # MEDALLION: a raw source record -> 0..N canonical facts. Each lands in silver_fact_history AND is
            # queued for re-emission to nba.facts (all) + nba.member.facts (action-mapped) below.
            for cf in normalize_inbound(d, int(time.time() * 1000)):
                cls, aid, ch = fact_dims(cf["key"])
                facts.append((cf["entityType"], cf["entityId"], cf["key"], normalize_send_value(cf["key"], str(cf["value"])),
                              cf["valueType"], int(cf["eventTs"]), cf["source"], "nba.member.facts", cls, aid, ch, None, None))
                inbound.append(cf)
            continue
        if topic in ("nba.facts", "nba.member.facts") and d.get("entityId") and d.get("key"):
            v = d.get("value")
            sval = json.dumps(v) if isinstance(v, (dict, list)) else (None if v is None else str(v))
            cls, aid, ch = fact_dims(d["key"]); score = None
            sval = normalize_send_value(d["key"], sval)
            if isinstance(v, dict) and v.get("score") is not None:
                try: score = float(v["score"])
                except Exception: score = None
            facts.append((d.get("entityType", "OPERATOR"), d["entityId"], d["key"], sval,
                          d.get("valueType"), int(d.get("eventTs", 0)), d.get("source"),
                          topic, cls, aid, ch, score, d.get("contentKey")))
        elif topic == "nba.evaluations":
            ev = int(d.get("evaluatedAt", 0))
            # the eval carries a unified channelActions[]; silver_eval_eligible tracks the ELIGIBLE ones.
            for e in (d.get("channelActions") or []):
                if not e.get("eligible"): continue
                evals.append((d.get("nbaId"), e.get("actionId"), e.get("channel"), e.get("name"), ev, d.get("correlationId")))
            for m in (d.get("milestones") or []):   # permanent milestone completions ride the eval
                mstones.append((d.get("nbaId"), m.get("id"), m.get("name"), int(m.get("completedAt", 0))))
        elif topic == "nba.activations":
            add_act(d)
        elif topic in ("nba.facts", "nba.member.facts") and d.get("op"):
            add_act(d)
        elif topic == "nba.snapshots":
            snaps.append((d.get("nbaId"), d.get("entityType"), d.get("entityId"),
                          int(d.get("updatedTs", 0)), len(d.get("facts") or {}), d.get("correlationId"),
                          json.dumps(d.get("facts") or {})))
        elif topic == "nba.definitions":
            dtype = mkey.split(":", 1)[0] if ":" in mkey else "ACTION"
            defs[d.get("id") or mkey] = (
                d.get("id"), dtype, d.get("name"), d.get("channel"),
                int(d["ttlSeconds"]) if d.get("ttlSeconds") is not None else None,
                json.dumps(d.get("channels")) if d.get("channels") is not None else None,
                json.dumps(d.get("inclusion")) if d.get("inclusion") is not None else None,
                json.dumps(d.get("exclusion")) if d.get("exclusion") is not None else None,
                json.dumps(d.get("logic")) if d.get("logic") is not None else None,
                json.dumps(d.get("factsUsed")) if d.get("factsUsed") is not None else None,
                int(d.get("updatedTs") or d.get("eventTs") or 0))

    def append(r, schema, table, dedup):
        if not r: return
        (spark.createDataFrame(r, schema).dropDuplicates(dedup).withColumn("ingestTs", F.current_timestamp())
         .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{NS}.{table}"))

    append(facts, FACTS_SCHEMA, "silver_fact_history", ["entityId","key","eventTs","value"])
    append(evals, EVALS_SCHEMA, "silver_eval_eligible", ["nbaId","actionId","channel","evaluatedAt"])
    append(acts, ACTS_SCHEMA, "silver_activations", ["nbaId","actionId","channel","op","eventTs"])
    append(snaps, SNAPS_SCHEMA, "silver_snapshots", ["nbaId","updatedTs"])
    if mstones:   # permanent: insert the first completion per (member, milestone); never overwrite
        spark.createDataFrame(mstones, MILES_SCHEMA).dropDuplicates(["nbaId", "milestoneId"]).withColumn("ingestTs", F.current_timestamp()).createOrReplaceTempView("ms")
        spark.sql(f"""MERGE INTO {NS}.silver_milestones t USING ms s ON t.nbaId=s.nbaId AND t.milestoneId=s.milestoneId
          WHEN NOT MATCHED THEN INSERT (nbaId, milestoneId, name, completedAt, ingestTs)
            VALUES (s.nbaId, s.milestoneId, s.name, s.completedAt, s.ingestTs)""")
    if defs:
        spark.createDataFrame(list(defs.values()), DEFS_SCHEMA).createOrReplaceTempView("d")
        spark.sql(f"""MERGE INTO {NS}.dim_definitions t USING d s ON t.id = s.id
          WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    # gold idmap (memberId -> nbaId) from snapshots
    idmap = {r[2]: (r[1] or "OPERATOR", r[2], r[0]) for r in snaps if r[0] and r[2]}
    if idmap:
        spark.createDataFrame(list(idmap.values()), IDMAP_SCHEMA).createOrReplaceTempView("im")
        spark.sql(f"""MERGE INTO {NS}.gold_member_idmap t USING im s ON t.entityId = s.entityId
          WHEN MATCHED AND t.nbaId <> s.nbaId THEN UPDATE SET nbaId=s.nbaId, updatedTs=current_timestamp()
          WHEN NOT MATCHED THEN INSERT (entityType, entityId, nbaId, updatedTs)
            VALUES (s.entityType, s.entityId, s.nbaId, current_timestamp())""")
    # gold_member_snapshot (current fact per member), nbaId from the idmap
    if facts:
        fdf = spark.createDataFrame(facts, FACTS_SCHEMA)
        w = Window.partitionBy("entityType","entityId","key").orderBy(F.col("eventTs").desc())
        snap = (fdf.withColumn("_rn", F.row_number().over(w)).where("_rn=1")
                .select("entityType","entityId","key","value","valueType","eventTs","source"))
        idm = spark.table(f"{NS}.gold_member_idmap").select("entityId", "nbaId")
        snap = (snap.join(idm, "entityId", "left")
                .select("entityType","entityId","nbaId","key","value","valueType","eventTs","source"))
        snap.createOrReplaceTempView("u")
        spark.sql(f"""MERGE INTO {NS}.gold_member_snapshot t USING u s
          ON t.entityType=s.entityType AND t.entityId=s.entityId AND t.key=s.key
          WHEN MATCHED AND s.eventTs >= t.eventTs AND (t.value <> s.value OR t.nbaId IS NULL)
            THEN UPDATE SET nbaId=s.nbaId, value=s.value, valueType=s.valueType, eventTs=s.eventTs, source=s.source
          WHEN NOT MATCHED THEN INSERT *""")
    # MODEL CARD — the ML job's latest card overwrites gold_model_card (one row); every emission appends to history.
    if cards:
        latest = cards[-1]
        (spark.createDataFrame([(latest,)], "card string").withColumn("ts", F.current_timestamp())
         .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{NS}.gold_model_card"))
        (spark.createDataFrame([(c,) for c in cards], "card string").withColumn("ts", F.current_timestamp())
         .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{NS}.gold_model_card_history"))
    # EVENT PATH — a send in THIS micro-batch immediately bumps the frequency facts (no separate count job for
    # the live case). throttle: IN_PROCESS (send) bumps the channel level, FAILED/SUPPRESSED (didn't go out)
    # decrements it; recompute the moment any of the three lands. comms still counts member-week sends.
    now = int(time.time() * 1000)
    if any(f[8] == "actionstate" and f[3] in ("IN_PROCESS", "FAILED", "SUPPRESSED") for f in facts):
        try: emit_throttle(now)
        except Exception as e: print(f"batch {batch_id}: throttle emit skipped: {e}")
    sent_members = sorted({f[1] for f in facts if f[8] == "actionstate" and f[3] == "sent" and f[1]})
    if sent_members:
        try: emit_comms(sent_members, now)
        except Exception as e: print(f"batch {batch_id}: comms emit skipped: {e}")
    # MEDALLION egress: re-emit the facts normalized from the inbound source stream this batch.
    emitted = (0, 0)
    if inbound:
        try: emitted = emit_inbound(inbound)
        except Exception as e: print(f"batch {batch_id}: inbound emit skipped: {e}")
    print(f"batch {batch_id}: facts={len(facts)} evals={len(evals)} acts={len(acts)} snaps={len(snaps)} defs={len(defs)} "
          f"sentMembers={len(sent_members)} inbound={len(inbound)} -> nba.facts={emitted[0]} member.facts={emitted[1]}")

qb = (spark.readStream.format("kafka")
     .option("kafka.bootstrap.servers", BOOTSTRAP)
     .option("kafka.security.protocol", "SASL_PLAINTEXT")
     .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
     .option("kafka.sasl.jaas.config", jaas)
     .option("subscribe", TOPICS)
     .option("startingOffsets", dbutils.widgets.get("start_offsets"))
     .option("includeHeaders", "true")
     .option("maxOffsetsPerTrigger", 50000)
     .option("failOnDataLoss", "false")   # tolerate a recreated topic (checkpoint offsets reset) -> resume from current earliest

     .load()
     .writeStream.foreachBatch(process)
     .option("checkpointLocation", CKPT))
# Serverless forbids an infinite (processingTime) trigger, so "continuous" is a LOOP of availableNow drains:
# each drain processes the data that arrived since the last checkpoint (the event path emits counts per
# micro-batch), then we sleep and drain again. availableNow (no loop) = a single drain + stop.
if TRIGGER == "continuous":
    while True:
        try:
            qb.trigger(availableNow=True).start().awaitTermination()
        except Exception as e:
            # SELF-HEAL: the cloud->local Kafka bridge (external tunnel) intermittently fails to resolve; a mid-stream
            # reconnect throws and — without this — the run DIES for hours (no auto-restart), the lake stops
            # ingesting, and hard-completions never land. Catch + resume from the SAME checkpoint within the run.
            print(f"[datalake-stream] drain failed ({type(e).__name__}: {str(e)[:180]}); resuming from checkpoint...")
        time.sleep(INTERVAL)
else:
    qb.trigger(availableNow=True).start().awaitTermination()   # the post-stream view refresh + exit below run only here

# refresh the derived views (defs fact-map + per-member fact map for the rule funnel)
spark.sql(f"""CREATE OR REPLACE VIEW {NS}.action_fact_map AS
  SELECT id, defType, name, explode(from_json(factsUsedJson, 'array<string>')) AS fact
  FROM {NS}.dim_definitions WHERE factsUsedJson IS NOT NULL""")
spark.sql(f"""CREATE OR REPLACE VIEW {NS}.gold_member_facts AS
  SELECT entityId, any_value(nbaId) AS nbaId, map_from_entries(collect_list(struct(key, value))) AS facts
  FROM {NS}.gold_member_snapshot GROUP BY entityId""")
counts = {t: spark.table(f"{NS}.{t}").count() for t in ("silver_fact_history","silver_activations","silver_eval_eligible","silver_snapshots","silver_milestones","gold_member_snapshot","dim_definitions")}
dbutils.notebook.exit(json.dumps({"status": "ok", "reset": RESET, "counts": counts}))
