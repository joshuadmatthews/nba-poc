# Databricks notebook source
# SCORE · EXPLORE — uniform-random BOOTSTRAP scorer for building BASE PROPENSITY. There is no policy yet (clean base),
# so this emits a RANDOM score per ELIGIBLE (action, channel); the router's argmax over random scores dispatches ~uniformly
# across the whole 52-arm catalog. That unbiased coverage is exactly what we need to measure each action's BASE response
# rate from real journeys. Same read/emit contract as nba_ml_score_rl (nba.evaluations -> nba.score.{action}.{channel}),
# just policy='explore' and a random score. Retire this once a learned policy exists.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, time, numpy as np
from pyspark.sql import functions as F
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("trigger", "continuous"); dbutils.widgets.text("interval_seconds", "6")
TRIGGER = dbutils.widgets.get("trigger"); INTERVAL = int(dbutils.widgets.get("interval_seconds"))
_cat, _sch = ML_NS.split("."); spark.sql(f"CREATE VOLUME IF NOT EXISTS {ML_NS}.ckpt")
CKPT = f"/Volumes/{_cat}/{_sch}/ckpt/score_explore"
SASL = sasl_opts()


def score_batch(df, batch_id):
    rows = df.selectExpr("CAST(value AS STRING) v").collect()
    latest = {}
    for r in rows:
        try: e = json.loads(r.v)
        except Exception: continue
        nb = e.get("nbaId")
        if nb and (nb not in latest or e.get("evaluatedAt", 0) >= latest[nb].get("evaluatedAt", 0)): latest[nb] = e
    cand = []
    for e in latest.values():
        for a in (e.get("channelActions") or []):
            if a.get("eligible") and not a.get("hardCompleted"):
                cand.append({"nbaId": e.get("nbaId"), "entityType": e.get("entityType"), "entityId": e.get("entityId"),
                             "correlationId": e.get("correlationId"), "evaluatedAt": e.get("evaluatedAt"),
                             "actionId": a.get("actionId"), "channel": a.get("channel"), "name": a.get("name"),
                             "contentKey": a.get("contentKey")})
    if not cand: return
    import pandas as pd
    cdf = pd.DataFrame(cand)
    cdf["score"] = np.random.RandomState(batch_id & 0x7fffffff).random(len(cdf))   # UNIFORM RANDOM per (action,channel)
    emit = spark.createDataFrame(cdf[["entityType", "entityId", "nbaId", "actionId", "channel", "name", "contentKey", "score", "correlationId", "evaluatedAt"]]).selectExpr(
        "concat_ws(':', entityType, entityId) AS key",
        "concat('{',"
        " '\"key\":\"nba.score.', actionId, '.', channel, '\",',"
        " '\"value\":', to_json(named_struct('actionId',actionId,'channel',channel,'score',score,'name',name,'contentKey',contentKey,'correlationId',correlationId,'policy','explore')), ',',"
        " '\"valueType\":\"OBJECT\",', '\"eventTs\":', cast(evaluatedAt AS string), ',',"
        " '\"source\":\"ml\",', '\"nbaId\":\"', nbaId, '\",', '\"entityType\":\"', entityType, '\",', '\"entityId\":\"', entityId, '\"}') AS value"
    ).withColumn("headers", F.array(F.struct(F.lit("kind").alias("key"), F.lit("score").cast("binary").alias("value"))))
    emit.write.format("kafka").options(**SASL).option("topic", "nba.member.facts").option("includeHeaders", "true").save()
    print(f"explore batch {batch_id}: random-scored {len(cdf)} (action,channel) candidates over {len(latest)} members")


src = (spark.readStream.format("kafka").options(**SASL).option("subscribe", "nba.evaluations")
       .option("startingOffsets", "latest").option("includeHeaders", "true")
       .option("maxOffsetsPerTrigger", 20000).option("failOnDataLoss", "false").load())
def run_once():
    src.writeStream.foreachBatch(score_batch).option("checkpointLocation", CKPT).trigger(availableNow=True).start().awaitTermination()
if TRIGGER == "continuous":
    while True:
        run_once(); time.sleep(INTERVAL)
else:
    run_once()
dbutils.notebook.exit(json.dumps({"status": "ok"}))
