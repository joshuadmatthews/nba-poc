# Databricks notebook source
# SCORE * PLAYBOOK -- the hand-crafted marketing journeys, as a scoring policy. This is the HISTORY the model learns to
# RECREATE (then optimize). Each member is assigned one of 5 journeys (stable hash); the scorer prescribes the journey's
# FIRST step (action, channel) that is currently eligible (prereqs met, not yet completed) and scores it 1.0, everything
# else 0.0, so the router dispatches exactly the hand-crafted next action. Same read/emit contract as the RL scorer
# (nba.evaluations -> nba.score.{action}.{channel}); policy='playbook'. Replaces the random explorer; retire once a
# learned policy trains on the recorded journeys.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, time, zlib, numpy as np
from pyspark.sql import functions as F
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("trigger", "continuous"); dbutils.widgets.text("interval_seconds", "6")
TRIGGER = dbutils.widgets.get("trigger"); INTERVAL = int(dbutils.widgets.get("interval_seconds"))
_cat, _sch = ML_NS.split("."); spark.sql(f"CREATE VOLUME IF NOT EXISTS {ML_NS}.ckpt")
CKPT = f"/Volumes/{_cat}/{_sch}/ckpt/score_playbook"
SASL = sasl_opts()

# 5 hand-crafted journeys: ordered (actionId, channel) through Reach -> Register/Assess -> Engage -> STARS gap-closure.
JOURNEYS = [
    [("action_plan_welcome","email"),("action_portal_registration","email"),("action_hra","email"),
     ("action_pcp_selection","push"),("action_care_manager_outreach","voice"),("action_annual_wellness_visit","email"),
     ("action_med_adherence","email"),("action_mammogram","push")],                                          # Onboarding
    [("action_plan_welcome","sms"),("action_hra","sms"),("action_pcp_selection","sms"),
     ("action_annual_wellness_visit","sms"),("action_med_adherence","sms"),("action_mammogram","sms"),
     ("action_colonoscopy","sms")],                                                                          # STARS / gap closure
    [("action_plan_welcome","email"),("action_portal_registration","email"),("action_login_reminder","push"),
     ("action_benefits_education","email"),("action_hra","email"),("action_pcp_selection","email"),
     ("action_annual_wellness_visit","email")],                                                              # Enrollment
    [("action_plan_welcome","voice"),("action_hra","voice"),("action_pcp_selection","voice"),
     ("action_care_manager_outreach","voice"),("action_annual_wellness_visit","voice"),("action_med_adherence","voice")], # Care Management
    [("action_reengage","email"),("action_hra_reminder","sms"),("action_login_reminder","push"),
     ("action_pcp_selection","push"),("action_annual_wellness_visit","email"),("action_med_adherence","email")],  # Re-engagement
]
def journey_of(member_id):
    return JOURNEYS[zlib.crc32((member_id or "").encode()) % len(JOURNEYS)]


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
        elig = {(a.get("actionId"), a.get("channel")) for a in (e.get("channelActions") or [])
                if a.get("eligible") and not a.get("hardCompleted")}
        if not elig: continue
        member = e.get("entityId") or e.get("nbaId")
        prescribed = next(((a, c) for (a, c) in journey_of(member) if (a, c) in elig), None)  # first eligible journey step
        if prescribed is None:
            prescribed = next(iter(elig))                      # off-journey fallback: keep the member moving
        for a in (e.get("channelActions") or []):
            if a.get("eligible") and not a.get("hardCompleted"):
                cand.append({"nbaId": e.get("nbaId"), "entityType": e.get("entityType"), "entityId": e.get("entityId"),
                             "correlationId": e.get("correlationId"), "evaluatedAt": e.get("evaluatedAt"),
                             "actionId": a.get("actionId"), "channel": a.get("channel"), "name": a.get("name"),
                             "contentKey": a.get("contentKey"),
                             "score": 1.0 if (a.get("actionId"), a.get("channel")) == prescribed else 0.0})
    if not cand: return
    import pandas as pd
    cdf = pd.DataFrame(cand)
    emit = spark.createDataFrame(cdf[["entityType", "entityId", "nbaId", "actionId", "channel", "name", "contentKey", "score", "correlationId", "evaluatedAt"]]).selectExpr(
        "concat_ws(':', entityType, entityId) AS key",
        "concat('{',"
        " '\"key\":\"nba.score.', actionId, '.', channel, '\",',"
        " '\"value\":', to_json(named_struct('actionId',actionId,'channel',channel,'score',score,'name',name,'contentKey',contentKey,'correlationId',correlationId,'policy','playbook')), ',',"
        " '\"valueType\":\"OBJECT\",', '\"eventTs\":', cast(evaluatedAt AS string), ',',"
        " '\"source\":\"ml\",', '\"nbaId\":\"', nbaId, '\",', '\"entityType\":\"', entityType, '\",', '\"entityId\":\"', entityId, '\"}') AS value"
    ).withColumn("headers", F.array(F.struct(F.lit("kind").alias("key"), F.lit("score").cast("binary").alias("value"))))
    emit.write.format("kafka").options(**SASL).option("topic", "nba.member.facts").option("includeHeaders", "true").save()
    print(f"playbook batch {batch_id}: scored {len(cdf)} candidates, prescribed {len(latest)} members down their journeys")


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
