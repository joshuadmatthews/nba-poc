# Databricks notebook source
# SCORE — the LIVE ML scorer (replaces the Java ais-nba-ml-scorer). Streams nba.evaluations, scores each member's
# eligible (action, channel) with @champion, and emits nba.score.{action}.{channel} to nba.member.facts. The score
# rides into the snapshot -> the next eval -> the action router's argmax picks the MODEL's winner. Reloads
# @champion EVERY batch, so a fresh promotion (the 5-min retrain->evaluate loop) goes live with no restart =
# auto-deploy. EXPLORATION lives here (softmax over scores, logged propensity) so off-policy learning stays
# unbiased without a router change.
#
# Loop guard: skip evals carrying a type=score HEADER — those are re-evals OUR OWN scores triggered (the rules
# engine re-emits on score change); re-scoring them is exactly what would loop. Features = gold_member_snapshot
# (current) read cross-workspace via UC. eventTs on the score = the eval's evaluatedAt -> replay-safe LWW.
# Continuous = a loop of availableNow drains (serverless forbids an infinite trigger), like the datalake stream.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, time, mlflow, numpy as np, pandas as pd
from pyspark.sql import functions as F
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("temperature", "0.2"); dbutils.widgets.text("trigger", "continuous")
dbutils.widgets.text("interval_seconds", "8")
TEMP0 = float(dbutils.widgets.get("temperature")); TRIGGER = dbutils.widgets.get("trigger")
INTERVAL = int(dbutils.widgets.get("interval_seconds"))
mlflow.set_registry_uri("databricks-uc"); NAME = model_name(ML_NS)
_cat, _sch = ML_NS.split("."); spark.sql(f"CREATE VOLUME IF NOT EXISTS {ML_NS}.ckpt")
CKPT = f"/Volumes/{_cat}/{_sch}/ckpt/score_live"
# Precompute everything the foreachBatch closure needs as PLAIN values (no spark/dbutils/SparkSession objects —
# Spark Connect can't serialize those). Inside the batch we use batch_df.sparkSession, not the global spark.
SASL = sasl_opts()
EST = None; CHM = {}; ACM = {}     # @champion's sklearn estimator + channel/action encoders (picklable -> safe in the
                                   # foreachBatch closure); reloaded in the MAIN process each loop iteration = auto-deploy.

def score_batch(batch_df, batch_id):
    sp = batch_df.sparkSession                       # MUST use the batch's own session under Spark Connect
    try:
        bp = sp.table(f"{ML_NS}.bandit_policy").orderBy(F.col("updatedAt").desc()).limit(1).collect()
        TEMP = float(json.loads(bp[0]["policy"]).get("temperature", TEMP0)) if bp else TEMP0
    except Exception:
        TEMP = TEMP0
    # 1) drop score-triggered re-evals (type=score header) -> loop guard; parse the rest
    rows = (batch_df.where("NOT exists(headers, h -> string(h.key)='type' AND string(h.value)='score')")
            .selectExpr("CAST(value AS STRING) v").collect())
    latest = {}
    for r in rows:
        try: e = json.loads(r.v)
        except Exception: continue
        nb = e.get("nbaId")
        if nb and (nb not in latest or e.get("evaluatedAt", 0) >= latest[nb].get("evaluatedAt", 0)):
            latest[nb] = e
    cand = []
    for e in latest.values():
        for a in (e.get("channelActions") or []):
            if a.get("eligible") and not a.get("hardCompleted"):
                cand.append({"nbaId": e.get("nbaId"), "entityType": e.get("entityType"), "entityId": e.get("entityId"),
                             "correlationId": e.get("correlationId"), "evaluatedAt": e.get("evaluatedAt"),
                             "actionId": a.get("actionId"), "channel": a.get("channel"),
                             "name": a.get("name"), "contentKey": a.get("contentKey")})
    if not cand:
        return
    cdf = pd.DataFrame(cand)

    # 2) features from gold (current snapshot), pivoted to the feature contract
    ids = [x for x in cdf["nbaId"].dropna().unique().tolist()]
    g = (sp.table(f"{SRC_NS}.gold_member_snapshot").where("key LIKE 'operator.%'").where(F.col("nbaId").isin(ids)))
    wide = g.groupBy("nbaId").pivot("key").agg(F.first("value")).toPandas()
    feat = pd.DataFrame({"nbaId": wide["nbaId"]}) if len(wide) else pd.DataFrame(columns=["nbaId"])
    for k, col, kind in FEATURE_KEYS:
        if len(wide) and k in wide.columns:
            feat[col] = (wide[k].apply(lambda v: 1.0 if str(v).lower() == "true" else 0.0) if kind == "bool"
                         else pd.to_numeric(wide[k], errors="coerce").fillna(0.0))
        else:
            feat[col] = 0.0
    pdf = cdf.merge(feat, on="nbaId", how="left")
    # CONTEXTUAL fatigue state per candidate, from the member's recent REAL sends (silver IN_PROCESS, trailing 14d) —
    # this is what lets @champion AVOID a channel/ask it has already hit too often for this member.
    now = int(time.time() * 1000); since = now - 14 * 86400 * 1000
    try:
        h = (sp.table(f"{SRC_NS}.silver_fact_history")
             .where("key LIKE 'nba.actionstate.%' AND value = 'IN_PROCESS'")
             .where((F.col("eventTs") >= F.lit(since)) & F.col("entityId").isin(cdf["entityId"].dropna().unique().tolist()))
             .select("entityId", "actionId", "channel", "eventTs").toPandas())
    except Exception:
        h = pd.DataFrame(columns=["entityId", "actionId", "channel", "eventTs"])
    chN = h.groupby(["entityId", "channel"]).size().to_dict() if len(h) else {}
    acN = h.groupby(["entityId", "actionId"]).size().to_dict() if len(h) else {}
    lastTs = h.groupby("entityId")["eventTs"].max().to_dict() if len(h) else {}
    pdf["thisChannelRecentN"] = [float(chN.get((e, c), 0)) for e, c in zip(pdf["entityId"], pdf["channel"])]
    pdf["thisActionRecentN"] = [float(acN.get((e, a), 0)) for e, a in zip(pdf["entityId"], pdf["actionId"])]
    pdf["daysSinceLastContact"] = [round((now - lastTs.get(e, 0)) / 86400000.0, 2) if lastTs.get(e, 0) else 14.0 for e in pdf["entityId"]]
    for col in FEATURE_COLS:
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0.0)

    # 3) score with @champion (EST is a LIST: K bootstraps -> Thompson, or 1). P = K x N per-bootstrap probs;
    #    base_score = the ensemble MEAN (the exploit estimate we log + emit for non-winners).
    X = pdf[FEATURE_COLS].astype(float).copy()
    X["channel_idx"] = pdf["channel"].map(CHM).fillna(-1).astype(float)
    X["action_idx"] = pdf["actionId"].map(ACM).fillna(-1).astype(float)
    Xf = X[TRAIN_FEATS]
    P = np.vstack([np.asarray(e.predict_proba(Xf)[:, 1]) for e in EST])      # K x N
    pdf["base_score"] = np.clip(P.mean(axis=0), 1e-4, 1 - 1e-4).round(4)

    # 4) THOMPSON exploration ACROSS ALL eligible arms per member: draw one bootstrap, its argmax (over every
    #    action,channel) wins -> boost it so the router dispatches it. A cold arm the bootstraps disagree on wins
    #    sometimes (explored); converges to the mean as data lands. K=1 (single model) degrades to greedy. The
    #    propensity = fraction of bootstraps that pick each arm = the honest P(selected) the OPE needs.
    rng = np.random.default_rng(); Kk = P.shape[0]
    pdf["emit_score"] = pdf["base_score"]; pdf["propensity"] = 0.0
    for _nb, grp in pdf.groupby("nbaId"):
        idx = grp.index.to_numpy(); Pm = P[:, idx]                          # K x n_arms for this member
        pdf.loc[idx, "propensity"] = np.bincount(np.argmax(Pm, axis=0), minlength=len(idx)) / float(Kk)
        win = idx[int(np.argmax(Pm[rng.integers(Kk)]))]                     # one sampled bootstrap's argmax
        pdf.loc[win, "emit_score"] = round(float(grp["base_score"].max()) + 1e-4, 6)
    pdf["propensity"] = pdf["propensity"].round(5)

    # 5) predictions_log (off-policy propensity source) — STABLE typed subset; best-effort (never crash the scorer)
    try:
        plog = pdf[["nbaId", "actionId", "channel", "base_score", "emit_score", "propensity", "evaluatedAt", "correlationId"]].copy()
        for c in ("base_score", "emit_score", "propensity"): plog[c] = plog[c].astype(float)
        for c in ("nbaId", "actionId", "channel", "correlationId"): plog[c] = plog[c].astype(str)
        plog["evaluatedAt"] = plog["evaluatedAt"].fillna(0).astype("int64")
        (sp.createDataFrame(plog).withColumn("temperature", F.lit(float(TEMP))).withColumn("scoredAt", F.current_timestamp())
         .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{ML_NS}.predictions_log"))
    except Exception as _e:
        print("predictions_log skipped:", str(_e)[:140])

    # 6) emit nba.score.{action}.{channel} -> nba.member.facts (kind=score; the snapshot-builder folds it in)
    emit = sp.createDataFrame(pdf[["entityType", "entityId", "nbaId", "actionId", "channel", "name",
                                      "contentKey", "emit_score", "correlationId", "evaluatedAt"]]).selectExpr(
        "concat_ws(':', entityType, entityId) AS key",
        "concat('{',"
        " '\"key\":\"nba.score.', actionId, '.', channel, '\",',"
        " '\"value\":', to_json(named_struct('actionId',actionId,'channel',channel,'score',emit_score,'name',name,'contentKey',contentKey,'correlationId',correlationId)), ',',"
        " '\"valueType\":\"OBJECT\",', '\"eventTs\":', cast(evaluatedAt AS string), ',',"
        " '\"source\":\"ml\",', '\"nbaId\":\"', nbaId, '\",', '\"entityType\":\"', entityType, '\",', '\"entityId\":\"', entityId, '\"}') AS value"
    ).withColumn("headers", F.array(F.struct(F.lit("kind").alias("key"), F.lit("score").cast("binary").alias("value"))))
    emit.write.format("kafka").options(**SASL).option("topic", "nba.member.facts").option("includeHeaders", "true").save()
    print(f"batch {batch_id}: scored {len(pdf)} candidates over {len(latest)} members (temp={TEMP})")

src = (spark.readStream.format("kafka").options(**sasl_opts())
       .option("subscribe", "nba.evaluations").option("startingOffsets", "latest")
       .option("includeHeaders", "true").option("maxOffsetsPerTrigger", 20000).option("failOnDataLoss", "false").load())

def load_champion():
    """Load @champion in the MAIN process (UC resolves here, not in the Spark Connect worker) and pull out the raw
    estimators + encoders. Reloading this between drains = a freshly-promoted model goes live with no restart.
    Returns a LIST of estimators: the K bootstraps of a PropensityEnsemble (-> Thompson), or [the single estimator]."""
    m = mlflow.pyfunc.load_model(f"models:/{NAME}@champion")
    try: pm = m.unwrap_python_model()
    except Exception: pm = m._model_impl.python_model
    ests = list(getattr(pm, "estimators", None) or [pm.estimator])
    return ests, pm.channels, pm.actions

def run_once():
    (src.writeStream.foreachBatch(score_batch).option("checkpointLocation", CKPT)
     .trigger(availableNow=True).start().awaitTermination())

if TRIGGER == "continuous":      # live loop of drains (the model scorer "service")
    while True:
        EST, CHM, ACM = load_champion()      # reload @champion (main process) before each drain -> auto-deploy
        run_once(); time.sleep(INTERVAL)
else:
    EST, CHM, ACM = load_champion()
    run_once()
dbutils.notebook.exit(json.dumps({"status": "ok"}))
