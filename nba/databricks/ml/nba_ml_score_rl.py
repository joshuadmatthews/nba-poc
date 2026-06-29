# Databricks notebook source
# SCORE · RL POLICY — the LIVE scorer running the offline-RL CQL policy instead of the myopic propensity model.
# Streams nba.evaluations; for each member builds the 13-dim journey observation (8 features from gold + 4 milestone
# flags computed from the features + a touch-count step), runs the CQL critic's Q-network (served as plain numpy
# weights — exact match to the trained model, no torch/d3rlpy needed), and emits Q(s,a) per ELIGIBLE arm as
# nba.score.{action}.{channel}. The action router's argmax over those Q-values = the policy's chosen next-best-action,
# now optimizing long-term milestone value instead of immediate conversion. Weights load from rl_qnet.json on a UC
# volume; same loop-guard + continuous-drain shape as the propensity scorer. Plain-numpy closure = Spark-Connect-safe.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

# MAGIC %run ./nba_journey_env

# COMMAND ----------

import json, time, numpy as np
from pyspark.sql import functions as F
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("trigger", "continuous"); dbutils.widgets.text("interval_seconds", "8")
dbutils.widgets.text("qnet_path", "")
dbutils.widgets.text("explore_c", "2.5")   # ongoing soft novelty-bonus strength (0 = off)
dbutils.widgets.text("first_try_bonus", "8.0")  # OPTIMISTIC first-try: a NEVER-sent eligible channel is lifted above the
#                                                 member's best exploited score so it gets one guaranteed try (0 = off).
#   Must exceed the Q range (~5) so an untried sms (Q~0) out-scores a tried email (Q~2.4); base Q breaks ties among untried.
dbutils.widgets.text("prior_w", "6")        # COLD-START channel prior: worth ~N real sends before the learned CQL fully
dbutils.widgets.text("prior_gain", "3.0")   # takes over; prior_gain = max nudge magnitude on the Q scale (0 on either = off)
TRIGGER = dbutils.widgets.get("trigger"); INTERVAL = int(dbutils.widgets.get("interval_seconds"))
EXPLORE_C = float(dbutils.widgets.get("explore_c") or "0")
FIRST_TRY = float(dbutils.widgets.get("first_try_bonus") or "0")
PRIOR_W = float(dbutils.widgets.get("prior_w") or "0"); PRIOR_GAIN = float(dbutils.widgets.get("prior_gain") or "0")
_cat, _sch = ML_NS.split("."); spark.sql(f"CREATE VOLUME IF NOT EXISTS {ML_NS}.ckpt")
QPATH = dbutils.widgets.get("qnet_path") or f"/Volumes/{_cat}/{_sch}/ckpt/rl_qnet.json"
CKPT = f"/Volumes/{_cat}/{_sch}/ckpt/score_rl_v2"   # bumped for the clean-slate reset (fresh start at live edge)
SASL = sasl_opts()

WEIGHTS = []; RL_ARMS = []; ARM_IDX = {}; CH_TO_ARM = {}; MAXS = 12.0; OBS_MEAN = None; OBS_STD = None
def reload_qnet():                                      # reloaded each loop iteration -> a fresh retrain auto-deploys
    global WEIGHTS, RL_ARMS, ARM_IDX, CH_TO_ARM, MAXS, OBS_MEAN, OBS_STD
    q = json.load(open(QPATH))
    WEIGHTS = [(np.array(l["W"], np.float32), np.array(l["b"], np.float32)) for l in q["layers"]]  # plain numpy (picklable)
    # (ACTION, CHANNEL)-KEYED: each arm is a real (actionId, channel) pair, so score each eligible candidate on its OWN
    # arm. ARM_IDX = {(actionId, channel): arm}; CH_TO_ARM = a per-channel fallback for an unmodeled (action,channel).
    RL_ARMS = q["arms"]; MAXS = float(q["max_steps"])
    ARM_IDX = {(a["actionId"], a["channel"]): i for i, a in enumerate(RL_ARMS)}
    CH_TO_ARM = {}
    for i, a in enumerate(RL_ARMS): CH_TO_ARM.setdefault(a["channel"], i)   # first arm on a channel = its fallback
    OBS_MEAN = np.array(q["obs_mean"], np.float32) if q.get("obs_mean") else None   # obs normalization (matches training)
    OBS_STD = np.array(q["obs_std"], np.float32) if q.get("obs_std") else None      # absent on legacy qnets -> no-op
reload_qnet()
print(f"loaded Q-net: layers {[w.shape for w, _ in WEIGHTS]}  arms {[a['actionId'] for a in RL_ARMS]}")

def obs_dim():                                          # the Q-net's input width (13 = no action-state, 26 = with dispositions)
    return int(WEIGHTS[0][0].shape[1]) if WEIGHTS else 13
def has_disp():                                         # does the loaded Q-net expect the channel-specific disposition block?
    return obs_dim() >= len(FEATURE_COLS) + NDISP_TOTAL + len(MILESTONE_IDS) + 1
def build_obs(F8, disp_block):                          # HEALTHCARE obs: [19 features | 13 dispositions | 5 milestones | step]
    # F8 = the 19 healthcare FEATURE_COLS (gold); disp_block = the per-channel disposition block. Milestones come from
    # the env's _milestones_batch (the SINGLE source of truth shared with training + reconstruction) so a state means the
    # SAME thing the Q-net was trained on. step = contact progress (totalThisWeek/MAX_STEPS), matching the env/reconstruction.
    Ffull = np.column_stack([F8, disp_block]).astype(np.float32) if has_disp() else F8.astype(np.float32)
    ms = _milestones_batch(Ffull).astype(np.float32)    # (n, 5) healthcare milestone flags
    step = (np.clip(F8[:, FCOL["totalThisWeek"]], 0, MAXS) / MAXS).reshape(-1, 1).astype(np.float32)
    return np.column_stack([Ffull, ms, step]).astype(np.float32)

def build_dispositions(sp, cdf, feat):                  # ACTION STATE: per (member, channel) the LATEST disposition, as the
    n = len(feat); block = np.zeros((n, NDISP_TOTAL), np.float32)   # channel-specific disp block (DISP_COLS order). The
    try:                                                # disposition fact's `value` = the activation layer's raw status.
        from pyspark.sql import Window
        nb2ent = dict(zip(cdf["nbaId"], cdf["entityId"])); ents = [e for e in set(nb2ent.values()) if e]
        if not ents: return block
        d = (sp.table(f"{SRC_NS}.silver_fact_history").where("factClass = 'disposition'")
             .where(F.col("entityId").isin(ents)).select("entityId", "channel", F.col("value").alias("raw"), "eventTs"))
        wsp = Window.partitionBy("entityId", "channel").orderBy(F.col("eventTs").desc())
        latest = d.withColumn("rn", F.row_number().over(wsp)).where("rn = 1").select("entityId", "channel", "raw").toPandas()
        lk = {}                                         # (entityId, channel) -> disposition column offset in the block
        for r in latest.itertuples():
            ch = str(r.channel); disp = RAW_TO_DISP.get(ch, {}).get(str(r.raw))   # map raw status -> env disposition name
            if disp is not None and (ch, disp) in DISP_AT: lk[(r.entityId, ch)] = DISP_AT[(ch, disp)]
        for i, nb in enumerate(feat["nbaId"]):
            ent = nb2ent.get(nb)
            for ch in CH_DISPS:
                off = lk.get((ent, ch))
                if off is not None: block[i, off] = 1.0
    except Exception as e:
        print(f"disposition compute skipped ({type(e).__name__}): {e}")
    return block

def send_counts(sp, cdf):
    """Per (entityId, channel) recent (14d) IN_PROCESS send count -> drives the EXPLORATION novelty bonus: the channels a
    member has been TRIED on least get the biggest boost, so the policy explores under-used channels (e.g. sms) instead of
    only exploiting its known favorite. The disposition the member then returns teaches their REAL per-member preference."""
    try:
        ents = [e for e in set(cdf["entityId"].dropna()) if e]
        if not ents: return {}
        since = int(time.time() * 1000) - 14 * 86400 * 1000
        d = (sp.table(f"{SRC_NS}.silver_fact_history").where("factClass = 'actionstate' AND value = 'IN_PROCESS'")
             .where((F.col("eventTs") >= F.lit(since)) & F.col("entityId").isin(ents))
             .groupBy("entityId", "channel").agg(F.count(F.lit(1)).alias("n")).toPandas())
        return {(r.entityId, r.channel): int(r.n) for r in d.itertuples()}
    except Exception as e:
        print(f"send-count (exploration) skipped ({type(e).__name__}): {e}"); return {}

def qnet(X):                                            # X (n, obs_dim) -> Q (n, action_dim)
    if OBS_MEAN is not None: X = (X - OBS_MEAN) / OBS_STD   # normalize IDENTICALLY to training (critical for a sane policy)
    for i, (W, b) in enumerate(WEIGHTS):
        X = X @ W.T + b
        if i < len(WEIGHTS) - 1: X = np.maximum(X, 0.0)
    return X

def score_batch(batch_df, batch_id):
    sp = batch_df.sparkSession
    rows = (batch_df.where("NOT exists(headers, h -> string(h.key)='type' AND string(h.value)='score')")
            .selectExpr("CAST(value AS STRING) v").collect())
    latest = {}
    for r in rows:
        try: e = json.loads(r.v)
        except Exception: continue
        nb = e.get("nbaId")
        if nb and (nb not in latest or e.get("evaluatedAt", 0) >= latest[nb].get("evaluatedAt", 0)): latest[nb] = e
    cand = []
    for e in latest.values():
        for a in (e.get("channelActions") or []):
            if a.get("eligible") and not a.get("hardCompleted") and a.get("channel") in CH_TO_ARM:
                cand.append({"nbaId": e.get("nbaId"), "entityType": e.get("entityType"), "entityId": e.get("entityId"),
                             "correlationId": e.get("correlationId"), "evaluatedAt": e.get("evaluatedAt"),
                             "actionId": a.get("actionId"), "channel": a.get("channel"), "name": a.get("name"),
                             "contentKey": a.get("contentKey")})
    if not cand: return
    import pandas as pd
    cdf = pd.DataFrame(cand)
    ids = list(cdf["nbaId"].dropna().unique())
    g = (sp.table(f"{SRC_NS}.gold_member_snapshot").where("key LIKE 'operator.%'").where(F.col("nbaId").isin(ids)))
    wide = g.groupBy("nbaId").pivot("key").agg(F.first("value")).toPandas()
    feat = pd.DataFrame({"nbaId": wide["nbaId"]}) if len(wide) else pd.DataFrame(columns=["nbaId"])
    for k, col, kind in FEATURE_KEYS:
        if len(wide) and k in wide.columns:
            # bool facts arrive as literal 'true' OR (from the activation layer) as LONG 1 -> accept both via _coerce,
            # else the policy reads every climbing member as baseline and never sees the journey it's meant to optimize.
            feat[col] = (wide[k].apply(lambda v: _coerce(v, "bool")) if kind == "bool"
                         else pd.to_numeric(wide[k], errors="coerce").fillna(0.0))
        else: feat[col] = 0.0
    # member-level Q: one obs + Q per unique member
    feat = feat.drop_duplicates("nbaId").reset_index(drop=True)
    F8 = feat[FEATURE_COLS].astype(float).values
    disp_block = build_dispositions(sp, cdf, feat) if has_disp() else np.zeros((len(feat), NDISP_TOTAL), np.float32)
    Q = qnet(build_obs(F8, disp_block))                  # (n_members, action_dim); obs width adapts to the Q-net
    qmap = {nb: Q[i] for i, nb in enumerate(feat["nbaId"])}
    # (ACTION,CHANNEL)-KEYED scoring: each (actionId, channel) has its OWN arm/value. Score on its own arm; FALL BACK to
    # the channel arm for an unmodeled (action,channel) -- the serving-side warm-start for a brand-new action.
    def _arm(action_id, channel):
        i = ARM_IDX.get((action_id, channel))
        return i if i is not None else CH_TO_ARM.get(channel)
    cdf["score"] = [float(qmap.get(r.nbaId, np.zeros(len(RL_ARMS) + 1))[_arm(r.actionId, r.channel)]) if r.nbaId in qmap else 0.0
                    for r in cdf.itertuples()]

    # EXPLORATION: the CQL is pure EXPLOIT — it plays the best KNOWN arm and never TRIES a cold channel, so it can never
    # learn that THIS member responds to (e.g.) sms. With sms Q~0 and email Q~2.4, a SOFT additive bonus can't break the
    # zero-trap (email always wins) -> sms is NEVER sent -> no disposition feedback -> never learned. Two-part fix:
    #  (1) FIRST_TRY (optimistic init): any eligible channel this member has NEVER been sent is lifted ABOVE their current
    #      best score, so it is GUARANTEED one try regardless of base Q. base Q breaks ties among untried -> channels are
    #      tried in a sane order; sms gets sampled once per member, then the returned disposition teaches the real value.
    #  (2) EXPLORE_C (ongoing soft novelty): EXPLORE_C/(1+sends) keeps gently re-trying under-used channels after the first.
    # HOLD is scored below (post-bonus) so it gets neither; an untried channel out-scores HOLD -> it actually fires.
    if PRIOR_W > 0 or EXPLORE_C > 0 or FIRST_TRY > 0:
        SC = send_counts(sp, cdf)
        cdf["_nsent"] = [int(SC.get((r.entityId, r.channel), 0)) for r in cdf.itertuples()]
        # COLD-START CHANNEL PRIOR — a BRAND-NEW member has no per-member send history, so the CQL doesn't yet know their
        # idiosyncratic channel tilt. Nudge each member toward their ARCHETYPE's likely-best channel (digital -> email/sms/
        # push, older/complex -> voice, low-digital+SDOH -> mail), with weight w = PRIOR_W/(PRIOR_W + total member sends) so
        # it DECAYS to zero as real dispositions arrive and the learned CQL takes over. Centered per member -> it only
        # REORDERS channels (it's a starting nudge, washed out by data — not the live policy, not the sim's hidden truth).
        if PRIOR_W > 0 and PRIOR_GAIN > 0 and len(feat):
            try:
                from collections import defaultdict
                tote = defaultdict(int)
                for (ent, ch), n in SC.items(): tote[ent] += int(n)
                fd = feat.drop_duplicates("nbaId").set_index("nbaId")
                digital = ((fd.get("portalLogins30d", 0.0) / 15.0 + fd.get("pagesViewed30d", 0.0) / 40.0) / 2.0).clip(0, 1)
                old = ((fd.get("age", 65.0) - 55.0) / 30.0).clip(0, 1)
                sdoh = (fd.get("sdohBarrier", 0.0) >= 1).astype(float)
                aff = pd.DataFrame({"email": 0.50 + 0.40 * digital, "sms": 0.45 + 0.40 * digital - 0.10 * old,
                                    "push": 0.35 + 0.40 * digital - 0.15 * old, "voice": 0.35 + 0.45 * old,
                                    "mail": 0.30 + 0.40 * old + 0.30 * sdoh}, index=fd.index).clip(0, 1)
                aff = aff.sub(aff.mean(axis=1), axis=0)              # center -> relative reorder, not an absolute lift
                al = aff.reset_index().melt(id_vars="nbaId", var_name="channel", value_name="_aff")
                cdf = cdf.merge(al, on=["nbaId", "channel"], how="left")
                cdf["_aff"] = cdf["_aff"].fillna(0.0)
                w = PRIOR_W / (PRIOR_W + pd.Series([float(tote.get(e, 0)) for e in cdf["entityId"]], index=cdf.index))
                cdf["score"] = cdf["score"] + w * PRIOR_GAIN * cdf["_aff"]
                cdf = cdf.drop(columns=["_aff"])
            except Exception as e:
                print(f"cold-start prior skipped ({type(e).__name__}): {e}")
        if EXPLORE_C > 0:                                  # ongoing soft novelty (gently re-try under-used channels)
            cdf["score"] = cdf["score"] + EXPLORE_C / (1.0 + cdf["_nsent"])
        if FIRST_TRY > 0:                                  # guarantee each never-sent channel one try — now BEST-first
            best = cdf.groupby("nbaId")["score"].transform("max")
            fresh = (cdf["_nsent"] == 0) & (cdf["channel"] != "hold")
            # among untried, try the HIGHEST-scored (prior-warmed) channel FIRST (ascending=True -> best score gets the
            # largest tiebreak -> argmax), so a new member is sent their LIKELY-BEST channel first, not the worst.
            cdf.loc[fresh, "score"] = best[fresh] + FIRST_TRY + cdf.loc[fresh, "score"].rank(method="dense", ascending=True) * 1e-6
        cdf = cdf.drop(columns=["_nsent"])

    # Emit NOOP ("hold") as a scored candidate per member so the router's argmax can choose to WAIT. We DON'T suppress
    # here — we score everything including no-op and let the router decide; if 'hold' is the top score the router holds
    # (and suppresses any in-flight, not-yet-delivered action naturally). The router treats actionId='__hold' as no-dispatch.
    NOOP_I = len(RL_ARMS); hold_rows = []; seen = set()
    for r in cdf.itertuples():
        if r.nbaId in seen or r.nbaId not in qmap: continue
        seen.add(r.nbaId)
        hold_rows.append({"entityType": r.entityType, "entityId": r.entityId, "nbaId": r.nbaId, "actionId": "__hold",
                          "channel": "hold", "name": "Hold (pace)", "contentKey": "",
                          "score": float(qmap[r.nbaId][NOOP_I]), "correlationId": r.correlationId, "evaluatedAt": r.evaluatedAt})
    if hold_rows:
        cdf = pd.concat([cdf, pd.DataFrame(hold_rows)], ignore_index=True)

    emit = sp.createDataFrame(cdf[["entityType", "entityId", "nbaId", "actionId", "channel", "name", "contentKey", "score", "correlationId", "evaluatedAt"]]).selectExpr(
        "concat_ws(':', entityType, entityId) AS key",
        "concat('{',"
        " '\"key\":\"nba.score.', actionId, '.', channel, '\",',"
        " '\"value\":', to_json(named_struct('actionId',actionId,'channel',channel,'score',score,'name',name,'contentKey',contentKey,'correlationId',correlationId,'policy','cql')), ',',"
        " '\"valueType\":\"OBJECT\",', '\"eventTs\":', cast(evaluatedAt AS string), ',',"
        " '\"source\":\"ml\",', '\"nbaId\":\"', nbaId, '\",', '\"entityType\":\"', entityType, '\",', '\"entityId\":\"', entityId, '\"}') AS value"
    ).withColumn("headers", F.array(F.struct(F.lit("kind").alias("key"), F.lit("score").cast("binary").alias("value"))))
    emit.write.format("kafka").options(**SASL).option("topic", "nba.member.facts").option("includeHeaders", "true").save()
    print(f"batch {batch_id}: RL-scored {len(cdf)} (arm,member) over {len(feat)} members")

src = (spark.readStream.format("kafka").options(**SASL).option("subscribe", "nba.evaluations")
       .option("startingOffsets", "latest").option("includeHeaders", "true")
       .option("maxOffsetsPerTrigger", 20000).option("failOnDataLoss", "false").load())
def run_once():
    src.writeStream.foreachBatch(score_batch).option("checkpointLocation", CKPT).trigger(availableNow=True).start().awaitTermination()
if TRIGGER == "continuous":
    while True:
        try:
            reload_qnet()                  # auto-deploy: pick up a freshly-retrained policy with no restart
            run_once()
        except Exception as e:
            # SELF-HEAL: the cloud->local Kafka bridge (external tunnel) intermittently fails to resolve; a mid-stream
            # reconnect then throws StreamingQueryException and — without this catch — the run DIES for hours (no
            # auto-restart). Catch it and resume from the SAME checkpoint within the SAME run (seconds), so the
            # scorer rides through tunnel blips instead of going dark. This is what kept the flywheel stalled.
            print(f"[score-rl] stream failed ({type(e).__name__}: {str(e)[:180]}); resuming from checkpoint...")
        time.sleep(INTERVAL)
else:
    run_once()
dbutils.notebook.exit(json.dumps({"status": "ok"}))
