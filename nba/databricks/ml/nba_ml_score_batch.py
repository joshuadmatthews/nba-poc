# Databricks notebook source
# SCORE BATCH — the HEALTHCARE EXPLORE KICK (replaces the dead SaaS-propensity path). Periodically RE-EVALUATES every
# member against the CORRECTED catalog (computes fresh eligibility from each action's inclusion rule vs the member's
# current gold facts — so a newly-fixed action like a1c_test actually surfaces), then emits a UNIFORM-RANDOM score per
# eligible (action,channel). The router's argmax over random scores dispatches with UNBIASED coverage across the whole
# clinical catalog — exactly what bootstraps each action's base response rate from real journeys (incl. actions the
# historic playbook never sent). This is the natural periodic kick of the flywheel: re-eval -> explore-dispatch ->
# source-sim outcomes -> climb -> re-eval. Retire once a learned policy + live exploration carry it. (2026-06-19)
#
# NOT the old behaviour: it does NOT read silver_eval_eligible (that historic log predates the a1c fix and has zero
# a1c), and it does NOT load a champion model (the propensity @champion is a stale SaaS schema).

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, time, numpy as np
from pyspark.sql import functions as F
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("trigger", "once"); dbutils.widgets.text("interval_seconds", "180")
TRIGGER = dbutils.widgets.get("trigger"); INTERVAL = int(dbutils.widgets.get("interval_seconds"))
# Trickle the scores onto nba.member.facts in small chunks instead of one big write, so an explore batch doesn't
# flood the snapshot-builder and queue AHEAD of the higher-priority disposition/outcome facts (which drive completions).
dbutils.widgets.text("chunk_rows", "300"); dbutils.widgets.text("chunk_delay_seconds", "5")
CHUNK_ROWS = max(1, int(dbutils.widgets.get("chunk_rows"))); CHUNK_DELAY = int(dbutils.widgets.get("chunk_delay_seconds"))
SASL = sasl_opts()

# the fact each action DRIVES (autoExcludeOnCompletion => once set, the action is hard-completed and ineligible). Lets us
# exclude already-achieved actions without the live action-state. Mirrors seed-healthcare-nba.py A. operator.activity.*
DRIVES = {
    "action_plan_welcome": "operator.activity.respondedToOutreach", "action_reengage": "operator.activity.respondedToOutreach",
    "action_portal_registration": "operator.activity.registeredForPortal", "action_login_reminder": "operator.activity.loggedIn",
    "action_benefits_education": "operator.activity.viewedBenefits", "action_hra": "operator.activity.hraCompleted",
    "action_hra_reminder": "operator.activity.hraCompleted", "action_pcp_selection": "operator.activity.pcpSelected",
    "action_care_manager_outreach": "operator.activity.careTeamEngaged", "action_wellness_education": "operator.activity.careTeamEngaged",
    "action_annual_wellness_visit": "operator.activity.awvCompleted", "action_med_adherence": "operator.activity.medAdherent",
    "action_mammogram": "operator.activity.mammogramDone", "action_a1c_test": "operator.activity.a1cControlled",
    "action_colonoscopy": "operator.activity.colonoscopyDone",
}


def _num(v):                                   # coerce a fact value to a number: true->1, false->0, numeric->float, else 0
    if v is None: return 0.0
    s = str(v).strip().lower()
    if s == "true": return 1.0
    if s in ("false", ""): return 0.0
    try: return float(v)
    except Exception: return 0.0


def load_catalog_incl(spark):                  # action -> (inclusion conditions, channels) from the LIVE (cleaned) catalog
    rows = (spark.table(f"{SRC_NS}.dim_definitions").where("defType='ACTION' AND id IS NOT NULL")
            .select("id", "inclusionJson", "channelsJson").collect())
    out = []
    for r in rows:
        try: incl = json.loads(r["inclusionJson"]) if r["inclusionJson"] else {}
        except Exception: incl = {}
        conds = [(c.get("fact"), c.get("cmp", "gte"), float(c.get("value", 1))) for c in incl.get("conditions", []) if c.get("fact")]
        try: chans = [c.get("channel") for c in (json.loads(r["channelsJson"]) if r["channelsJson"] else []) if c.get("channel")]
        except Exception: chans = []
        out.append((r["id"], conds, chans))
    return out


def _ok(fv, cmp, val):
    if cmp == "gte": return fv >= val
    if cmp == "lte": return fv <= val
    if cmp == "lt":  return fv < val
    if cmp == "gt":  return fv > val
    if cmp == "eq":  return fv == val
    return fv >= val


def run_once(actions):
    g = spark.table(f"{SRC_NS}.gold_member_snapshot").where("key LIKE 'operator.%'")
    wide = g.groupBy("entityType", "entityId", "nbaId").pivot("key").agg(F.first("value")).toPandas()
    if not len(wide):
        print("no members in gold yet"); return 0
    now = int(time.time() * 1000); rng = np.random.RandomState(now & 0x7fffffff); rows = []
    for _, m in wide.iterrows():
        if _num(m.get("operator.profile.isDNC")) >= 1: continue                    # global: DNC
        if _num(m.get("operator.comms.totalThisWeek")) >= 3: continue              # global: comms cap 3/wk
        sms_ok = _num(m.get("operator.profile.smsConsent")) >= 1
        for aid, conds, chans in actions:
            if not all(_ok(_num(m.get(fk)), cmp, val) for fk, cmp, val in conds): continue   # inclusion (re-eval!)
            dr = DRIVES.get(aid)
            if dr and _num(m.get(dr)) >= 1: continue                               # hard-completed -> ineligible
            for ch in chans:
                if ch == "sms" and not sms_ok: continue
                rows.append((m["entityType"], m["entityId"], m["nbaId"], aid, ch, float(round(rng.random(), 4)), now))
    if not rows:
        print("no eligible (action,channel) candidates this pass"); return 0
    import pandas as pd
    cdf = pd.DataFrame(rows, columns=["entityType", "entityId", "nbaId", "actionId", "channel", "score", "ts"])
    cdf = cdf.sample(frac=1.0, random_state=now & 0x7fffffff).reset_index(drop=True)   # shuffle so each chunk is a mix
    def _emit_chunk(pdf):
        spark.createDataFrame(pdf).selectExpr(
            "concat_ws(':', entityType, entityId) AS key",
            "concat('{',"
            " '\"key\":\"nba.score.', actionId, '.', channel, '\",',"
            " '\"value\":', to_json(named_struct('actionId',actionId,'channel',channel,'score',score,'name',actionId,'policy','explore')), ',',"
            " '\"valueType\":\"OBJECT\",', '\"eventTs\":', cast(ts AS string), ',',"
            " '\"source\":\"ml\",', '\"nbaId\":\"', nbaId, '\",', '\"entityType\":\"', entityType, '\",', '\"entityId\":\"', entityId, '\"}') AS value"
        ).withColumn("headers", F.array(F.struct(F.lit("kind").alias("key"), F.lit("score").cast("binary").alias("value")))) \
         .write.format("kafka").options(**SASL).option("topic", "nba.member.facts").option("includeHeaders", "true").save()
    for i in range(0, len(cdf), CHUNK_ROWS):                                            # trickle in chunks so we don't
        _emit_chunk(cdf.iloc[i:i + CHUNK_ROWS])                                         # flood the snapshot-builder /
        if i + CHUNK_ROWS < len(cdf): time.sleep(CHUNK_DELAY)                           # delay disposition facts
    nm = cdf["nbaId"].nunique(); na = cdf["actionId"].nunique()
    print(f"explore-kick: re-evaluated -> {len(cdf)} eligible (action,channel) over {nm} members, {na} distinct actions "
          f"(a1c={int((cdf['actionId']=='action_a1c_test').sum())}, wellness={int((cdf['actionId']=='action_wellness_education').sum())}) "
          f"-> trickled in chunks of {CHUNK_ROWS} every {CHUNK_DELAY}s")
    return len(cdf)


actions = load_catalog_incl(spark)
print(f"catalog: {len(actions)} healthcare actions; trigger={TRIGGER}")
if TRIGGER == "continuous":
    while True:
        run_once(actions); time.sleep(INTERVAL)
else:
    n = run_once(actions)
    dbutils.notebook.exit(json.dumps({"scored": n}))
