# Databricks notebook source
# SOURCE-SYSTEM SIMULATOR -- in the real implementation a member's RESPONSE and the facts it generates come from SOURCE
# SYSTEMS (claims, portal, EHR) that drop records onto the bronze-ingress topic (datalake.streaming-inbound). Here the
# TEST plays those source systems: it consumes nba.activations (the action that was actually dispatched), reads the
# member's facts from gold + their RECENT CONTACT HISTORY from silver, applies the GROUND-TRUTH response model to decide
# a HARD COMPLETION (or an OPT-OUT), and drops onto datalake.streaming-inbound:
#   - the COMPLETION/outcome fact (the milestone the action drove), OR an OPT-OUT fact (DNC / channel burn), AND
#   - fresh ENGAGEMENT telemetry (the member is active right now -> new logins/pages, recency resets).
# The medallion carries these to gold; the MODEL reads gold. The action-layer only walks the delivery funnel; it never
# touches member facts. The model never sees this response model -- it must LEARN it from the outcomes in the data.
#
# THE RESPONSE IS DYNAMIC: a touch's outcome depends on the member's WHOLE recent history, not just their type. Three
# factors multiply: (a) intrinsic ACTION fit (f*·facts), (b) the member's CHANNEL PROPENSITY (an archetype vector,
# fact-correlated + per-member idiosyncrasy -> thousands of distinct profiles, each with 2-3 good channels), and (c)
# FATIGUE over recent contacts -- hammering the same channel, repeating the same action ("seen this ask already"), or
# too many touches all suppress the response, and at the extreme the member OPTS OUT. So the only good policy is a
# VARIED, paced mix -- the model must learn to ROTATE channels, VARY the ask, and PACE the cadence, not converge.

# COMMAND ----------

# SELF-CONTAINED (no %run) so this is a LAKE-bundle notebook, not ML. Gold + silver read IN-WORKSPACE.

# COMMAND ----------

import json, time, math, hashlib, random
import numpy as np
from pyspark.sql import functions as F

for _k, _v in [("src_catalog", "workspace"), ("src_schema", "nba_poc"), ("bootstrap", ""), ("sasl_user", ""),
               ("sasl_pass", ""), ("inbound_topic", "datalake.streaming-inbound"), ("act_topic", "nba.activations")]:
    try: dbutils.widgets.text(_k, _v)
    except Exception: pass
_g = dbutils.widgets.get
SRC_NS = f"{_g('src_catalog')}.{_g('src_schema')}"
INBOUND = _g("inbound_topic"); ACT = _g("act_topic")

def _kafka_cfg(key):                                   # job param first, else the nba-kafka secret scope (durable)
    v = ""
    try: v = _g(key)
    except Exception: pass
    if not v:
        try: v = dbutils.secrets.get("nba-kafka", key)
        except Exception: pass
    return v
def _sasl_opts():
    boot, su, sp = _kafka_cfg("bootstrap"), _kafka_cfg("sasl_user"), _kafka_cfg("sasl_pass")
    if not boot:
        raise ValueError("kafka bootstrap EMPTY — set BUNDLE_VAR_kafka_bootstrap or the nba-kafka secret scope")
    jaas = f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="{su}" password="{sp}";'
    return {"kafka.bootstrap.servers": boot, "kafka.security.protocol": "SASL_PLAINTEXT",
            "kafka.sasl.mechanism": "SCRAM-SHA-256", "kafka.sasl.jaas.config": jaas}
SASL = _sasl_opts()
_cat, _sch = SRC_NS.split("."); spark.sql(f"CREATE VOLUME IF NOT EXISTS {SRC_NS}.ckpt")
CKPT = f"/Volumes/{_cat}/{_sch}/ckpt/source_sim_v4"   # bumped: dynamic-response rebuild (fresh start at live edge)
CHANNELS = ["email", "sms", "push", "voice", "mail"]

# feature contract — (gold snapshot key, the model's feature name, kind). The inputs the response model reasons over.
FEATURE_KEYS = [
    ("operator.activity.daysSinceLogin", "daysSinceLogin", "num"), ("operator.activity.respondedToOutreach", "respondedToOutreach", "bool"),
    ("operator.activity.registeredForPortal", "registeredForPortal", "bool"), ("operator.activity.loggedIn", "loggedIn", "bool"),
    ("operator.activity.viewedBenefits", "viewedBenefits", "bool"), ("operator.activity.hraCompleted", "hraCompleted", "bool"),
    ("operator.activity.pcpSelected", "pcpSelected", "bool"), ("operator.activity.careTeamEngaged", "careTeamEngaged", "bool"),
    ("operator.activity.awvCompleted", "awvCompleted", "bool"), ("operator.activity.medAdherent", "medAdherent", "bool"),
    ("operator.activity.mammogramDone", "mammogramDone", "bool"), ("operator.activity.a1cControlled", "a1cControlled", "bool"),
    ("operator.activity.colonoscopyDone", "colonoscopyDone", "bool"), ("operator.profile.riskScore", "riskScore", "num"),
    ("operator.profile.diabetic", "diabetic", "bool"), ("operator.profile.isDNC", "isDNC", "bool"),
    ("operator.profile.smsConsent", "smsConsent", "bool"), ("operator.comms.totalThisWeek", "totalThisWeek", "num"),
    ("operator.comms.emailsThisWeek", "emailsThisWeek", "num"), ("operator.profile.age", "age", "num"),
    ("operator.profile.planDSNP", "planDSNP", "bool"), ("operator.profile.tenureMonths", "tenureMonths", "num"),
    ("operator.profile.sdohBarrier", "sdohBarrier", "bool"), ("operator.clinical.comorbidityCount", "comorbidityCount", "num"),
    ("operator.clinical.erVisits12mo", "erVisits12mo", "num"), ("operator.clinical.rxAdherencePDC", "rxAdherencePDC", "num"),
    ("operator.clinical.openCareGaps", "openCareGaps", "num"), ("operator.activity.portalLogins30d", "portalLogins30d", "num"),
    ("operator.activity.pagesViewed30d", "pagesViewed30d", "num"), ("operator.activity.avgTimeOnPageSec", "avgTimeOnPageSec", "num"),
    ("operator.activity.benefitsPageViews", "benefitsPageViews", "num"),
]
def _coerce(raw, kind):
    if isinstance(raw, dict): raw = raw.get("value")
    if kind == "bool":
        s = str(raw).strip().lower()
        if raw is True or s in ("true", "t", "yes"): return 1.0
        try: return 1.0 if float(s) >= 1 else 0.0
        except Exception: return 0.0
    try: return float(raw)
    except Exception: return 0.0

# ===================== GROUND-TRUTH RESPONSE MODEL (HIDDEN — the model must LEARN it from outcomes) =====================
# (1) ACTION fit: which action suits which member. FACT-DOMINANT (low base + high fact weights) so an action only
# converts for the RIGHT profile. Reach/early actions stay accessible so members climb.
FSTAR = {
    "action_plan_welcome": {"b": 1.4, "daysSinceLogin": 0.02},
    "action_reengage": {"b": -1.8, "daysSinceLogin": 0.08},                      # LAPSED: 45d->0.86, 5d->0.20
    "action_portal_registration": {"b": 1.4},
    "action_login_reminder": {"b": -1.2, "registeredForPortal": 2.6},
    "action_benefits_education": {"b": -0.8, "benefitsPageViews": 0.5, "pagesViewed30d": 0.03},
    "action_hra": {"b": 1.2}, "action_hra_reminder": {"b": 1.0}, "action_pcp_selection": {"b": 1.1},
    "action_care_manager_outreach": {"b": -3.2, "riskScore": 3.6, "comorbidityCount": 0.5, "erVisits12mo": 0.4, "sdohBarrier": 1.5},  # HIGH-RISK COMPLEX
    "action_wellness_education": {"b": 0.6, "riskScore": -2.6, "age": -0.012},   # LOW-RISK YOUNG
    "action_annual_wellness_visit": {"b": -0.6, "openCareGaps": 0.42},
    "action_med_adherence": {"b": -1.2, "diabetic": 2.6, "rxAdherencePDC": -2.0},  # DIABETIC LOW-ADHERENCE
    "action_mammogram": {"b": -1.2, "openCareGaps": 0.5},                        # OPEN-GAPS
    "action_a1c_test": {"b": -3.2, "diabetic": 5.6},                             # DIABETIC
    "action_colonoscopy": {"b": -1.2, "openCareGaps": 0.5},
}
EFFECT = {  # action -> the milestone/outcome fact a HARD COMPLETION drives
    "action_plan_welcome": "operator.activity.respondedToOutreach", "action_reengage": "operator.activity.respondedToOutreach",
    "action_portal_registration": "operator.activity.registeredForPortal", "action_login_reminder": "operator.activity.loggedIn",
    "action_benefits_education": "operator.activity.viewedBenefits", "action_hra": "operator.activity.hraCompleted",
    "action_hra_reminder": "operator.activity.hraCompleted", "action_pcp_selection": "operator.activity.pcpSelected",
    "action_care_manager_outreach": "operator.activity.careTeamEngaged", "action_wellness_education": "operator.activity.careTeamEngaged",
    "action_annual_wellness_visit": "operator.activity.awvCompleted", "action_med_adherence": "operator.activity.medAdherent",
    "action_mammogram": "operator.activity.mammogramDone", "action_a1c_test": "operator.activity.a1cControlled",
    "action_colonoscopy": "operator.activity.colonoscopyDone",
}

# (2) CHANNEL PROPENSITY: a per-member archetype VECTOR. Fact-correlated base (digital->email/sms/push, old/complex
# ->voice, old-low-digital/SDOH->mail) + a deterministic PER-MEMBER idiosyncratic tilt (log-normal) so there are
# thousands of distinct profiles, each with 2-3 GOOD channels. The model learns facts->propensity (its cold-start
# prior); the tilt is the irreducible per-member taste that exploration pins down.
def channel_propensity(eid, f):
    digital = max(0.0, min(1.0, (f.get("portalLogins30d", 0.0) / 15.0 + f.get("pagesViewed30d", 0.0) / 40.0) / 2.0))
    old = max(0.0, min(1.0, (f.get("age", 65.0) - 55.0) / 30.0))
    comp = max(0.0, min(1.0, f.get("comorbidityCount", 0.0) / 5.0))
    sdoh = 1.0 if f.get("sdohBarrier", 0.0) >= 1 else 0.0
    # TEXT-FIRST is a real, LEARNABLE archetype: digitally-engaged members (heavy app/portal users) prefer SMS and convert
    # well on it. SMS gets the STEEPEST slope on `digital` (0.72, vs email/push 0.55), so it is the clear BEST channel for
    # the high-digital slice — the model can identify them from features and route them to SMS instead of holding. email
    # stays the channel for moderate-digital; voice for older/complex; mail for old/low-digital/SDOH. (This is what was
    # missing: previously SMS's digital slope was the LOWEST (0.28), so it was never anyone's best channel -> always held.)
    base = {"email": 0.35 + 0.55 * digital,
            "push":  0.18 + 0.55 * digital - 0.12 * old,
            "sms":   0.38 + 0.72 * digital - 0.10 * old,
            "voice": 0.20 + 0.45 * old + 0.45 * comp,
            "mail":  0.24 + 0.42 * old * (1.0 - digital) + 0.30 * sdoh - 0.22 * comp}
    r = random.Random(int(hashlib.md5((eid + "|chan").encode()).hexdigest(), 16))   # deterministic per-member tilt
    return {ch: max(0.03, min(1.0, base[ch] * math.exp(r.gauss(0.0, 0.45)))) for ch in CHANNELS}

def member_tolerance(eid):   # per-member contact sensitivity (some tolerate many touches, some few)
    r = random.Random(int(hashlib.md5((eid + "|tol").encode()).hexdigest(), 16))
    return max(0.4, min(2.2, r.gauss(1.0, 0.4)))

# (3) DYNAMIC FATIGUE: the response multiplier from the member's RECENT contact history (all decays, scaled by tolerance).
def fatigue(ch_n, act_n, tot_n, days_since, tol):
    f_ch  = math.exp(-0.40 / tol * ch_n)                                   # same-channel saturation
    f_act = math.exp(-0.55 / tol * act_n)                                  # same-ASK repetition ("seen this already")
    f_tot = math.exp(-0.10 / tol * tot_n)                                  # overall contact load
    # MILD too-soon penalty with a 0.8 FLOOR. The loop runs in COMPRESSED time (touches are seconds apart in
    # wall-clock, not days), so days_since is ~0 on every RETRY -> a 0.4 floor pinned retries permanently and members
    # who missed attempt 1 could never "eventually complete." 0.8 keeps a small recency nudge while letting retries
    # stay viable; over-contact is still penalized by the count-based f_ch/f_act/f_tot terms above.
    # NOTE: watching whether the fresh-population ~95% settles into a healthier band as fatigue matures; if it stays
    # too high (no learning signal), drop this floor toward ~0.55 and give DNC/opt-out more teeth.
    f_rec = 0.8 + 0.2 * (1.0 - math.exp(-max(0.0, days_since) / 2.5))
    return f_ch * f_act * f_tot * f_rec

# DYNAMIC OPT-OUT: probability RISES with over-contact. The HARD per-channel opt-out is legally binding and exists ONLY
# on email (Unsubscribe) + sms (STOP) — emitted as the DISPOSITION the rules engine's BUILT-IN OPTOUT_RAW already latches
# (channel goes permanently ineligible). Over-contact on ANY channel can also escalate to a GLOBAL DNC. (voice/push/mail
# have no hard opt-out: the fatigue + the negative outcome are what the model learns from; they re-approach.)
HARD_OPTOUT_RAW = {"email": "Unsubscribe", "sms": "STOP"}                   # mirrors RulesEngine.OPTOUT_RAW
def opt_out_kind(ch, ch_n, tot_n, tol, u):
    # over-contact -> opt-out is a REAL but RARE risk (so a spammy policy is still punished), NOT a death spiral that
    # permanently removes the majority. The old rates (DNC 0.015*(tot-6) cap 0.30; burn 0.05*(ch-2) cap 0.45) drained
    # the population to all-EXPIRED over time; these let it climb to its milestones while keeping the restraint signal.
    p_dnc = min(0.06, max(0.0, 0.004 * (tot_n - 12))) / tol                # too many touches overall -> global DNC (rare)
    p_ch  = (min(0.20, max(0.0, 0.03 * (ch_n - 3))) / tol) if ch in HARD_OPTOUT_RAW else 0.0   # burn email/sms only (softened)
    if u < p_dnc: return "dnc"
    if u < p_dnc + p_ch: return "channel"
    return None

def action_fit(action, f):
    spec = FSTAR.get(action)
    if spec is None: return 0.0
    z = spec.get("b", 0.0) + sum(w * f.get(k, 0.0) for k, w in spec.items() if k != "b")
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))

# WARM-LIFT: a member who already SOFT-COMPLETED (engaged) on a channel converts materially MORE on the next touch — the
# realistic "warm" effect the CQL is meant to learn. soft-complete is a state on the obs, so the DATA must SHOW the lift or
# the model can't learn it (the lake historically showed NEGATIVE lift because this term was missing). warm in {0,1}.
WARM_LIFT = 1.0
def convert_prob(action, ch, f, prop, fat, warm=0.0):
    # per-channel opt-out (email Unsubscribe / sms STOP) is enforced UPSTREAM by the rules engine's built-in OPTOUT_RAW
    # (latched -> channel ineligible -> no more dispatches reach here), so we only race-gate global DNC + sms consent.
    if f.get("isDNC", 0.0) >= 1: return 0.0
    if ch == "sms" and f.get("smsConsent", 0.0) < 1: return 0.0
    return min(0.98, action_fit(action, f) * prop * fat * (1.0 + WARM_LIFT * warm))

def _nbafromtrk(trk):  # "nba-ca:{nbaId}:{actionId}:{channel}|{corr}"
    if not trk: return None
    head = trk.split("|", 1)[0].split(":")
    return head[1] if len(head) >= 2 else None

def read_facts(sp, entids):
    g = (sp.table(f"{SRC_NS}.gold_member_snapshot").where("key LIKE 'operator.%'").where(F.col("entityId").isin(entids)))
    wide = g.groupBy("entityId").pivot("key").agg(F.first("value")).toPandas()
    out = {}
    for _, row in wide.iterrows():
        d = {}
        for keyname, col, kind in FEATURE_KEYS:
            if keyname in wide.columns and row.get(keyname) is not None: d[col] = _coerce(row[keyname], kind)
        out[row["entityId"]] = d
    return out

# RECENT CONTACT HISTORY straight from the real sends (silver actionstate IN_PROCESS) — NOT the comms counts (those are
# emit_comms-derived and currently broken). Per member: per-channel + per-action + total counts, and last-send recency.
def contact_history(sp, entids, now):
    since = now - 14 * 86400 * 1000
    try:
        df = (sp.table(f"{SRC_NS}.silver_fact_history")
              .where("key LIKE 'nba.actionstate.%' AND value IN ('IN_PROCESS','SOFT_COMPLETED')")
              .where((F.col("eventTs") >= F.lit(since)) & F.col("entityId").isin(entids))
              .select("entityId", "actionId", "channel", "value", "eventTs").toPandas())
    except Exception:
        df = None
    H = {e: {"ch": {}, "act": {}, "tot": 0, "last": 0, "soft": {}} for e in entids}
    if df is not None:
        for _, r in df.iterrows():
            h = H.get(r["entityId"])
            if h is None: continue
            c, a = r.get("channel"), r.get("actionId")
            if r.get("value") == "SOFT_COMPLETED":
                if c: h["soft"][c] = 1.0                # WARM: engaged (soft-completed) on this channel -> lifts the next convert
                continue                                # a soft-complete is not a fresh send (don't fatigue on it)
            if c: h["ch"][c] = h["ch"].get(c, 0) + 1
            if a: h["act"][a] = h["act"].get(a, 0) + 1
            h["tot"] += 1
            h["last"] = max(h["last"], int(r.get("eventTs") or 0))
    return H

def process(batch_df, batch_id):
    sp = batch_df.sparkSession
    rows = batch_df.selectExpr("CAST(value AS STRING) v").collect()
    disp = []
    for r in rows:
        try: a = json.loads(r.v)
        except Exception: continue
        if a.get("op") != "DISPATCH": continue
        et, eid, ch = a.get("entityType", "OPERATOR"), a.get("memberId") or a.get("entityId"), a.get("channel", "")
        acts = a.get("actions") if isinstance(a.get("actions"), list) and a.get("actions") else [a]
        for act in acts:
            aid = act.get("actionId") or a.get("actionId")
            if eid and aid and aid in EFFECT: disp.append((et, str(eid), aid, ch, _nbafromtrk(act.get("trackingId") or a.get("trackingId"))))
    if not disp: return
    eids = list({d[1] for d in disp})
    facts = read_facts(sp, eids)
    H = contact_history(sp, eids, int(time.time() * 1000))
    now = int(time.time() * 1000); rng = np.random.RandomState(batch_id & 0x7fffffff); out = []; dispo = []
    nconv = noptout = 0
    def emit(eid, key, val, vt): out.append({"_src": "nba-sim", "entityId": eid, "key": key, "value": val, "valueType": vt, "ts": now})
    for et, eid, aid, ch, nb in disp:
        f = facts.get(eid, {}); h = H.get(eid, {"ch": {}, "act": {}, "tot": 0, "last": 0, "soft": {}})
        prop = channel_propensity(eid, f).get(ch, 0.3); tol = member_tolerance(eid)
        days_since = (now - h["last"]) / 86400000.0 if h["last"] else 14.0
        fat = fatigue(h["ch"].get(ch, 0), h["act"].get(aid, 0), h["tot"], days_since, tol)
        if rng.random() < convert_prob(aid, ch, f, prop, fat, h.get("soft", {}).get(ch, 0.0)):
            emit(eid, EFFECT[aid], True, "BOOLEAN"); nconv += 1                       # HARD COMPLETION -> milestone fact
            emit(eid, "operator.activity.portalLogins30d", int(f.get("portalLogins30d", 0.0)) + 1, "LONG")
            emit(eid, "operator.activity.pagesViewed30d", int(f.get("pagesViewed30d", 0.0)) + int(rng.randint(2, 7)), "LONG")
            emit(eid, "operator.activity.daysSinceLogin", int(rng.randint(0, 2)), "LONG")
        else:                                                                          # no convert -> maybe OPT OUT
            kind = opt_out_kind(ch, h["ch"].get(ch, 0), h["tot"], tol, rng.random())
            if kind == "dnc":
                emit(eid, "operator.profile.isDNC", True, "BOOLEAN"); noptout += 1     # global -> the No DNC rule
            elif kind == "channel":                                                    # email Unsubscribe / sms STOP ->
                dispo.append({"entityType": "OPERATOR", "entityId": eid, "key": f"nba.disposition.{aid}.{ch}",
                              "value": HARD_OPTOUT_RAW[ch], "valueType": "STRING", "eventTs": now, "source": "nba-sim"}); noptout += 1
    if out:                                                                            # outcome/telemetry facts -> the lake
        kdf = sp.createDataFrame([(f'OPERATOR:{o["entityId"]}', json.dumps(o)) for o in out], "key string, value string")
        kdf.write.format("kafka").options(**SASL).option("topic", INBOUND).save()
    if dispo:                                                                          # opt-out dispositions -> member.facts
        ddf = sp.createDataFrame([(f'OPERATOR:{d["entityId"]}', json.dumps(d)) for d in dispo], "key string, value string")
        ddf.write.format("kafka").options(**SASL).option("topic", "nba.member.facts").save()   # -> snapshot -> OPTOUT_RAW latch
    print(f"batch {batch_id}: {len(disp)} dispatches -> {nconv} completions, {noptout} opt-outs -> {INBOUND}/member.facts")


# INBOUND member behavior now lives in the LOCAL inbound simulator (nba/services/nba-inbound-sim): warm members come
# inbound through the REAL inbound APIs (serve -> disposition -> completion), not a shortcut fact onto the bus. This sim
# models OUTBOUND response only; the warm-lift above (soft-complete raises conversion) stays here.
print(f"[source-sim] consuming {ACT} (DISPATCH) -> DYNAMIC ground-truth outcomes (fit x propensity x fatigue) + warm-lift -> {INBOUND}")
src = (spark.readStream.format("kafka").options(**SASL).option("subscribe", ACT)
       .option("startingOffsets", "latest").option("maxOffsetsPerTrigger", 4000).load())
def run_once():  # serverless: availableNow (drain available) in a loop, not a processingTime trigger
    src.writeStream.foreachBatch(process).option("checkpointLocation", CKPT).trigger(availableNow=True).start().awaitTermination()
while True:
    try:
        run_once()
    except Exception as e:
        print(f"[source-sim] stream failed ({type(e).__name__}: {str(e)[:160]}); resuming from checkpoint...")
    time.sleep(6)
