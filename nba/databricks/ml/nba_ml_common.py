# Databricks notebook source
# COMMON — shared config, the feature contract, the heuristic-we-run-today, kafka produce, label logic.
# %run-included by the other ML notebooks (defines names with no side effects). Keep this the single source
# of truth for the model's input vocabulary so train and score can never drift.
import json, math, hashlib, random
import numpy as np
import mlflow.pyfunc
from pyspark.sql import functions as F, types as T
from databricks.sdk.runtime import dbutils, spark   # so these resolve whether %run-included or imported

# ---------- catalogs / widgets ----------
def ml_widgets():
    for k, v in [("ml_catalog", "nba_ml"), ("ml_schema", "core"),
                 ("src_catalog", "workspace"), ("src_schema", "nba_poc"),
                 ("bootstrap", ""), ("sasl_user", ""), ("sasl_pass", "")]:
        try: dbutils.widgets.text(k, v)
        except Exception: pass
    g = dbutils.widgets.get
    ml_ns = f"{g('ml_catalog')}.{g('ml_schema')}"
    src_ns = f"{g('src_catalog')}.{g('src_schema')}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ml_ns}")   # catalog pre-exists (ml_worspace); SP owns schemas it makes
    return ml_ns, src_ns

def model_name(ml_ns):           # the single UC-registered model, champion/challenger via aliases
    return f"{ml_ns}.nba_propensity"

# ---------- the feature contract (model input) ----------
# (key in the snapshot/gold facts, feature column name, kind). The model also takes actionId + channel.
# HEALTHCARE member-state features the model trains on. Digital + care facts (each flipped by an action that drives it)
# + member attributes (riskScore/diabetic) for personalization + recency/consent/contact-load. The model learns which
# (action, channel) best moves the next fact for THIS member.
FEATURE_KEYS = [
    ("operator.activity.daysSinceLogin",         "daysSinceLogin",         "num"),
    ("operator.activity.respondedToOutreach",    "respondedToOutreach",    "bool"),
    ("operator.activity.registeredForPortal",    "registeredForPortal",    "bool"),
    ("operator.activity.loggedIn",               "loggedIn",               "bool"),
    ("operator.activity.viewedBenefits",         "viewedBenefits",         "bool"),
    ("operator.activity.hraCompleted",           "hraCompleted",           "bool"),
    ("operator.activity.pcpSelected",            "pcpSelected",            "bool"),
    ("operator.activity.careTeamEngaged",        "careTeamEngaged",        "bool"),
    ("operator.activity.awvCompleted",           "awvCompleted",           "bool"),
    ("operator.activity.medAdherent",            "medAdherent",            "bool"),
    ("operator.activity.mammogramDone",          "mammogramDone",          "bool"),
    ("operator.activity.a1cControlled",          "a1cControlled",          "bool"),
    ("operator.activity.colonoscopyDone",        "colonoscopyDone",        "bool"),
    ("operator.profile.riskScore",               "riskScore",              "num"),
    ("operator.profile.diabetic",                "diabetic",               "bool"),
    ("operator.profile.isDNC",                   "isDNC",                  "bool"),
    ("operator.profile.smsConsent",              "smsConsent",             "bool"),
    ("operator.comms.totalThisWeek",             "totalThisWeek",          "num"),
    ("operator.comms.emailsThisWeek",            "emailsThisWeek",         "num"),
    # ── DECISION ATTRIBUTES (who the member is) — the model reasons over THESE to pick the best action/channel; they do
    # NOT gate eligibility. profile + claims + engagement telemetry, the inputs a real NBA uses for personalization.
    ("operator.profile.age",                     "age",                    "num"),
    ("operator.profile.planDSNP",                "planDSNP",               "bool"),   # dual-eligible -> more support
    ("operator.profile.tenureMonths",            "tenureMonths",           "num"),
    ("operator.profile.sdohBarrier",             "sdohBarrier",            "bool"),   # transport/food/housing -> human touch
    ("operator.clinical.comorbidityCount",       "comorbidityCount",       "num"),    # complexity -> care management
    ("operator.clinical.erVisits12mo",           "erVisits12mo",           "num"),    # avoidable utilization -> care mgr
    ("operator.clinical.rxAdherencePDC",         "rxAdherencePDC",         "num"),    # low PDC -> med adherence action
    ("operator.clinical.openCareGaps",           "openCareGaps",           "num"),    # which screening to prioritize
    ("operator.activity.portalLogins30d",        "portalLogins30d",        "num"),    # digital engagement -> channel
    ("operator.activity.pagesViewed30d",         "pagesViewed30d",         "num"),    # interest signal
    ("operator.activity.avgTimeOnPageSec",       "avgTimeOnPageSec",       "num"),    # depth of engagement
    ("operator.activity.benefitsPageViews",      "benefitsPageViews",      "num"),    # researching benefits -> readiness
]
GOLD_FEATS = [c for _, c, _ in FEATURE_KEYS]               # snapshot/gold-sourced member features
# CONTEXTUAL fatigue state — NOT in the snapshot; computed per (member, action, channel) candidate from the recent send
# history (scorer per-candidate / training point-in-time / sim synthetic). This is what lets the model LEARN the dynamic
# fatigue: "hit sms 6x and this exact ask 4x this week, last touch an hour ago -> it converts poorly, route elsewhere."
CONTEXT_FEATS = ["thisChannelRecentN", "thisActionRecentN", "daysSinceLastContact"]
FEATURE_COLS = GOLD_FEATS + CONTEXT_FEATS
CHANNELS = ["email", "sms", "push", "voice", "mail"]

def _coerce(raw, kind):
    if isinstance(raw, dict): raw = raw.get("value")
    if kind == "bool":
        # The live activation layer emits driven bool facts as LONG 1 (not all are in its BOOL_FEATS set), so accept
        # BOTH representations: literal true OR a numeric >= 1. Otherwise the model reads every climbing member as baseline.
        s = str(raw).strip().lower()
        if raw is True or s in ("true", "t", "yes"): return 1.0
        try: return 1.0 if float(s) >= 1 else 0.0
        except Exception: return 0.0
    try: return float(raw)
    except Exception: return 0.0

def features_from_factmap(facts: dict) -> dict:
    """factsJson map {key -> {value,...}} (or key->value) -> {featureCol: float}."""
    return {c: _coerce(facts.get(k), kind) for k, c, kind in FEATURE_KEYS}

FEATURE_STRUCT = T.StructType([T.StructField(c, T.DoubleType()) for c in GOLD_FEATS])

@F.udf(FEATURE_STRUCT)
def parse_features_udf(facts_json):    # snapshot -> GOLD features only; the CONTEXT_FEATS are added by the caller
    try: facts = json.loads(facts_json) if facts_json else {}
    except Exception: facts = {}
    f = features_from_factmap(facts)
    return tuple(f[c] for c in GOLD_FEATS)

# ---------- the heuristic we run TODAY (verbatim from ml-scorer) ----------
# Kept here so train_initial can register it AS the starting champion (zero-shock cutover) and so the learned
# model can be A/B'd against the exact current behavior.
def heuristic_score(f: dict, channel: str) -> float:
    s = 0.5
    s -= min(0.30, f.get("daysSinceLogin", 0.0) * 0.01)
    s += min(0.20, f.get("completedTasks", 0.0) * 0.02)
    if f.get("usedChat", 0.0) >= 1: s += 0.10
    if f.get("viewedDashboard", 0.0) >= 1: s += 0.05
    s -= min(0.20, f.get("totalThisWeek", 0.0) * 0.05)
    if channel == "email": s += 0.05
    elif channel == "sms": s -= 0.05
    return max(0.01, min(0.99, round(s, 4)))

# ---------- GROUND-TRUTH DYNAMIC response model — MIRRORS nba_source_sim.py (keep the two in sync). The sim trains the
# model on THIS world so synthetic coverage matches the real outcomes the source-sim produces. Every touch:
#   convert = action_fit(f*·facts) · channel_propensity[member archetype][ch] · fatigue(recent channel/action/total/recency)
# with the DNC + sms-consent gates. So the model must learn BOTH who-fits-what AND the cost of repetition -> a varied mix.
HEALTHCARE_ACTIONS = [
    "action_plan_welcome", "action_reengage", "action_portal_registration", "action_login_reminder",
    "action_benefits_education", "action_hra", "action_hra_reminder", "action_pcp_selection",
    "action_care_manager_outreach", "action_wellness_education", "action_annual_wellness_visit",
    "action_med_adherence", "action_mammogram", "action_a1c_test", "action_colonoscopy",
]
ARMS = [(a, c) for a in HEALTHCARE_ACTIONS for c in CHANNELS]
FSTAR = {            # ACTION fit (fact-dominant) — verbatim from nba_source_sim.FSTAR
    "action_plan_welcome": {"b": 1.4, "daysSinceLogin": 0.02},
    "action_reengage": {"b": -1.8, "daysSinceLogin": 0.08},
    "action_portal_registration": {"b": 1.4},
    "action_login_reminder": {"b": -1.2, "registeredForPortal": 2.6},
    "action_benefits_education": {"b": -0.8, "benefitsPageViews": 0.5, "pagesViewed30d": 0.03},
    "action_hra": {"b": 1.2}, "action_hra_reminder": {"b": 1.0}, "action_pcp_selection": {"b": 1.1},
    "action_care_manager_outreach": {"b": -3.2, "riskScore": 3.6, "comorbidityCount": 0.5, "erVisits12mo": 0.4, "sdohBarrier": 1.5},
    "action_wellness_education": {"b": 0.6, "riskScore": -2.6, "age": -0.012},
    "action_annual_wellness_visit": {"b": -0.6, "openCareGaps": 0.42},
    "action_med_adherence": {"b": -1.2, "diabetic": 2.6, "rxAdherencePDC": -2.0},
    "action_mammogram": {"b": -1.2, "openCareGaps": 0.5},
    "action_a1c_test": {"b": -3.2, "diabetic": 5.6},
    "action_colonoscopy": {"b": -1.2, "openCareGaps": 0.5},
}

def action_fit(action, f: dict) -> float:           # P(this action suits this member), independent of channel/fatigue
    spec = FSTAR.get(action)
    if spec is None: return 0.0
    z = spec.get("b", 0.0) + sum(w * f.get(k, 0.0) for k, w in spec.items() if k != "b")
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))

def channel_propensity(eid, f: dict) -> dict:       # per-member archetype VECTOR (verbatim nba_source_sim.channel_propensity)
    digital = max(0.0, min(1.0, (f.get("portalLogins30d", 0.0) / 15.0 + f.get("pagesViewed30d", 0.0) / 40.0) / 2.0))
    old = max(0.0, min(1.0, (f.get("age", 65.0) - 55.0) / 30.0))
    comp = max(0.0, min(1.0, f.get("comorbidityCount", 0.0) / 5.0))
    sdoh = 1.0 if f.get("sdohBarrier", 0.0) >= 1 else 0.0
    base = {"email": 0.35 + 0.55 * digital, "push": 0.18 + 0.55 * digital - 0.12 * old,
            "sms": 0.40 + 0.28 * digital - 0.10 * old, "voice": 0.20 + 0.45 * old + 0.45 * comp,
            "mail": 0.24 + 0.42 * old * (1.0 - digital) + 0.30 * sdoh - 0.22 * comp}
    r = random.Random(int(hashlib.md5((str(eid) + "|chan").encode()).hexdigest(), 16))   # deterministic per-member tilt
    return {ch: max(0.03, min(1.0, base[ch] * math.exp(r.gauss(0.0, 0.45)))) for ch in CHANNELS}

def member_tolerance(eid):
    r = random.Random(int(hashlib.md5((str(eid) + "|tol").encode()).hexdigest(), 16))
    return max(0.4, min(2.2, r.gauss(1.0, 0.4)))

def fatigue(ch_n, act_n, tot_n, days_since, tol):   # response multiplier from recent history (verbatim source-sim.fatigue)
    f_ch = math.exp(-0.40 / tol * ch_n); f_act = math.exp(-0.55 / tol * act_n)
    f_tot = math.exp(-0.10 / tol * tot_n); f_rec = 0.4 + 0.6 * (1.0 - math.exp(-max(0.0, days_since) / 2.5))
    return f_ch * f_act * f_tot * f_rec

def dyn_convert(action, ch, f, eid, ch_n=0, act_n=0, tot_n=0, days_since=14.0) -> float:
    """full dynamic convert prob — mirrors nba_source_sim.convert_prob × fatigue. The model must LEARN this from data."""
    if f.get("isDNC", 0.0) >= 1: return 0.0
    if ch == "sms" and f.get("smsConsent", 0.0) < 1: return 0.0
    return action_fit(action, f) * channel_propensity(eid, f).get(ch, 0.3) * fatigue(ch_n, act_n, tot_n, days_since, member_tolerance(eid))

# ---------- serveable models (both expose predict() -> P(convert), so champion/challenger are interchangeable) ----------
TRAIN_FEATS = FEATURE_COLS + ["channel_idx", "action_idx"]

class HeuristicModel(mlflow.pyfunc.PythonModel):
    """The current production heuristic, as a model — the zero-shock @champion baseline ('what we do today')."""
    def predict(self, ctx, df):
        out = []
        for _, r in df.iterrows():
            f = {c: float(r.get(c, 0.0)) for c in FEATURE_COLS}
            out.append(heuristic_score(f, str(r.get("channel", "email"))))
        return np.array(out)

class PropensityModel(mlflow.pyfunc.PythonModel):
    """Wraps a fitted sklearn classifier + the channel/action encoders. predict() -> P(convert | features, arm)."""
    def __init__(self, estimator=None, channels=None, actions=None):
        self.estimator = estimator; self.channels = channels or {}; self.actions = actions or {}
    def _X(self, df):
        X = df[FEATURE_COLS].astype(float).copy()
        X["channel_idx"] = df["channel"].map(self.channels).fillna(-1).astype(float)
        X["action_idx"] = df["actionId"].map(self.actions).fillna(-1).astype(float)
        return X[TRAIN_FEATS]
    def predict(self, ctx, df):
        return self.estimator.predict_proba(self._X(df))[:, 1]

class PropensityEnsemble(mlflow.pyfunc.PythonModel):
    """K bootstrapped estimators for THOMPSON exploration. predict() returns the MEAN P(convert) so every existing
    consumer (evaluate, score_batch, OPE) keeps working unchanged. The live scorer instead reads .estimators and
    samples per decision: an arm the bootstraps DISAGREE on (cold / under-observed) gets a high draw sometimes
    (explored); as real data accrues the bootstraps converge and the draw collapses to the mean (pure exploit).
    No epsilon, no floor — self-annealing, and the fraction of bootstraps that pick an arm IS its selection
    propensity (the honest P(selected) the OPE needs)."""
    def __init__(self, estimators=None, channels=None, actions=None):
        self.estimators = list(estimators or []); self.channels = channels or {}; self.actions = actions or {}
    def _X(self, df):
        X = df[FEATURE_COLS].astype(float).copy()
        X["channel_idx"] = df["channel"].map(self.channels).fillna(-1).astype(float)
        X["action_idx"] = df["actionId"].map(self.actions).fillna(-1).astype(float)
        return X[TRAIN_FEATS]
    def predict_all(self, df):
        """K x N matrix of per-bootstrap P(convert) — the scorer's input for Thompson sampling + vote propensity."""
        X = self._X(df)
        return np.vstack([e.predict_proba(X)[:, 1] for e in self.estimators])
    def predict(self, ctx, df):
        return self.predict_all(df).mean(axis=0)

# ---------- label policy (terminal workflow state -> training label) ----------
# Only REAL outcomes of a real send are labels:
#   HARD_COMPLETED -> 1 (converted).  EXPIRED / FAILED / DECLINED -> 0 (sent, fair shot, no convert).
# Everything else is a NON-OUTCOME, excluded (not a negative):
#   DEBOUNCED  -> a streaming artifact — the router fired on partial/bursty data and the state machine deduped a
#                 sibling; nothing was ever sent. The model must not learn from it.
#   SUPPRESSED -> the action was pulled (operator) or rerouted (throttle) before it got a fair shot.
#   non-terminal (PRESENTED/SOFT_COMPLETED/...) -> censored; excluded until the workflow ends.
# (build_training_set anchors on DISPATCH, so debounced/pre-send-suppressed decisions never even appear.)
# A valid label requires the member to have ACTUALLY RECEIVED the offer (the episode reached PRESENTED or
# beyond). Anything before that — they never saw it — is excluded, even FAILED (bounced) or IN_PROCESS->EXPIRED
# (sent but never confirmed delivered). The model predicts P(convert | the member got the offer); deliverability
# is a separate concern. DECLINED counts as received (they saw it and opted out -> a real 0). EXPIRED only counts
# as a 0 if the episode ALSO reached PRESENTED first (TTL elapsed after delivery, no convert).
PRESENTED_OR_BEYOND = {"PRESENTED", "SOFT_COMPLETED", "HARD_COMPLETED", "DECLINED"}

def label_from_states(states):
    """states = the SET of workflow states this send-episode reached -> 1 / 0 / None(exclude)."""
    states = set(states or [])
    if "HARD_COMPLETED" in states: return 1
    if states & PRESENTED_OR_BEYOND: return 0   # delivered, did not convert
    return None                                 # never reached the member -> not a fair label

# ---------- kafka produce (matches the lake jobs; origin=lake header so ingest drops our own emissions) ----------
# ---------- the live evaluation shape on nba.evaluations (compacted -> latest per member = current eligibility) ----------
EVAL_SCHEMA = T.StructType([
    T.StructField("nbaId", T.StringType()), T.StructField("entityType", T.StringType()),
    T.StructField("entityId", T.StringType()), T.StructField("correlationId", T.StringType()),
    T.StructField("evaluatedAt", T.LongType()),
    T.StructField("channelActions", T.ArrayType(T.StructType([
        T.StructField("actionId", T.StringType()), T.StructField("channel", T.StringType()),
        T.StructField("name", T.StringType()), T.StructField("eligible", T.BooleanType()),
        T.StructField("active", T.BooleanType()), T.StructField("cancellable", T.BooleanType()),
        T.StructField("hardCompleted", T.BooleanType()), T.StructField("contentKey", T.StringType()),
        T.StructField("ttlSeconds", T.LongType()), T.StructField("score", T.DoubleType())]))),
])

# ---------- exploration: softmax over scores (temperature). argmax of a softmax sample = principled exploration
# with a KNOWN propensity (what makes off-policy learning unbiased). temp -> 0 = pure exploit (today). ----------
def softmax(scores, temp=0.2):
    x = np.asarray(scores, dtype=float) / max(float(temp), 1e-6)
    x = x - x.max()
    e = np.exp(x)
    s = e.sum()
    return (e / s) if s > 0 else np.full(len(x), 1.0 / max(len(x), 1))

def kafka_cfg(key):
    """Kafka config from the job param, falling back to the 'nba-kafka' SECRET SCOPE when empty. This closes the
    deploy gap: the bundle vars default to "" and a bundle deploy that doesn't supply BUNDLE_VAR_kafka_* would ship
    an empty bootstrap -> the streams crash on 'Failed to construct kafka consumer' (this took the scorer down once).
    The secret scope is the durable source of truth and survives any redeploy."""
    v = ""
    try: v = dbutils.widgets.get(key)
    except Exception: pass
    if not v:
        try: v = dbutils.secrets.get("nba-kafka", key)
        except Exception: pass
    return v

def sasl_opts():
    boot, su, sp = kafka_cfg("bootstrap"), kafka_cfg("sasl_user"), kafka_cfg("sasl_pass")
    if not boot:
        raise ValueError("kafka bootstrap is EMPTY (no widget param and no 'nba-kafka' secret scope) — "
                         "set BUNDLE_VAR_kafka_bootstrap or the nba-kafka secret scope. Refusing to start a "
                         "stream that would crash on an empty consumer config (the deploy-gap guard).")
    jaas = (f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required '
            f'username="{su}" password="{sp}";')
    return {"kafka.bootstrap.servers": boot, "kafka.security.protocol": "SASL_PLAINTEXT",
            "kafka.sasl.mechanism": "SCRAM-SHA-256", "kafka.sasl.jaas.config": jaas}
