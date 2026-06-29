# Databricks notebook source
# SERVE — register the CQL Q-net (the RL policy) as an MLflow pyfunc in Unity Catalog so it can be deployed to a
# Databricks MODEL SERVING endpoint (the production "model endpoint in Databricks"). Self-contained pyfunc: the
# numpy inference + the observation builder (features | dispositions | milestones | step) are embedded so the model
# runs standalone on serving compute, with the qnet artifact baked in. Mirrors nba_ml_train's pyfunc registration.
#
# Input  (serving): a DataFrame with a single "facts" column = the member's fact map as a JSON STRING.
# Output (serving): one JSON STRING per row = {"scores":[{actionId,channel,score},...], "holdScore": float}.
#   The action-library posts the member's facts (from its gold-fed cache), gets per-arm Q-values, picks the eligible ones.

# COMMAND ----------
# MAGIC %run ./nba_ml_common

# COMMAND ----------
import json, mlflow, numpy as np
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient

ML_NS, SRC_NS = ml_widgets()
mlflow.set_registry_uri("databricks-uc")
_cat, _sch = ML_NS.split(".")
dbutils.widgets.text("qnet_path", ""); dbutils.widgets.text("endpoint", "nba-cql")   # defaults so standalone/loop runs need no extra params
QPATH = dbutils.widgets.get("qnet_path") or f"/Volumes/{_cat}/{_sch}/ckpt/rl_qnet.json"
NAME = f"{ML_NS}.nba_cql"

# COMMAND ----------
class CQLModel(mlflow.pyfunc.PythonModel):
    """Self-contained CQL Q-net serving model. All constants/logic live in load_context so the serving env needs
    only the qnet artifact + numpy (no %run, no Spark). Data-driven from the qnet JSON (feature_cols/milestones/
    state_dim/obs_mean) so it serves whatever policy is registered."""

    def load_context(self, context):
        import json, numpy as np
        self.np = np
        q = json.load(open(context.artifacts["qnet"]))
        self.W = [(np.array(l["W"], np.float32), np.array(l["b"], np.float32)) for l in q["layers"]]
        self.arms = q["arms"]
        self.feature_cols = q.get("feature_cols") or []
        self.milestone_ids = [m["id"] for m in q.get("milestones", [])]
        self.state_dim = int(q.get("state_dim") or self.W[0][0].shape[1])
        self.max_steps = float(q.get("max_steps") or 18.0)
        self.noop_index = int(q.get("noop_index", len(self.arms)))
        self.obs_mean = np.array(q["obs_mean"], np.float32) if q.get("obs_mean") else None
        self.obs_std = np.array(q["obs_std"], np.float32) if q.get("obs_std") else None
        self.nf = len(self.feature_cols); self.nms = len(self.milestone_ids)
        self.ndisp = self.state_dim - self.nf - self.nms - 1
        # feature contract (mirror nba_ml_common.FEATURE_KEYS)
        self.COL_TO_KEY = {
            "daysSinceLogin": ("operator.activity.daysSinceLogin", "num"), "respondedToOutreach": ("operator.activity.respondedToOutreach", "bool"),
            "registeredForPortal": ("operator.activity.registeredForPortal", "bool"), "loggedIn": ("operator.activity.loggedIn", "bool"),
            "viewedBenefits": ("operator.activity.viewedBenefits", "bool"), "hraCompleted": ("operator.activity.hraCompleted", "bool"),
            "pcpSelected": ("operator.activity.pcpSelected", "bool"), "careTeamEngaged": ("operator.activity.careTeamEngaged", "bool"),
            "awvCompleted": ("operator.activity.awvCompleted", "bool"), "medAdherent": ("operator.activity.medAdherent", "bool"),
            "mammogramDone": ("operator.activity.mammogramDone", "bool"), "a1cControlled": ("operator.activity.a1cControlled", "bool"),
            "colonoscopyDone": ("operator.activity.colonoscopyDone", "bool"), "riskScore": ("operator.profile.riskScore", "num"),
            "diabetic": ("operator.profile.diabetic", "bool"), "isDNC": ("operator.profile.isDNC", "bool"),
            "smsConsent": ("operator.profile.smsConsent", "bool"), "totalThisWeek": ("operator.comms.totalThisWeek", "num"),
            "emailsThisWeek": ("operator.comms.emailsThisWeek", "num"), "age": ("operator.profile.age", "num"),
            "planDSNP": ("operator.profile.planDSNP", "bool"), "tenureMonths": ("operator.profile.tenureMonths", "num"),
            "sdohBarrier": ("operator.profile.sdohBarrier", "bool"), "comorbidityCount": ("operator.clinical.comorbidityCount", "num"),
            "erVisits12mo": ("operator.clinical.erVisits12mo", "num"), "rxAdherencePDC": ("operator.clinical.rxAdherencePDC", "num"),
            "openCareGaps": ("operator.clinical.openCareGaps", "num"), "portalLogins30d": ("operator.activity.portalLogins30d", "num"),
            "pagesViewed30d": ("operator.activity.pagesViewed30d", "num"), "avgTimeOnPageSec": ("operator.activity.avgTimeOnPageSec", "num"),
            "benefitsPageViews": ("operator.activity.benefitsPageViews", "num"),
        }
        DBC = {"email": ["opened", "link_clicked", "ignored", "unsubscribe"], "sms": ["link_clicked", "ignored", "stop"],
               "push": ["opened", "dismissed", "ignored"], "voice": ["answered", "declined", "ignored"]}
        self.RAW_TO_DISP = {"email": {"Opened": "opened", "LinkClicked": "link_clicked", "Unsubscribe": "unsubscribe", "EXPIRED": "ignored"},
                            "sms": {"LinkClicked": "link_clicked", "STOP": "stop", "EXPIRED": "ignored"},
                            "push": {"Opened": "opened", "Dismissed": "dismissed", "EXPIRED": "ignored"},
                            "voice": {"Answered": "answered", "Completed": "answered", "Declined": "declined", "EXPIRED": "ignored"}}
        self.DISP_AT = {}; cols = []
        for ch in DBC:
            for d in DBC[ch]: self.DISP_AT[(ch, d)] = len(cols); cols.append(d + "_" + ch)
        self.NDISP = len(cols)
        self.MS_PREDS = {"reached": lambda g: g("respondedToOutreach") >= 1, "registered": lambda g: g("registeredForPortal") >= 1,
                         "assessed": lambda g: g("hraCompleted") >= 1, "engaged": lambda g: g("pcpSelected") >= 1 and g("careTeamEngaged") >= 1,
                         "stars": lambda g: g("awvCompleted") >= 1 and g("medAdherent") >= 1 and g("mammogramDone") >= 1}

    def _coerce(self, raw, kind):
        if isinstance(raw, dict): raw = raw.get("value")
        if kind == "bool":
            s = str(raw).strip().lower()
            if raw is True or s in ("true", "t", "yes"): return 1.0
            try: return 1.0 if float(s) >= 1 else 0.0
            except Exception: return 0.0
        try: return float(raw)
        except Exception: return 0.0

    def _obs(self, facts):
        np = self.np
        feat = np.array([self._coerce(facts.get(self.COL_TO_KEY.get(c, ("operator.activity." + c, "num"))[0]),
                                      self.COL_TO_KEY.get(c, (None, "num"))[1]) for c in self.feature_cols], np.float32)
        disp = np.zeros(self.NDISP, np.float32)
        if self.ndisp > 0:
            latest = {}
            for k, v in facts.items():
                if not str(k).startswith("nba.disposition."): continue
                ch = str(k).rsplit(".", 1)[-1]
                raw = v.get("value") if isinstance(v, dict) else v
                ts = float(v.get("eventTs", 0)) if isinstance(v, dict) else 0.0
                if ch not in latest or ts >= latest[ch][0]: latest[ch] = (ts, str(raw))
            for ch, (_, raw) in latest.items():
                d = self.RAW_TO_DISP.get(ch, {}).get(raw); off = self.DISP_AT.get((ch, d)) if d else None
                if off is not None and self.ndisp == self.NDISP: disp[off] = 1.0
        byname = {c: feat[i] for i, c in enumerate(self.feature_cols)}; g = lambda k: byname.get(k, 0.0)
        ms = np.array([1.0 if (m in self.MS_PREDS and self.MS_PREDS[m](g)) else 0.0 for m in self.milestone_ids], np.float32)
        step = np.float32(min(max(byname.get("totalThisWeek", 0.0), 0.0), self.max_steps) / self.max_steps)
        return np.concatenate([feat, disp, ms, [step]]).astype(np.float32)

    def _qnet(self, X):
        np = self.np
        if self.obs_mean is not None: X = (X - self.obs_mean) / self.obs_std
        for i, (W, b) in enumerate(self.W):
            X = X @ W.T + b
            if i < len(self.W) - 1: X = np.maximum(X, 0.0)
        return X

    def predict(self, context, model_input):
        import json
        col = "facts" if "facts" in model_input.columns else model_input.columns[0]
        out = []
        for raw in model_input[col]:
            facts = json.loads(raw) if isinstance(raw, str) else (raw or {})
            Q = self._qnet(self._obs(facts)[None, :])[0]
            scores = [{"actionId": a["actionId"], "channel": a["channel"], "score": float(Q[i])} for i, a in enumerate(self.arms)]
            hold = float(Q[self.noop_index]) if 0 <= self.noop_index < len(Q) else None
            out.append(json.dumps({"scores": scores, "holdScore": hold}))
        return out

# COMMAND ----------
import pandas as pd, hashlib, requests
sig_in = pd.DataFrame({"facts": ['{"operator.profile.age":{"value":67},"operator.profile.isDNC":{"value":false}}']})
sig_out = np.array(["{}"])
_c = MlflowClient()
ENDPOINT = dbutils.widgets.get("endpoint") or "nba-cql"

# IDEMPOTENT register: only cut a NEW model version when the promoted qnet artifact actually CHANGED (content md5),
# so the hourly retrain loop doesn't spam the registry with identical versions. rl_promote only rewrites QPATH on a
# real promotion, so in steady state this cuts ~one version per promotion.
_md5 = hashlib.md5(open(QPATH, "rb").read()).hexdigest()
_latest = max((int(mv.version) for mv in _c.search_model_versions(f"name='{NAME}'")), default=0)
_prev = _c.get_model_version(NAME, str(_latest)).tags.get("qnet_md5") if _latest else None
if _prev == _md5:
    v = str(_latest); print(f"qnet unchanged (md5 {_md5[:8]}) -> reuse {NAME} v{v}")
else:
    with mlflow.start_run(run_name="cql-serve-register"):
        info = mlflow.pyfunc.log_model("model", python_model=CQLModel(), artifacts={"qnet": QPATH},
                                       registered_model_name=NAME, signature=infer_signature(sig_in, sig_out),
                                       pip_requirements=["numpy"])
    v = info.registered_model_version
    _c.set_model_version_tag(NAME, v, "qnet_md5", _md5)
    print(f"registered {NAME} v{v} (md5 {_md5[:8]})")
_c.set_registered_model_alias(NAME, "champion", v)

# DEPLOY: re-point the serving endpoint at this version so the hot-path dbx scorer serves the new champion
# AUTOMATICALLY. THIS was the missing link — the endpoint sat pinned to v1 forever. Idempotent: a no-op config
# update when it's already on v. The endpoint re-deploys async (~minutes); scale-to-zero kept (warms on traffic).
deployed = None
try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    _host, _tok = _ctx.apiUrl().get(), _ctx.apiToken().get()
    _cur = requests.get(f"{_host}/api/2.0/serving-endpoints/{ENDPOINT}", headers={"Authorization": f"Bearer {_tok}"}, timeout=30).json()
    _onv = next((se.get("entity_version") for se in (_cur.get("config", {}).get("served_entities") or []) if se.get("entity_name") == NAME), None)
    if str(_onv) == str(v):
        deployed = f"already on v{v}"
    else:
        _cfg = {"served_entities": [{"name": ENDPOINT, "entity_name": NAME, "entity_version": str(v),
                                     "workload_size": "Small", "scale_to_zero_enabled": True}]}
        _r = requests.put(f"{_host}/api/2.0/serving-endpoints/{ENDPOINT}/config",
                          headers={"Authorization": f"Bearer {_tok}"}, json=_cfg, timeout=60)
        deployed = f"HTTP {_r.status_code}: v{_onv}->v{v}"
    print(f"endpoint {ENDPOINT} -> {NAME} v{v} ({deployed})")
except Exception as _e:
    deployed = f"skipped ({_e})"; print(f"endpoint update {deployed}")
dbutils.notebook.exit(json.dumps({"model": NAME, "version": v, "qnet": QPATH, "md5": _md5, "endpoint": deployed}))
