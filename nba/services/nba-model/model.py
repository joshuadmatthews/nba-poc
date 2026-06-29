"""Local CQL Q-net inference — pure numpy, NO Spark/Databricks/torch. Ported from nba_ml_score_rl.py (reload_qnet/qnet/
build_obs), nba_ml_common.py (FEATURE_KEYS/_coerce) and nba_journey_env.py (the disposition + milestone STATE layout).

DATA-DRIVEN from the qnet JSON: the observation is reconstructed from the artifact's OWN feature_cols / milestones /
state_dim / obs_mean, so the same code serves the legacy 13-dim model AND the healthcare 50-dim model unchanged. The
ONE invariant enforced per request: len(obs) == state_dim (else the model would silently score garbage)."""
import json, os, time, threading
import numpy as np

# ── feature contract: featureCol -> (snapshot fact key, kind). Verbatim from nba_ml_common.FEATURE_KEYS. Cols a qnet
#    references that aren't here fall back to operator.activity.<col> then 0.0 (covers the legacy completedTasks/etc.).
FEATURE_KEYS = [
    ("operator.activity.daysSinceLogin", "daysSinceLogin", "num"),
    ("operator.activity.respondedToOutreach", "respondedToOutreach", "bool"),
    ("operator.activity.registeredForPortal", "registeredForPortal", "bool"),
    ("operator.activity.loggedIn", "loggedIn", "bool"),
    ("operator.activity.viewedBenefits", "viewedBenefits", "bool"),
    ("operator.activity.hraCompleted", "hraCompleted", "bool"),
    ("operator.activity.pcpSelected", "pcpSelected", "bool"),
    ("operator.activity.careTeamEngaged", "careTeamEngaged", "bool"),
    ("operator.activity.awvCompleted", "awvCompleted", "bool"),
    ("operator.activity.medAdherent", "medAdherent", "bool"),
    ("operator.activity.mammogramDone", "mammogramDone", "bool"),
    ("operator.activity.a1cControlled", "a1cControlled", "bool"),
    ("operator.activity.colonoscopyDone", "colonoscopyDone", "bool"),
    ("operator.profile.riskScore", "riskScore", "num"),
    ("operator.profile.diabetic", "diabetic", "bool"),
    ("operator.profile.isDNC", "isDNC", "bool"),
    ("operator.profile.smsConsent", "smsConsent", "bool"),
    ("operator.comms.totalThisWeek", "totalThisWeek", "num"),
    ("operator.comms.emailsThisWeek", "emailsThisWeek", "num"),
    ("operator.profile.age", "age", "num"),
    ("operator.profile.planDSNP", "planDSNP", "bool"),
    ("operator.profile.tenureMonths", "tenureMonths", "num"),
    ("operator.profile.sdohBarrier", "sdohBarrier", "bool"),
    ("operator.clinical.comorbidityCount", "comorbidityCount", "num"),
    ("operator.clinical.erVisits12mo", "erVisits12mo", "num"),
    ("operator.clinical.rxAdherencePDC", "rxAdherencePDC", "num"),
    ("operator.clinical.openCareGaps", "openCareGaps", "num"),
    ("operator.activity.portalLogins30d", "portalLogins30d", "num"),
    ("operator.activity.pagesViewed30d", "pagesViewed30d", "num"),
    ("operator.activity.avgTimeOnPageSec", "avgTimeOnPageSec", "num"),
    ("operator.activity.benefitsPageViews", "benefitsPageViews", "num"),
]
COL_TO_KEY = {col: (key, kind) for key, col, kind in FEATURE_KEYS}

# ── per-channel disposition block layout (nba_journey_env.py:35-71). DISP_AT[(ch,disp)] -> offset in the 13-wide block.
DISPOSITIONS_BY_CHANNEL = {
    "email": ["opened", "link_clicked", "ignored", "unsubscribe"],
    "sms": ["link_clicked", "ignored", "stop"],
    "push": ["opened", "dismissed", "ignored"],
    "voice": ["answered", "declined", "ignored"],
}
RAW_TO_DISP = {
    "email": {"Opened": "opened", "LinkClicked": "link_clicked", "Unsubscribe": "unsubscribe", "EXPIRED": "ignored"},
    "sms": {"LinkClicked": "link_clicked", "STOP": "stop", "EXPIRED": "ignored"},
    "push": {"Opened": "opened", "Dismissed": "dismissed", "EXPIRED": "ignored"},
    "voice": {"Answered": "answered", "Completed": "answered", "Declined": "declined", "EXPIRED": "ignored"},
}
DISP_CHANNELS = list(DISPOSITIONS_BY_CHANNEL.keys())
DISP_AT, DISP_COLS = {}, []
for _ch in DISP_CHANNELS:
    for _d in DISPOSITIONS_BY_CHANNEL[_ch]:
        DISP_AT[(_ch, _d)] = len(DISP_COLS); DISP_COLS.append(f"{_d}_{_ch}")
NDISP_TOTAL = len(DISP_COLS)   # 13

# ── milestone predicates by id (nba_journey_env.py:75-81), evaluated by feature NAME (absent -> 0). A qnet whose
#    milestone id isn't here (legacy activated/onboarded/upgraded) -> 0, which is correct (its features are absent too).
MILESTONE_PREDS = {
    "reached": lambda g: g("respondedToOutreach") >= 1,
    "registered": lambda g: g("registeredForPortal") >= 1,
    "assessed": lambda g: g("hraCompleted") >= 1,
    "engaged": lambda g: g("pcpSelected") >= 1 and g("careTeamEngaged") >= 1,
    "stars": lambda g: g("awvCompleted") >= 1 and g("medAdherent") >= 1 and g("mammogramDone") >= 1,
}


def _coerce(raw, kind):                       # verbatim from nba_ml_common._coerce — bool facts arrive as true OR LONG 1
    if isinstance(raw, dict):
        raw = raw.get("value")
    if kind == "bool":
        s = str(raw).strip().lower()
        if raw is True or s in ("true", "t", "yes"):
            return 1.0
        try:
            return 1.0 if float(s) >= 1 else 0.0
        except Exception:
            return 0.0
    try:
        return float(raw)
    except Exception:
        return 0.0


class QNet:
    """Loaded model artifact + the data-driven observation builder. Thread-safe swap on mtime change."""

    def __init__(self, path):
        self.path = path
        self.mtime = 0.0
        self._lock = threading.Lock()
        self.load()

    def load(self):
        q = json.load(open(self.path))
        self.weights = [(np.array(l["W"], np.float32), np.array(l["b"], np.float32)) for l in q["layers"]]
        self.arms = q["arms"]                                            # [{actionId, channel}, ...]
        self.arm_idx = {(a["actionId"], a["channel"]): i for i, a in enumerate(self.arms)}
        self.ch_to_arm = {}
        for i, a in enumerate(self.arms):
            self.ch_to_arm.setdefault(a["channel"], i)                   # first arm on a channel = its fallback
        self.feature_cols = q.get("feature_cols") or []
        self.milestone_ids = [m["id"] for m in q.get("milestones", [])]
        self.state_dim = int(q.get("state_dim") or self.weights[0][0].shape[1])
        self.max_steps = float(q.get("max_steps") or 18.0)
        self.noop_index = q.get("noop_index", len(self.arms))
        self.obs_mean = np.array(q["obs_mean"], np.float32) if q.get("obs_mean") else None
        self.obs_std = np.array(q["obs_std"], np.float32) if q.get("obs_std") else None
        self.nf = len(self.feature_cols)
        self.nms = len(self.milestone_ids)
        self.ndisp = self.state_dim - self.nf - self.nms - 1            # disposition block width implied by state_dim
        self.has_disp = self.ndisp > 0
        self.mtime = os.path.getmtime(self.path)

    def maybe_reload(self):
        try:
            if os.path.getmtime(self.path) != self.mtime:
                with self._lock:
                    self.load()
        except Exception:
            pass                                                        # keep last-good on a transient read error

    # ── observation: [features | dispositions(if any) | milestones | step] — must equal state_dim ──────────────────
    def build_obs(self, facts):
        feat = np.array([_coerce(facts.get(COL_TO_KEY.get(c, ("operator.activity." + c, "num"))[0]),
                                 COL_TO_KEY.get(c, (None, "num"))[1]) for c in self.feature_cols], np.float32)
        disp = self._disp_block(facts) if self.has_disp else np.zeros(0, np.float32)
        byname = {c: feat[i] for i, c in enumerate(self.feature_cols)}
        g = lambda k: byname.get(k, 0.0)
        ms = np.array([1.0 if (mid in MILESTONE_PREDS and MILESTONE_PREDS[mid](g)) else 0.0
                       for mid in self.milestone_ids], np.float32)
        step = np.float32(min(max(byname.get("totalThisWeek", 0.0), 0.0), self.max_steps) / self.max_steps)
        obs = np.concatenate([feat, disp, ms, [step]]).astype(np.float32)
        if len(obs) != self.state_dim:
            raise ValueError(f"obs width {len(obs)} != state_dim {self.state_dim} "
                             f"(nf={self.nf} ndisp={self.ndisp} nms={self.nms})")
        return obs

    def _disp_block(self, facts):
        block = np.zeros(NDISP_TOTAL, np.float32)
        latest = {}                                                     # channel -> (eventTs, raw)
        for k, v in facts.items():
            if not k.startswith("nba.disposition."):
                continue
            ch = k.rsplit(".", 1)[-1]
            if ch not in DISPOSITIONS_BY_CHANNEL:
                continue
            raw = v.get("value") if isinstance(v, dict) else v
            ts = float(v.get("eventTs", 0)) if isinstance(v, dict) else 0.0
            if ch not in latest or ts >= latest[ch][0]:
                latest[ch] = (ts, str(raw))
        for ch, (_, raw) in latest.items():
            disp = RAW_TO_DISP.get(ch, {}).get(raw)
            off = DISP_AT.get((ch, disp)) if disp else None
            if off is not None and self.ndisp == NDISP_TOTAL:
                block[off] = 1.0
        return block

    def forward(self, X):                                               # (n, state_dim) -> (n, action_dim)
        if self.obs_mean is not None:
            X = (X - self.obs_mean) / self.obs_std
        for i, (W, b) in enumerate(self.weights):
            X = X @ W.T + b
            if i < len(self.weights) - 1:
                X = np.maximum(X, 0.0)
        return X

    def score(self, facts, candidates):
        obs = self.build_obs(facts)
        Q = self.forward(obs[None, :])[0]
        out = []
        for c in candidates:
            aid, ch = c.get("actionId"), c.get("channel")
            i = self.arm_idx.get((aid, ch))
            if i is None:
                i = self.ch_to_arm.get(ch)
            out.append({"actionId": aid, "channel": ch,
                        "score": float(Q[i]) if i is not None else None})
        hold = float(Q[self.noop_index]) if 0 <= self.noop_index < len(Q) else None
        return out, hold

    def health(self):
        return {"status": "ok", "state_dim": self.state_dim, "n_arms": len(self.arms),
                "feature_cols_len": self.nf, "n_milestones": self.nms, "ndisp": self.ndisp,
                "has_obs_norm": self.obs_mean is not None, "qnet_mtime": self.mtime,
                "layers": [list(w.shape) for w, _ in self.weights]}


_QNET = None


def get_qnet():
    global _QNET
    path = os.environ.get("NBA_QNET_PATH", "/app/rl_qnet.json")
    if _QNET is None:
        _QNET = QNet(path)
    else:
        _QNET.maybe_reload()
    return _QNET
