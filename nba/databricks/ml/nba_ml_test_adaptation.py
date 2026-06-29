# Databricks notebook source
# TEST: ADAPTATION — proves the model RE-LEARNS when the world (f*) changes. Self-contained (no nested notebook runs):
# trains TWO worlds inline using the SAME production contract — common.FSTAR + CHANNEL_AFFINITY (the ground truth),
# PropensityModel (the served wrapper), TRAIN_FEATS / FEATURE_COLS / ARMS (the feature + arm vocabulary) and the same
# HistGradientBoosting estimator the pipeline uses. So it tests the real learning dynamics end to end. We compare a
# PAIR of condition-specific actions (a1c test vs care-manager outreach) so the result is robust to channel calibration
# and to the high-base "welcome" actions:
#   Phase A: baseline f*  -> a DIABETIC member prefers a1c, a HIGH-RISK/SDOH member prefers care-manager.
#   Phase B: SWAP the a1c <-> care-manager coefficient dicts (a genuinely new world) -> retrain -> the preferences FLIP.
# Failing assertions raise (CI-style). A pass = shift the ground truth, the next retrain tracks it.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, copy, numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
ML_NS, SRC_NS = ml_widgets()
rng = np.random.default_rng(7)
N = 200_000

# parametric synthetic population over the HEALTHCARE feature contract (no external dependency -> deterministic + fast)
M = pd.DataFrame({
    "daysSinceLogin":   rng.integers(0, 60, N).astype(float),
    "riskScore":        np.clip(rng.normal(1.5, 1.0, N), 0, 5),
    "diabetic":         (rng.random(N) < 0.30).astype(float),
    "isDNC":            (rng.random(N) < 0.05).astype(float),
    "smsConsent":       (rng.random(N) < 0.65).astype(float),
    "age":              np.clip(rng.normal(68, 12, N), 18, 95),
    "sdohBarrier":      (rng.random(N) < 0.25).astype(float),
    "comorbidityCount": rng.poisson(1.8, N).astype(float),
    "erVisits12mo":     rng.poisson(0.4, N).astype(float),
    "rxAdherencePDC":   np.clip(rng.normal(0.7, 0.2, N), 0, 1),
    "openCareGaps":     rng.poisson(1.5, N).astype(float),
    "portalLogins30d":  rng.poisson(2.0, N).astype(float),
    "pagesViewed30d":   rng.poisson(3.0, N).astype(float)})

CH = {c: i for i, c in enumerate(CHANNELS)}
AC = {a: i for i, a in enumerate(HEALTHCARE_ACTIONS)}

def fstar_vec(world, aid, ch):                          # VECTORIZED ground truth for `world` over the population
    spec = world.get(aid, {})
    z = np.full(N, float(spec.get("b", 0.0)))
    for k, w in spec.items():
        if k != "b" and k in M.columns: z = z + float(w) * M[k].to_numpy()
    for k, w in CHANNEL_AFFINITY.get(ch, {}).items():
        if k in M.columns: z = z + float(w) * M[k].to_numpy()
    p = 1.0 / (1.0 + np.exp(-z))
    p = np.where(M["isDNC"].to_numpy() >= 1, 0.0, p)
    if ch == "sms": p = np.where(M["smsConsent"].to_numpy() < 1, 0.0, p)
    return np.clip(p, 0.0, 0.98)

def train_world(world):                                 # generate labeled data from `world`, fit the served model
    K = len(ARMS)
    elig = np.column_stack([((M["isDNC"].to_numpy() < 1) &
                             ~(np.full(N, c == "sms") & (M["smsConsent"].to_numpy() < 1))) for _, c in ARMS])
    pstar = np.column_stack([fstar_vec(world, a, c) for a, c in ARMS])
    choice = (rng.random((N, K)) * elig).argmax(1)      # broad exploration over eligible arms -> full coverage
    label = (rng.random(N) < pstar[np.arange(N), choice]).astype(int)
    out = M.copy(); out["actionId"] = [ARMS[k][0] for k in choice]; out["channel"] = [ARMS[k][1] for k in choice]
    out["label"] = label
    for c in FEATURE_COLS:                               # full schema for TRAIN_FEATS
        if c not in out.columns: out[c] = 0.0
    out["channel_idx"] = out["channel"].map(CH).fillna(-1).astype(float)
    out["action_idx"]  = out["actionId"].map(AC).fillna(-1).astype(float)
    clf = HistGradientBoostingClassifier(max_iter=300, max_depth=6, learning_rate=0.08)
    clf.fit(out[TRAIN_FEATS].astype(float), out["label"].astype(int))
    return PropensityModel(clf, CH, AC)

# two distinct healthcare members: a DIABETIC with care gaps vs a HIGH-RISK, SDOH-barriered complex member.
ARCHE = {
    "DIABETIC":  {"age": 62, "riskScore": 1.5, "diabetic": 1, "openCareGaps": 3, "comorbidityCount": 1, "portalLogins30d": 4, "smsConsent": 1},
    "HIGH_RISK": {"age": 78, "riskScore": 3.5, "comorbidityCount": 4, "sdohBarrier": 1, "erVisits12mo": 3, "openCareGaps": 2, "portalLogins30d": 0, "smsConsent": 1},
}
PAIR = ["action_a1c_test", "action_care_manager_outreach"]
def pref(model):                                        # archetype -> which of the PAIR the model scores higher (best channel)
    res = {}
    for name, f in ARCHE.items():
        df = pd.DataFrame([{**{c: float(f.get(c, 0.0)) for c in FEATURE_COLS}, "actionId": a, "channel": ch}
                           for a in PAIR for ch in CHANNELS])
        df["p"] = model.predict(None, df[FEATURE_COLS + ["actionId", "channel"]])
        res[name] = df.groupby("actionId")["p"].max().idxmax()
    return res

# ---- Phase A: baseline world ----
prefA = pref(train_world(FSTAR)); print("Phase A (baseline) pref:", prefA)

# ---- Phase B: SWAP a1c <-> care-manager coefficient dicts (genuinely new world) ----
SHIFTED = copy.deepcopy(FSTAR)
SHIFTED["action_a1c_test"], SHIFTED["action_care_manager_outreach"] = FSTAR["action_care_manager_outreach"], FSTAR["action_a1c_test"]
prefB = pref(train_world(SHIFTED)); print("Phase B (shifted)  pref:", prefB)

a1c, cm = "action_a1c_test", "action_care_manager_outreach"
checks = [
    ("A: DIABETIC  -> a1c",                    prefA["DIABETIC"] == a1c),
    ("A: HIGH_RISK -> care manager",           prefA["HIGH_RISK"] == cm),
    ("B: DIABETIC  FLIPS -> care manager",     prefB["DIABETIC"] == cm),
    ("B: HIGH_RISK FLIPS -> a1c",              prefB["HIGH_RISK"] == a1c),
    ("adaptation: DIABETIC decision changed",  prefA["DIABETIC"] != prefB["DIABETIC"]),
    ("adaptation: HIGH_RISK decision changed", prefA["HIGH_RISK"] != prefB["HIGH_RISK"]),
]
print("\n=== ADAPTATION TEST ===")
ok = True
for name, passed in checks:
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}"); ok = ok and bool(passed)
if not ok: raise Exception("ADAPTATION TEST FAILED — the model did not track the shifted world")
print("\nALL PASS — the model adapts: shift f*, the retrain re-ranks the actions to match.")
dbutils.notebook.exit(json.dumps({"phaseA": prefA, "phaseB": prefB, "pass": ok, "checks": [[n, bool(p)] for n, p in checks]}))
