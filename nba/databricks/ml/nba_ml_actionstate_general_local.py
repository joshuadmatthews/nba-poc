#!/usr/bin/env python
"""GENERAL action-state, no special-casing. Earlier we proved ONE state (warm/soft-complete) is learnable. Here the
generator gives every channel a recent DISPOSITION (engaged / declined / ignored / none), each with its OWN effect on
conversion — engaged lifts it, declined kills it, ignored dampens it. We generate the data and show a SINGLE response
model recovers the effect of EVERY disposition, with no per-state code. 'Warm' is just one row in the table the model
learns. This is the architecture: represent the full action-state, let the model discover what each state means."""
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

rng = np.random.default_rng(0)
CHANNELS = ["email", "sms", "push", "voice"]
BASE = {"email": -1.0, "sms": -1.4, "push": -0.5, "voice": -0.8}
DISPOSITIONS = ["none", "engaged", "declined", "ignored"]
# the GENERATIVE TRUTH: each disposition's effect on the convert logit (the model must recover these from data)
DISP_EFFECT = {"none": 0.0, "engaged": +1.6, "declined": -2.2, "ignored": -0.8}

def p_convert(ch, disp, x):
    return 1.0 / (1.0 + np.exp(-(BASE[ch] + 0.5 * x + DISP_EFFECT[disp])))

# generate touch logs: each touch's outcome depends on the channel's CURRENT disposition; the outcome UPDATES it
rows = []
for m in range(30000):
    x = rng.normal(); disp = {c: "none" for c in CHANNELS}
    for _ in range(int(rng.integers(2, 7))):
        ch = CHANNELS[rng.integers(4)]
        hard = rng.random() < p_convert(ch, disp[ch], x)
        rows.append((ch, disp[ch], round(float(x), 3), int(hard)))
        if hard:
            disp[ch] = "none"                                        # converted -> reset
        else:                                                       # not converted -> the member reacted somehow
            disp[ch] = rng.choice(["engaged", "declined", "ignored"], p=[0.5, 0.2, 0.3])
df = pd.DataFrame(rows, columns=["channel", "disposition", "x", "hard"])
print(f"generated {len(df)} touch records across {len(DISPOSITIONS)} dispositions")

# SIGNAL: convert rate by disposition (does the data carry each effect?)
print("\nconvert rate in the GENERATED data, by disposition:")
for d in DISPOSITIONS:
    sub = df[df.disposition == d]
    print(f"  {d:9s}: P(hard)={sub.hard.mean():.3f}  (n={len(sub)})")

# LEARN: one model on [channel, disposition, x] -> hard. Recover EVERY disposition's effect, no per-state code.
ci = {c: i for i, c in enumerate(CHANNELS)}; di = {d: i for i, d in enumerate(DISPOSITIONS)}
X = np.column_stack([df.channel.map(ci).values, df.disposition.map(di).values, df.x.values]).astype(float)
clf = HistGradientBoostingClassifier(max_iter=250, learning_rate=0.08).fit(X, df.hard.values)
print("\nLEARNED effect per disposition (avg over channels, vs 'none'):")
base_p = np.mean([clf.predict_proba([[ci[c], di["none"], 0.0]])[0, 1] for c in CHANNELS])
learned = {}
for d in DISPOSITIONS:
    pd_ = np.mean([clf.predict_proba([[ci[c], di[d], 0.0]])[0, 1] for c in CHANNELS])
    learned[d] = pd_ - base_p
    print(f"  {d:9s}: P(hard)={pd_:.3f}  learned lift vs none = {pd_-base_p:+.3f}")

assert learned["engaged"] > 0.1, "must learn engaged LIFTS conversion"
assert learned["declined"] < -0.1, "must learn declined HURTS conversion"
assert learned["ignored"] < 0, "must learn ignored dampens conversion"
print("\nPASS: ONE model learned the effect of EVERY action-state from data — engaged lifts, declined kills, ignored")
print("dampens — with zero per-state logic. 'Warm' is just the 'engaged' row. No special-casing anywhere.")
