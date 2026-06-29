#!/usr/bin/env python
"""PROVE the data->learn loop. The lake data had warm lift -0.586 (the behavior was ABSENT/inverted). Here we put the
warm behavior into the GENERATOR (the dummy world's truth: a channel that soft-completed converts MORE on follow-up),
generate logs, confirm the signal is now POSITIVE in the data, and show a response model LEARNS/recovers the warm lift.
That's the whole thesis: a behavior is born in the data generator, then learned from the data — never hard-coded in
the consumer. Sourced entirely from the generated data."""
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

rng = np.random.default_rng(0)
CHANNELS = ["email", "sms", "push", "voice"]
BASE = {"email": -1.2, "sms": -1.5, "push": -0.6, "voice": -0.9}      # base convert logit per channel
WARM_LIFT = 1.6                                                       # GENERATIVE TRUTH: a warm channel converts more

def p_convert(ch, warm, x):                                          # x = member engagement propensity
    return 1.0 / (1.0 + np.exp(-(BASE[ch] + 0.5 * x + WARM_LIFT * warm)))

# generate touch logs: per (member, channel), a soft-complete (no convert) sets warm -> the follow-up converts more
rows = []
for m in range(25000):
    x = rng.normal()
    for ch in CHANNELS:
        warm = 0
        for _touch in range(int(rng.integers(1, 4))):
            hard = rng.random() < p_convert(ch, warm, x)
            rows.append((ch, warm, round(float(x), 3), int(hard)))
            if hard: break                                          # converted -> done with this channel
            warm = 1                                                # engaged but didn't convert -> SOFT-COMPLETE -> warm
df = pd.DataFrame(rows, columns=["channel", "warm", "x", "hard"])
print(f"generated {len(df)} touch records")

# 1) SIGNAL CHECK on the generated data (the same check that gave -0.586 on the lake)
r_warm = df[df.warm == 1].hard.mean(); r_cold = df[df.warm == 0].hard.mean()
print(f"\nGENERATED-data warm signal: P(hard|warm)={r_warm:.3f}  P(hard|cold)={r_cold:.3f}  -> lift {r_warm-r_cold:+.3f}")
print(f"  (the LAKE data had lift -0.586 — behavior absent. The generator now PUTS it in the data.)")

# 2) LEARN: train a response model on [channel, warm, x] -> hard. Does it RECOVER the warm effect?
ci = {c: i for i, c in enumerate(CHANNELS)}
X = np.column_stack([df.channel.map(ci).values, df.warm.values, df.x.values]).astype(float)
clf = HistGradientBoostingClassifier(max_iter=250, learning_rate=0.08).fit(X, df.hard.values)
print("\nLEARNED warm lift per channel (model recovers the effect from data):")
recovered = []
for ch in CHANNELS:
    cold = float(clf.predict_proba([[ci[ch], 0, 0.0]])[0, 1]); warm = float(clf.predict_proba([[ci[ch], 1, 0.0]])[0, 1])
    recovered.append(warm - cold)
    print(f"  {ch:6s}: P(hard) cold={cold:.3f}  warm={warm:.3f}  -> learned lift {warm-cold:+.3f}")

avg_recovered = float(np.mean(recovered))
print(f"\n  avg learned warm lift = {avg_recovered:+.3f}  (all 4 channels positive: {all(r > 0 for r in recovered)})")
assert all(r > 0.05 for r in recovered), "the model must recover a positive warm lift on every channel"
print("\nPASS: the warm behavior is IN the generated data (lift positive, not -0.586) AND the model LEARNS it.")
print("=> behavior sourced from the data generator, recovered by a learned model — no hard-coded heuristic in the policy.")
