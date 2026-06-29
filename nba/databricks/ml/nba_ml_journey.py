# Databricks notebook source
"""LAKE -> MDP adapter: reconstruct REAL member journeys from logged sends into the SAME (state, action, reward,
next_state, done) transitions the CQL trains on. This is how the offline-RL policy learns from real outcomes. The
state/action/reward/milestone definitions are imported from nba_journey_env so the real transitions share the EXACT
obs layout the trainer + scorer use (and the dynamic-sim transitions unioned in by nba_ml_build_journey_set).
%run-loadable + importable.

A real journey = a member's ordered DISPATCH events (each = one arm sent at a time), with the point-in-time feature
snapshot captured at each send. Between consecutive sends the member's features evolve (they did tasks, viewed the
dashboard, used chat); reward = milestone value gained over that interval, minus the send cost. We read the realized
next features straight from the lake, so we DON'T need to know whether a send 'landed' — the outcome is observed.
NOOP steps aren't observable (no dispatch row), so the reconstructed MDP is naturally a sends-only sequence — correct
for real data."""
import numpy as np
try:                                                                   # LOCAL: real import
    from nba_journey_env import (FEATURE_COLS, ALL_FEAT, DISP_COLS, CHANNELS, MAX_STEPS, SEND_COST, MS_VALUES,
                                 _milestones_batch, _obs_batch)
except (ImportError, ModuleNotFoundError):                            # DATABRICKS: names already in scope from `%run ./nba_journey_env`
    pass
HOLD_CHANNEL = "__hold"                                                # a non-dispatched evaluation = a 'hold' decision step
# the catalog supplies NOOP at runtime (load_catalog -> len(arms)); this is only the fallback default when a caller
# doesn't pass one. Fatigue is OFF in the reconstruction reward (over-contact is already penalized in milestone terms).
_DEFAULT_NOOP = -1; FATIGUE_COEF = 0.0; FREE_CONTACTS = 2.0

# PER-(action,channel) arm map, passed in at RUNTIME from nba_ml_catalog.load_catalog -> {(actionId, channel): i}, with
# noop = len(arms). No module-load import-timing. Falls back to channel-keying (CH_ARM) for an unmodeled (action,channel).
# CH_ARM keys the STATE's disposition-bearing CHANNELS (the catalog is the real arm source; this is a serving warm-start).
CH_ARM = {ch: i for i, ch in enumerate(CHANNELS)}


def arm_index(action, channel, arm_idx, noop):
    if str(channel) == HOLD_CHANNEL: return noop                       # non-dispatched evaluation = the system held (no send)
    a = arm_idx.get((str(action), str(channel))) if arm_idx else None
    return a if a is not None else CH_ARM.get(str(channel), -1)


def reconstruct_member(F, actions, channels, final_F, arm_idx, noop):
    """One member's journey. F=(k,NF+disp) feature snapshots at each send (chronological); actions/channels=k strings;
    final_F the member's features AFTER the last send (terminal next-state). Returns (obs, action, reward, next_obs, done)."""
    F = np.asarray(F, np.float32); k = len(F)
    ms = _milestones_batch(F)                                       # (k, n_milestones) milestones holding at each send
    nms = ms.shape[1]
    O, A, R, S2, D = [], [], [], [], []
    for i in range(k):
        a = arm_index(actions[i], channels[i], arm_idx, noop)
        if a < 0:
            continue
        obs = _obs_batch(F[i:i + 1], ms[i:i + 1], i)[0]
        if i + 1 < k:
            Fn, msn, stepn = F[i + 1], ms[i + 1], i + 1
        else:                                                      # terminal: observed final features
            if final_F is not None:
                fv = np.asarray(final_F, np.float32)
                Fn = fv if len(fv) == F.shape[1] else np.concatenate([fv, np.zeros(F.shape[1] - len(fv), np.float32)])
            else:
                Fn = F[i]
            msn = _milestones_batch(Fn[None])[0]; stepn = i + 1
        obs2 = _obs_batch(Fn[None], msn[None], stepn)[0]
        gained = msn & ~ms[i]                                      # milestones newly reached over the interval
        cost = 0.0 if a == noop else (SEND_COST + FATIGUE_COEF * max(0.0, float(F[i, FEATURE_COLS.index("totalThisWeek")]) - FREE_CONTACTS))
        reward = float((gained.astype(np.float64) * MS_VALUES).sum() - cost)
        done = (i + 1 >= k) or bool(msn[nms - 1])                  # last send OR the deep goal (STARS) reached
        O.append(obs); A.append(int(a)); R.append(reward); S2.append(obs2); D.append(bool(done))
    return O, A, R, S2, D


def reconstruct_pandas(df, final_by_member=None, arm_idx=None, noop=None):
    """df: pandas [entityId, decisionTs, actionId, channel] + FEATURE_COLS (one row per SEND). arm_idx/noop from the
    catalog. Returns transition arrays O,A,R,S2,DONE keyed per (action, channel)."""
    if noop is None: noop = _DEFAULT_NOOP
    for dc in DISP_COLS:
        if dc not in df.columns: df[dc] = 0.0
    if "actionId" not in df.columns: df["actionId"] = ""
    O, A, R, S2, D = [], [], [], [], []
    for ent, g in df.sort_values("decisionTs").groupby("entityId"):
        g = g.reset_index(drop=True)
        F = g[ALL_FEAT].to_numpy(np.float32); chans = g["channel"].tolist(); acts = g["actionId"].tolist()
        fin = (final_by_member or {}).get(ent)
        o, a, r, s2, d = reconstruct_member(F, acts, chans, fin, arm_idx, noop)
        O += o; A += a; R += r; S2 += s2; D += d
    return (np.asarray(O, np.float32), np.asarray(A, np.int64), np.asarray(R, np.float32),
            np.asarray(S2, np.float32), np.asarray(D, np.float32))
