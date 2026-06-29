# Databricks notebook source
"""LIVE A/B HARNESS — the real-world validation that OPE can only estimate. Assign each member to RL vs control by a
STABLE hash (deterministic, stateless — same member always lands in the same arm), and measure the milestone-rate
lift with a two-proportion z-test. In serving, the scorer reads assign_variant(memberId): 'rl' members get the CQL
policy's action, 'control' members get today's myopic action — so a holdout keeps running while the policy is live,
and real Upgraded-rate (not a simulator estimate) decides. This module owns assignment + measurement so the
experiment is auditable + reproducible. Tested on simulated populations: nba_ml_ab_local.py."""
import hashlib, math
import numpy as np


def assign_variant(member_id, rl_fraction=0.5, salt="nba-rl-ab"):
    """Deterministic, stateless assignment. Same member_id + salt -> same arm forever. rl_fraction = treatment share."""
    h = int(hashlib.md5(f"{salt}:{member_id}".encode()).hexdigest()[:8], 16) / float(0xFFFFFFFF)
    return "rl" if h < rl_fraction else "control"


def ab_measure(variant, reached):
    """variant: array of 'rl'/'control'; reached: 0/1 array (e.g. reached Upgraded). Returns per-arm rate + two-
    proportion z-test of RL vs control (the standard experiment readout)."""
    variant = np.asarray(variant); reached = np.asarray(reached, float)
    arm = {}
    for v in ("control", "rl"):
        m = variant == v; n = int(m.sum()); x = float(reached[m].sum())
        arm[v] = {"n": n, "reached": int(x), "rate": round(x / n, 4) if n else 0.0}
    n1, x1 = arm["rl"]["n"], arm["rl"]["reached"]; n0, x0 = arm["control"]["n"], arm["control"]["reached"]
    p1 = x1 / n1 if n1 else 0.0; p0 = x0 / n0 if n0 else 0.0
    p = (x1 + x0) / (n1 + n0) if (n1 + n0) else 0.0
    se = math.sqrt(p * (1 - p) * (1 / n1 + 1 / n0)) if (0 < p < 1 and n1 and n0) else 0.0
    z = (p1 - p0) / se if se else 0.0
    pval = math.erfc(abs(z) / math.sqrt(2))                            # two-sided, normal approximation
    return {"control": arm["control"], "rl": arm["rl"], "abs_lift": round(p1 - p0, 4),
            "rel_lift": round((p1 - p0) / p0, 3) if p0 else None,
            "z": round(z, 2), "p_value": round(pval, 5), "significant_95": bool(pval < 0.05)}
