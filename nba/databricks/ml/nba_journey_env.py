# Databricks notebook source
"""Self-contained journey-MDP STATE definition (numpy only — no Spark) so it imports locally AND %run-loads on Databricks.
This module is the SINGLE source of truth for the offline-RL STATE/observation: the feature contract, the per-channel
disposition (action-state) block, and the milestone ladder. The live pipeline uses ONLY these definitions — the real
reconstruction (nba_ml_build_journey_set / nba_ml_journey), the CQL trainer (nba_ml_rl_train), and the live scorer
(nba_ml_score_rl) all build their obs from the layout defined here so a state means the SAME thing everywhere.

NOTE: the synthetic 4-arm DEMO simulator (ARMS/DEMO_ARMS, FSTAR/fstar, ARM_EFFECT, JourneyWorld, collect_batch,
make_gym_env, the reference policies, and the opt-out/react/fatigue knobs) is REMOVED. The CQL trains on REAL
reconstructed journeys (nba_ml_build_journey_set) UNION'd with the DYNAMIC sim (nba_ml_simulate -> sim_examples), NOT
synthetic trajectories. The catalog (nba_ml_catalog.load_catalog) — not a hardcoded arm list — is THE arm source; this
module is action-agnostic and only defines the STATE."""
import numpy as np

# HEALTHCARE member-state (must match nba_ml_common.FEATURE_KEYS): digital + care facts + attributes + recency/consent.
FEATURE_COLS = ["daysSinceLogin", "respondedToOutreach", "registeredForPortal", "loggedIn", "viewedBenefits",
                "hraCompleted", "pcpSelected", "careTeamEngaged", "awvCompleted", "medAdherent", "mammogramDone",
                "a1cControlled", "colonoscopyDone", "riskScore", "diabetic", "isDNC", "smsConsent", "totalThisWeek", "emailsThisWeek",
                # DECISION ATTRIBUTES (must match nba_ml_common.FEATURE_KEYS order) — profile + claims + engagement telemetry
                "age", "planDSNP", "tenureMonths", "sdohBarrier", "comorbidityCount", "erVisits12mo", "rxAdherencePDC",
                "openCareGaps", "portalLogins30d", "pagesViewed30d", "avgTimeOnPageSec", "benefitsPageViews"]

# CANONICAL channel list = the channels that HAVE a disposition block in the STATE (i.e. DISPOSITIONS_BY_CHANNEL keys).
# This drives the per-channel disposition layout below. It is the STATE's channel vocabulary, NOT the action catalog —
# the catalog (nba_ml_catalog.load_catalog) is the arm source. NOTE: the dynamic sim covers a 5th channel ("mail"); mail
# carries no engagement disposition in this state contract, so it is intentionally absent here (state-dim neutral).
CHANNELS = ["email", "sms", "push", "voice"]

# ── ACTION-STATE = the channel's recent DISPOSITION ───────────────────────────────────────────────────────────────
# A disposition is a CHANNEL-SPECIFIC event the UNIFIED ACTIVATION LAYER emits (nba.disposition.{action}.{channel},
# value = the raw provider status) — the SAME signal that DRIVES the state machine (open/click/answer -> SOFT_COMPLETED;
# unsubscribe/STOP/declined -> DECLINED). We do NOT compute these from raw signals and we do NOT force a uniform grid:
# each channel has its OWN dispositions (mirrors ActionLayer CHANNEL_FUNNEL + DECLINE_RAW), and the model LEARNS each
# one's effect on conversion (proven general: nba_ml_actionstate_general_local.py). 'none' = the channel's flags all 0.
DISPOSITIONS_BY_CHANNEL = {
    "email": ["opened", "link_clicked", "ignored", "unsubscribe"],   # Opened/LinkClicked (ActionLayer), Unsubscribe = opt-out
    "sms":   ["link_clicked", "ignored", "stop"],                    # LinkClicked, STOP = opt-out (legally enforced)
    "push":  ["opened", "dismissed", "ignored"],                     # Opened, Dismissed (push opt-out = global isDNC)
    "voice": ["answered", "declined", "ignored"],                    # Answered, Declined = offer declined (opt-out = isDNC)
}
# map the ACTIVATION LAYER's raw provider status (the disposition fact's `value` in silver_fact_history) -> the env's
# disposition name, PER CHANNEL. ONE source of truth shared by training (this env), the live scorer, and the journey
# reconstruction — so a feature means the same thing everywhere. Mirrors ActionLayer CHANNEL_FUNNEL + DECLINE_RAW. A
# bare 'Delivered' (reached, no engagement yet) maps to no disposition (none); EXPIRED (actionstate) -> ignored.
RAW_TO_DISP = {
    "email": {"Opened": "opened", "LinkClicked": "link_clicked", "Unsubscribe": "unsubscribe", "EXPIRED": "ignored"},
    "sms":   {"LinkClicked": "link_clicked", "STOP": "stop", "EXPIRED": "ignored"},
    "push":  {"Opened": "opened", "Dismissed": "dismissed", "EXPIRED": "ignored"},
    "voice": {"Answered": "answered", "Completed": "answered", "Declined": "declined", "EXPIRED": "ignored"},
}
ENGAGING_DISPS = {"link_clicked", "answered", "opened"}              # a positive disposition IS engagement -> sets the flags
# Per-channel OPT-OUT disposition that ENFORCES ineligibility (a RULE, not a soft signal): the channel is never scored
# again for that member. email/sms have a true channel opt-out; push/voice opt-out is the GLOBAL isDNC (a declined offer
# or dismissed push does NOT permanently remove the channel — only Unsubscribe/STOP/DNC do).
OPTOUT_DISP = {"email": "unsubscribe", "sms": "stop"}

NF = len(FEATURE_COLS)                                # base width; the per-channel disposition block starts at index NF
# flat layout of the disposition block (variable width per channel): DISP_AT[(channel, disp)] -> offset within the block.
# Built by iterating the CANONICAL CHANNELS (the disposition-bearing channels), NOT a hardcoded arm list — so the state
# layout is decoupled from any action catalog. (email 4 + sms 3 + push 3 + voice 3 = 13 disposition cols.)
DISP_COLS = []; DISP_AT = {}; CH_DISPS = {}
for _ch in CHANNELS:
    CH_DISPS[_ch] = DISPOSITIONS_BY_CHANNEL[_ch]
    for _d in CH_DISPS[_ch]:
        DISP_AT[(_ch, _d)] = len(DISP_COLS); DISP_COLS.append(f"{_d}_{_ch}")
ALL_FEAT = FEATURE_COLS + DISP_COLS
NDISP_TOTAL = len(DISP_COLS)
WARM_COLS = DISP_COLS; WARM0 = NF                     # back-compat aliases (downstream code referencing WARM_COLS/WARM0)
DCOL = {(ch, d): NF + off for (ch, d), off in DISP_AT.items()}   # absolute column index in the full feature vector
CH_DISP_COLS = {ch: [NF + DISP_AT[(ch, d)] for d in CH_DISPS[ch]] for ch in CHANNELS}   # all of a channel's disp cols
OPTOUT_COL = {ch: NF + DISP_AT[(ch, d)] for ch, d in OPTOUT_DISP.items()}   # the opt-out disp's column, per gated channel

# milestone ladder (real progression; mirrors the live action-library milestone defs). value = reward gained on first
# reach, increasing toward STARS (the goal). reward = milestone value gained between sends - send cost.
MILESTONES = [
    {"id": "reached",    "name": "Reached",         "value": 1.0,  "pred": lambda f: f["respondedToOutreach"] >= 1},
    {"id": "registered", "name": "Registered",      "value": 2.0,  "pred": lambda f: f["registeredForPortal"] >= 1},
    {"id": "assessed",   "name": "Assessed",        "value": 4.0,  "pred": lambda f: f["hraCompleted"] >= 1},
    {"id": "engaged",    "name": "Engaged in Care", "value": 8.0,  "pred": lambda f: f["pcpSelected"] >= 1 and f["careTeamEngaged"] >= 1},
    {"id": "stars",      "name": "STARS Compliant", "value": 20.0, "pred": lambda f: f["awvCompleted"] >= 1 and f["medAdherent"] >= 1 and f["mammogramDone"] >= 1},
]
MILESTONE_IDS = [m["id"] for m in MILESTONES]

SEND_COST = 0.15; GAMMA = 0.95; MAX_STEPS = 18    # horizon for the soft-complete journey (MAX_STEPS=30 destabilized
# training at gamma 0.99 — Q-values exploded). Serving has no episode concept, so step = contact progress (totalThisWeek/MAX_STEPS).
# MILESTONE-BASED economics: the only positive VALUE is milestone progression — the reconstruction + reward use the
# milestone ladder above. SEND_COST is the one real operational $ cost (kept small). All knobs remain tunable.

STATE_COLS = ALL_FEAT + [f"ms_{m}" for m in MILESTONE_IDS] + ["step_frac"]
STATE_DIM = len(STATE_COLS)

# FCOL indexes base features AND disposition features; MS_VALUES = the milestone reward ladder used by the reconstruction.
FCOL = {c: i for i, c in enumerate(ALL_FEAT)}
MS_VALUES = np.array([m["value"] for m in MILESTONES], np.float64)


def _milestones_batch(F):                                              # (N,>=NF) -> (N,5) bool: which milestones hold
    g = lambda k: F[:, FCOL[k]] >= 1                                  # HEALTHCARE milestones (must match MILESTONES order)
    return np.stack([g("respondedToOutreach"), g("registeredForPortal"), g("hraCompleted"),
                     g("pcpSelected") & g("careTeamEngaged"),
                     g("awvCompleted") & g("medAdherent") & g("mammogramDone")], 1)


def _obs_batch(F, done, t=None):                                     # [base+disp | milestones | step]; step = contact
    n = len(F)                                                       # progress (totalThisWeek/MAX_STEPS, clipped) — the
    step = (np.clip(F[:, FCOL["totalThisWeek"]], 0, MAX_STEPS) / MAX_STEPS).reshape(n, 1)   # SAME definition the scorer
    return np.concatenate([F.astype(np.float32), done.astype(np.float32), step.astype(np.float32)], 1)  # + reconstruction use
