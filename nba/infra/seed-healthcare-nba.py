#!/usr/bin/env python
"""Seed the HEALTHCARE NBA fact model THE REAL WAY (action-library REST API). Defines:
  * 15 multi-channel actions, each DRIVING one member fact, with FACT-BASED eligibility (prerequisite facts) +
    HARD completion on the driven fact (autoExcludeOnCompletion retires it once done),
  * 5 sensible fact-based MILESTONES (Reached -> Registered -> Assessed -> Engaged -> STARS),
  * nba:sim:effect so the activation layer flips the driven fact on a positive response.

All facts live under operator.activity.* so the activation layer's default key-mapping (fullKeyOf) matches the model
feature keys (nba_ml_common.FEATURE_KEYS). Members start with all facts 0; actions move them up the ladder. See
nba/docs/12-healthcare-nba-redesign.md.

  python nba/infra/seed-healthcare-nba.py [--dry-run]
"""
import json, subprocess, sys

ALIB = "ais-nba-action-library"; LIB_URL = "http://localhost:7001"; REDIS = "ais-nba-redis"
DRY = "--dry-run" in sys.argv
ALL = ["email", "sms", "push", "voice", "mail"]; NOVOICE = ["email", "sms", "push", "mail"]   # mail = the low-digital/SDOH/older archetype channel (model-gated by propensity; care-manager stays voice-only)
P = "operator.activity."          # all action-driven facts live here (matches activation-layer default key map)
PROFILE = {"diabetic", "riskScore"}   # PROFILE facts: set by the MEMBER seed (operator.profile.*), NOT action-driven.
def _factkey(fact): return ("operator.profile." if fact in PROFILE else P) + fact  # eligibility must read the REAL key

# CANONICAL FACT-TYPE CATALOG (full key -> type). Seeded to Redis nba:facttype: the action-library API validates
# every rule operator against it, and the rule-builder UI constrains operator options by it. BOOL facts compare
# with eq/ne (is true / is false); numerics with gte/lt/etc. Keeps the model typed end-to-end (no boolean-gte hack).
FACT_TYPES = {
    # journey driven facts (boolean milestones the actions move)
    "operator.activity.respondedToOutreach": "BOOL", "operator.activity.registeredForPortal": "BOOL",
    "operator.activity.loggedIn": "BOOL", "operator.activity.viewedBenefits": "BOOL",
    "operator.activity.hraCompleted": "BOOL", "operator.activity.pcpSelected": "BOOL",
    "operator.activity.careTeamEngaged": "BOOL", "operator.activity.awvCompleted": "BOOL",
    "operator.activity.medAdherent": "BOOL", "operator.activity.mammogramDone": "BOOL",
    "operator.activity.a1cControlled": "BOOL", "operator.activity.colonoscopyDone": "BOOL",
    # profile (set by the member seed)
    "operator.profile.riskScore": "DOUBLE", "operator.profile.diabetic": "BOOL", "operator.profile.age": "LONG",
    "operator.profile.planDSNP": "BOOL", "operator.profile.tenureMonths": "LONG", "operator.profile.sdohBarrier": "BOOL",
    "operator.profile.smsConsent": "BOOL", "operator.profile.isDNC": "BOOL",
    # clinical
    "operator.clinical.comorbidityCount": "LONG", "operator.clinical.erVisits12mo": "LONG",
    "operator.clinical.rxAdherencePDC": "DOUBLE", "operator.clinical.openCareGaps": "LONG",
    # activity telemetry (numeric)
    "operator.activity.portalLogins30d": "LONG", "operator.activity.pagesViewed30d": "LONG",
    "operator.activity.avgTimeOnPageSec": "LONG", "operator.activity.benefitsPageViews": "LONG",
    "operator.activity.daysSinceLogin": "LONG",
    # comms
    "operator.comms.totalThisWeek": "LONG", "operator.comms.emailsThisWeek": "LONG",
}

def met(fact, v=1):   # "fact is satisfied" — TYPED: boolean -> eq true; numeric -> gte v
    key = _factkey(fact)
    return ({"cmp": "eq", "fact": key, "value": True} if FACT_TYPES.get(key) == "BOOL"
            else {"cmp": "gte", "fact": key, "value": v})
def incl(*facts): return {"op": "all", "conditions": [met(f) for f in facts]} if facts else {"op": "all", "conditions": []}
def done(fact): return {"op": "all", "conditions": [met(fact)]}

# (id, name, channels, drives_fact, [prereq_facts], fstar_appeal)
A = [
    # REACH
    ("action_plan_welcome",     "Plan Welcome",            ALL,     "respondedToOutreach", [], {"b": 0.6, "daysSinceLogin": 0.010}),
    ("action_reengage",         "Re-engage Lapsed Member", NOVOICE, "respondedToOutreach", [], {"b": 0.3, "daysSinceLogin": 0.020}),
    # DIGITAL
    ("action_portal_registration","Portal Registration",   ALL,     "registeredForPortal", ["respondedToOutreach"], {"b": 0.1}),
    ("action_login_reminder",   "Portal Login Reminder",   NOVOICE, "loggedIn",            ["registeredForPortal"], {"b": -0.2}),
    ("action_benefits_education","Benefits Education",      NOVOICE, "viewedBenefits",      ["registeredForPortal"], {"b": -0.3}),
    # ASSESS
    ("action_hra",              "Health Risk Assessment",  ALL,     "hraCompleted",        ["respondedToOutreach"], {"b": -0.2}),
    ("action_hra_reminder",     "Assessment Reminder",     NOVOICE, "hraCompleted",        ["respondedToOutreach"], {"b": -0.4}),
    # ENGAGE
    ("action_pcp_selection",    "PCP Selection",           ALL,     "pcpSelected",         ["hraCompleted"], {"b": -0.4}),
    ("action_care_manager_outreach","Care Manager Outreach",["voice"],"careTeamEngaged",   ["pcpSelected"], {"b": -1.6, "riskScore": 0.4}),
    ("action_wellness_education","Wellness Education",      NOVOICE, "careTeamEngaged",     ["pcpSelected"], {"b": -1.2}),   # weak driver
    # GAP CLOSURE (STARS)
    ("action_annual_wellness_visit","Annual Wellness Visit",ALL,    "awvCompleted",        ["pcpSelected"], {"b": -0.6}),
    ("action_med_adherence",    "Medication Adherence",    NOVOICE, "medAdherent",         ["pcpSelected"], {"b": -0.8}),
    ("action_mammogram",        "Mammogram Reminder",      NOVOICE, "mammogramDone",       ["pcpSelected"], {"b": -0.9}),
    ("action_a1c_test",         "A1C Test Reminder",       NOVOICE, "a1cControlled",       ["pcpSelected", "diabetic"], {"b": -0.7}),
    ("action_colonoscopy",      "Colonoscopy Reminder",    NOVOICE, "colonoscopyDone",     ["pcpSelected"], {"b": -1.0}),
]
# milestones (id, name, logic) — sensible fact-based ladder
MILESTONES = [
    ("milestone_reached",    "Reached",        incl("respondedToOutreach")),
    ("milestone_registered", "Registered",     incl("registeredForPortal")),
    ("milestone_assessed",   "Assessed",       incl("hraCompleted")),
    ("milestone_engaged",    "Engaged in Care", incl("pcpSelected", "careTeamEngaged")),
    ("milestone_stars",      "STARS Compliant", incl("awvCompleted", "medAdherent", "mammogramDone")),
]
# generic intents from prior seeds to remove
OLD = ["action_plan_welcome","action_reengage","action_benefits_intro","action_getting_started_hc","action_welcome_call",
       "action_hra","action_hra_reminder","action_complete_profile","action_nurse_onboarding","action_wellness_education",
       "action_care_gap_alert","action_screening_reminder","action_care_manager_call","action_health_survey",
       "action_annual_wellness_visit","action_med_adherence","action_screening_completion","action_goal_call",
       "action_welcome","action_winback","action_intro_offer","action_getting_started","action_activation_call",
       "action_setup_checklist","action_task_nudge","action_quick_start","action_onboarding_call","action_power_tips",
       "action_feature_spotlight","action_best_practices","action_checkin_call","action_survey","action_upgrade_offer",
       "action_loyalty_perk","action_success_story","action_upgrade_call"]
OLD = [o for o in OLD if o not in [a[0] for a in A]]   # don't delete ids we're (re)seeding


def lib(method, path, body=None):
    cmd = ["podman", "exec", "-i", ALIB, "curl", "-s", "-X", method, "-H", "Content-Type: application/json", f"{LIB_URL}{path}"]
    if body is not None:
        cmd += ["-d", "@-"]; return subprocess.run(cmd, input=json.dumps(body), capture_output=True, text=True).stdout
    return subprocess.run(cmd, capture_output=True, text=True).stdout


def main():
    print(f"healthcare NBA: {len(A)} actions + {len(MILESTONES)} milestones via {ALIB}  (dry-run={DRY})\n")
    sim_effect = {}
    for aid, name, chans, drives, prereqs, fstar in A:
        doc = {"name": name, "channels": [{"channel": ch, "contentKey": f"tmpl.{aid}.{ch}.v1"} for ch in chans],
               "inclusion": incl(*prereqs), "completion": done(drives), "autoExcludeOnCompletion": True, "ttlSeconds": 600}
        sim_effect[aid] = {drives: 1.0}
        if DRY:
            print(f"PUT {aid:32s} drives={drives:20s} prereqs={prereqs}"); continue
        out = lib("PUT", f"/actions/{aid}", doc); ok = '"id"' in out or aid in out
        print(f"  {'OK ' if ok else 'ERR'} {aid:32s} drives={drives:20s} prereqs={prereqs}" + ("" if ok else f" -> {out[:90]}"))
    for mid, name, logic in MILESTONES:
        if DRY: print(f"PUT milestone {mid} ({name})"); continue
        out = lib("PUT", f"/milestones/{mid}", {"name": name, "logic": logic})
        print(f"  milestone {name}: {out.strip()[:80]}")
    if not DRY:
        for o in OLD: lib("DELETE", f"/actions/{o}")
        print(f"\ndeleted {len(OLD)} stale actions")
        # keep nba:sim:fstar appeal per action; add nba:sim:effect (drives the fact)
        fstar = {aid: f for aid, name, chans, drives, prereqs, f in A}
        subprocess.run(["podman","exec","-i",REDIS,"redis-cli","-x","set","nba:sim:fstar"], input=json.dumps(fstar), capture_output=True, text=True)
        subprocess.run(["podman","exec","-i",REDIS,"redis-cli","-x","set","nba:sim:effect"], input=json.dumps(sim_effect), capture_output=True, text=True)
        print(f"seeded nba:sim:fstar + nba:sim:effect ({len(A)} actions)")
        # canonical fact-type catalog -> Redis (action-library validates rule operators against it; UI constrains by it)
        subprocess.run(["podman","exec",REDIS,"redis-cli","del","nba:facttype"], capture_output=True, text=True)
        _ftargs = []
        for _k, _t in FACT_TYPES.items(): _ftargs += [_k, _t]
        subprocess.run(["podman","exec",REDIS,"redis-cli","hset","nba:facttype"] + _ftargs, capture_output=True, text=True)
        print(f"seeded nba:facttype ({len(FACT_TYPES)} facts)")
    print(f"\n{len(A)} actions -> {sum(len(a[2]) for a in A)} (action,channel) arms. flows: action-library -> definitions -> dim_definitions -> ML")


if __name__ == "__main__":
    main()
