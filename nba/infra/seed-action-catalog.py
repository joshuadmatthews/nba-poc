#!/usr/bin/env python
"""Seed a clean ~18-intent NBA catalog THE REAL WAY — through the action-library REST API (the Command Center's backing
store). Each action is a CHANNEL-AGNOSTIC INTENT with a channels[] array, so the rules-engine emits one candidate per
(action x channel) and the MODEL picks the right channel AND best action per member. Lands in Postgres -> outboxes to
nba.definitions -> silver dim_definitions -> gold, where the ML sources it (NOT a hardcoded registry).

Also:
  * deletes the previous single-channel actions (action_<group>_<channel>) + demos/legacy/test,
  * seeds the activation layer's per-action SIM RESONANCE (nba:sim:fstar) — the hidden 'real market' appeal the
    simulator uses and the ML must DISCOVER from real history (channels further differ via the per-channel funnel).

Intents are grouped by the MILESTONE they advance. inclusion = prior-stage eligibility; completion = the milestone
predicate (HARD_COMPLETE on reach). "Call" intents are voice-only (high-touch); most others span email/sms/push(/voice).

  python nba/infra/seed-action-catalog.py            # seed + clean
  python nba/infra/seed-action-catalog.py --dry-run  # print, don't write
"""
import json, subprocess, sys

ALIB = "ais-nba-action-library"; LIB_URL = "http://localhost:7001"
REDIS = "ais-nba-redis"
DRY = "--dry-run" in sys.argv
ALL = ["email", "sms", "push", "voice"]; NOVOICE = ["email", "sms", "push"]

F = "operator.activity."
def tree(op, *c): return {"op": op, "conditions": list(c)}
def ge(fact, v): return {"cmp": "gte", "fact": F + fact, "value": v}
DONE = {  # completion = the milestone predicate (mirrors nba_journey_env.MILESTONES)
    "activated": tree("any", ge("viewedDashboard", 1), ge("usedChat", 1)),
    "onboarded": tree("all", ge("completedTasks", 3)),
    "engaged":   tree("all", ge("completedTasks", 6), ge("usedChat", 1)),
    "upgraded":  tree("all", ge("completedTasks", 9), ge("usedChat", 1), ge("viewedDashboard", 1)),
}
ELIG = {  # eligibility = member is in the PRIOR stage (so intents stage the journey)
    "activated": tree("all", ge("daysSinceLogin", 3)),
    "onboarded": tree("all", ge("completedTasks", 1)),
    "engaged":   tree("all", ge("completedTasks", 3)),
    "upgraded":  tree("all", ge("completedTasks", 6)),
}
TTL = 1800

# (id, name, group, channels, fstar) — fstar = the SIM's hidden per-action appeal (b + per-feature weights); base
# sigmoid(b) spans ~0.1..0.7 so the catalog has stars AND duds. Channels further differ via the per-channel funnel.
A = [
    # REACH (engagement stage 'activated'): get the health-plan member to first engage
    ("action_plan_welcome",   "Plan Welcome",          "activated", ALL,     {"b": 0.5,  "daysSinceLogin": 0.010, "usedChat": -0.4}),
    ("action_reengage",       "Re-engage Lapsed Member","activated", NOVOICE, {"b": 0.4,  "daysSinceLogin": 0.020, "completedTasks": -0.08}),
    ("action_benefits_intro", "Benefits Intro",        "activated", ALL,     {"b": 0.1,  "daysSinceLogin": 0.008, "viewedDashboard": 0.3}),
    ("action_getting_started_hc","Getting Started",    "activated", NOVOICE, {"b": -0.3, "daysSinceLogin": 0.006, "completedTasks": 0.04}),
    ("action_welcome_call",   "Welcome Call",          "activated", ["voice"],{"b": -1.6, "daysSinceLogin": 0.004, "completedTasks": 0.05}),
    # ASSESS ('onboarded'): complete the health risk assessment / profile
    ("action_hra",            "Health Risk Assessment","onboarded", ALL,     {"b": -0.2, "completedTasks": 0.08, "viewedDashboard": 0.2}),
    ("action_hra_reminder",   "Assessment Reminder",   "onboarded", NOVOICE, {"b": -0.1, "completedTasks": 0.10, "usedChat": 0.2}),
    ("action_complete_profile","Complete Your Profile","onboarded", ALL,     {"b": -0.5, "completedTasks": 0.06, "viewedDashboard": 0.3}),
    ("action_nurse_onboarding","Nurse Onboarding Call","onboarded", ["voice"],{"b": -2.0, "completedTasks": 0.15, "usedChat": 0.6}),
    # ENGAGE ('engaged'): engage in care (education, care-team, screenings)
    ("action_wellness_education","Wellness Education", "engaged",   ALL,     {"b": -0.6, "completedTasks": 0.10, "usedChat": 0.4}),
    ("action_care_gap_alert", "Care Gap Alert",        "engaged",   NOVOICE, {"b": -0.3, "viewedDashboard": 0.6, "completedTasks": 0.12, "usedChat": 0.3}),
    ("action_screening_reminder","Preventive Screening Reminder","engaged", ALL, {"b": -0.5, "completedTasks": 0.10, "usedChat": 0.3}),
    ("action_care_manager_call","Care Manager Call",   "engaged",   ["voice"],{"b": -2.2, "usedChat": 1.3, "viewedDashboard": 1.0, "completedTasks": 0.18}),
    ("action_health_survey",  "Health Survey",         "engaged",   NOVOICE, {"b": -1.8, "usedChat": 0.3, "completedTasks": 0.02}),   # dud-ish: weak driver
    # GOAL ('upgraded'): close STARS gaps / adherence
    ("action_annual_wellness_visit","Annual Wellness Visit","upgraded", ALL, {"b": -0.9, "completedTasks": 0.12, "usedChat": 0.5, "viewedDashboard": 0.4}),
    ("action_med_adherence",  "Medication Adherence",  "upgraded",  NOVOICE, {"b": -1.1, "completedTasks": 0.10, "viewedDashboard": 0.5}),
    ("action_screening_completion","Screening Completion","upgraded", ["email","push"], {"b": -1.5, "completedTasks": 0.06, "usedChat": 0.3}),  # dud-ish
    ("action_goal_call",      "Care Goal Call",        "upgraded",  ["voice"],{"b": -2.0, "usedChat": 1.1, "viewedDashboard": 0.9, "completedTasks": 0.2}),
]
# the generic (non-healthcare) intents from the prior seed, to remove so the catalog is exactly the 18 healthcare ones
OLD = ["action_welcome","action_winback","action_intro_offer","action_getting_started","action_activation_call",
       "action_setup_checklist","action_task_nudge","action_quick_start","action_onboarding_call","action_power_tips",
       "action_feature_spotlight","action_best_practices","action_checkin_call","action_survey","action_upgrade_offer",
       "action_loyalty_perk","action_success_story","action_upgrade_call"]


def lib(method, path, body=None):
    cmd = ["podman", "exec", "-i", ALIB, "curl", "-s", "-X", method, "-H", "Content-Type: application/json", f"{LIB_URL}{path}"]
    if body is not None:
        cmd += ["-d", "@-"]
        return subprocess.run(cmd, input=json.dumps(body), capture_output=True, text=True).stdout
    return subprocess.run(cmd, capture_output=True, text=True).stdout


def main():
    print(f"seeding {len(A)} multi-channel intents via {ALIB}  (dry-run={DRY})\n")
    sim_fstar = {}
    for aid, name, grp, chans, fstar in A:
        doc = {"name": name, "channels": [{"channel": ch, "contentKey": f"tmpl.{aid}.{ch}.v1"} for ch in chans],
               "inclusion": ELIG[grp], "completion": DONE[grp], "autoExcludeOnCompletion": True,
               "groupId": grp, "ttlSeconds": TTL}
        sim_fstar[aid] = fstar
        if DRY:
            print(f"PUT {aid:26s} {grp:10s} [{','.join(chans)}]  {name}"); continue
        out = lib("PUT", f"/actions/{aid}", doc); ok = '"id"' in out or aid in out
        print(f"  {'OK ' if ok else 'ERR'} {aid:26s} {grp:10s} [{','.join(chans)}]  {name}" + ("" if ok else f"  -> {out[:100]}"))
    for j in OLD:
        if DRY: print(f"DELETE {j}"); continue
        lib("DELETE", f"/actions/{j}")
    # nba:sim:effect = the engagement PROGRESS the activation layer applies on a positive response, per action's group
    # (so members advance through the milestone ladder). Drives sensible journeys: activate->onboard->engage->upgrade.
    EFFECT_BY_GROUP = {
        "activated": {"viewedDashboard": 1.0, "completedTasks": 0.3},   # first touch of the product
        "onboarded": {"completedTasks": 1.0},                            # task progress toward 3
        "engaged":   {"completedTasks": 1.0, "usedChat": 1.0},           # toward 6 + chat
        "upgraded":  {"completedTasks": 1.5, "viewedDashboard": 1.0, "usedChat": 1.0},  # toward 9 + all flags
    }
    sim_effect = {aid: EFFECT_BY_GROUP[grp] for aid, name, grp, chans, fstar in A}
    if not DRY:
        print(f"\ndeleted {len(OLD)} old single-channel/demo actions")
        subprocess.run(["podman", "exec", "-i", REDIS, "redis-cli", "-x", "set", "nba:sim:fstar"],
                       input=json.dumps(sim_fstar), capture_output=True, text=True)
        subprocess.run(["podman", "exec", "-i", REDIS, "redis-cli", "-x", "set", "nba:sim:effect"],
                       input=json.dumps(sim_effect), capture_output=True, text=True)
        print(f"seeded nba:sim:fstar + nba:sim:effect with {len(sim_fstar)} actions")
    arms = sum(len(chans) for *_ , chans, _ in [(a[0],a[1],a[2],a[3],a[4]) for a in A])
    print(f"\n{len(A)} intents -> {arms} (action,channel) arms. flows: action-library -> nba.definitions -> dim_definitions -> ML")


if __name__ == "__main__":
    main()
