# Databricks notebook source
"""THE catalog source for the ML — reads the LIVE action catalog from silver dim_definitions (fed by the action-library
-> nba.definitions pipeline), NOT a hardcoded list. Actions are multi-channel intents; this EXPANDS each into one arm
per (actionId, channel), so the policy scores + learns each (action, channel) individually and picks the best combo per
member. This retires the phantom DEMO_ARMS + channel-keying.

  arms = load_catalog(spark)   ->  [{"actionId","channel","name","group"}, ...]   (one per action x channel)

group = the milestone an intent targets, inferred from its INCLUSION stage-gate (logicJson/completion is not always
populated in silver; inclusion always is). Test/dev defs (no channel, vtest content, Compl* names) are filtered out.
NOTE: dim_definitions is upsert-only on the definitions stream (delete-tombstones aren't applied), so a deleted action
can linger — keep it clean by deleting stale rows (see nba_ml_catalog_clean) until the stream handles tombstones.
"""
import json

# inclusion stage-gate -> milestone group (mirrors seed-action-catalog ELIG): activated gates on daysSinceLogin only;
# onboarded/engaged/upgraded gate on completedTasks >= 1/3/6.
def _group_from_inclusion(incl):
    try:
        conds = json.loads(incl).get("conditions", []) if incl else []
    except Exception:
        conds = []
    ct = None
    for c in conds:
        if isinstance(c, dict) and c.get("fact", "").endswith("completedTasks"):
            try: ct = float(c.get("value"))
            except Exception: pass
    if ct is None:
        return "activated"          # gated on daysSinceLogin only
    if ct >= 6: return "upgraded"
    if ct >= 3: return "engaged"
    return "onboarded"              # ct >= 1


def _is_junk(name, contentkeys):
    n = (name or "").lower()
    if n in ("varianttest", "complcrit", "complnoexcl"):
        return True
    return any("vtest" in (ck or "") for ck in contentkeys)


# the DRIVEN (completion) fact = the fact the action USES that is NOT one of its INCLUSION prereqs. Returned as the
# short FEATURE_COL name (operator.activity.hraCompleted -> hraCompleted), so the offline-RL can flip it on a convert.
def _driven_fact(facts_used_json, inclusion_json):
    try: used = json.loads(facts_used_json) if facts_used_json else []
    except Exception: used = []
    incl = set()
    try:
        for c in (json.loads(inclusion_json).get("conditions", []) if inclusion_json else []):
            if isinstance(c, dict) and c.get("fact"): incl.add(c["fact"])
    except Exception: pass
    driven = [f for f in used if f not in incl]
    return driven[0].split(".")[-1] if driven else None


def load_catalog(spark, src_ns="workspace.nba_poc"):
    rows = (spark.table(f"{src_ns}.dim_definitions")
            .where("defType = 'ACTION' AND id IS NOT NULL").collect())
    arms = []
    for r in rows:
        chans = []
        try:
            chans = json.loads(r["channelsJson"]) if r["channelsJson"] else []
        except Exception:
            chans = []
        if not chans or _is_junk(r["name"], [c.get("contentKey") for c in chans]):
            continue
        grp = _group_from_inclusion(r["inclusionJson"])
        drives = _driven_fact(r["factsUsedJson"], r["inclusionJson"])   # the milestone fact a convert flips (for offline-RL)
        for c in chans:
            ch = c.get("channel")
            if ch:
                arms.append({"actionId": r["id"], "channel": ch, "name": r["name"], "group": grp, "drives": drives})
    arms.sort(key=lambda a: (a["actionId"], a["channel"]))   # stable arm order (arm index = position)
    return arms


def arm_index_map(arms):
    """[{actionId,channel},...] -> {(actionId, channel): arm_index}. NOOP = len(arms). Used by the reconstruction +
    scorer to key each real (action, channel) send to its own arm (computed at runtime, no module-load import-timing)."""
    return {(a["actionId"], a["channel"]): i for i, a in enumerate(arms)}
