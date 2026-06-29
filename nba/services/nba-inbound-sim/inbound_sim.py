#!/usr/bin/env python3
# LOCAL inbound member simulator — the counterpart to the Databricks source-sim (which models OUTBOUND response).
# A real inbound member is an external CLIENT, so we model it as one: warm members proactively come inbound and
# complete an action through the REAL inbound APIs (serve -> disposition -> completion), never a shortcut fact on
# the bus. Their completions flow the same proven path every other inbound disposition does (outbox -> member.facts
# -> local snapshot-builder + the Databricks medallion), so the lake carries "soft-complete -> inbound completion"
# and the CQL learns to surface that action when the member comes inbound.
#
#   warm signal :  nba:snapshot:{nbaId} hash field  fact:nba.actionstate.{aid}.{ch} == SOFT_COMPLETED
#   dedup       :  skip an action already hard-completed  (fact:nba.completion.{aid} present)
#   per warm (member, aid, ch), with prob INBOUND_RATE this loop:
#     GET  /next-action/{entity}?channel={ch}              -> correlationId   (emits INBOUND_SERVE)
#     POST /disposition {entity,aid,ch,status:<deepest funnel>,correlationId} (emits INBOUND_DISPOSITION + writeback)
#     POST /completion  {entity,aid,source:inbound}                            (writes nba.completion.{aid} — the goal,
#                                                                               the SAME fact the router emits outbound)
import os, time, json, random, urllib.request, urllib.error
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import redis

API   = os.environ.get("NBA_API_BASE", "http://nba-action-library:7001")
RHOST = os.environ.get("NBA_REDIS_HOST", "nba-redis")
RPORT = int(os.environ.get("NBA_REDIS_PORT", "6379"))
RATE  = float(os.environ.get("INBOUND_RATE", "0.25"))      # P(a warm member proactively completes inbound this loop)
LOOP  = int(os.environ.get("LOOP_SECONDS", "45"))
CAP   = int(os.environ.get("INBOUND_CAP", "300"))          # max completions driven per loop (protect the API)
SCAN  = int(os.environ.get("SCAN_LIMIT", "8000"))          # snapshots scanned per loop
# disposition scoring: 'local' (in-network nba-model) is fast + free, so the sim stays robust under load. The inbound
# RANKING signal comes from the serve's CACHED scores (already the dbx @champion via the flywheel), so 'local' loses
# nothing the sim measures. Set NBA_SCORER=dbx to stress-test the live dbx hot path instead (slower; cold-start risk).
SCORER = os.environ.get("NBA_SCORER", "local")
HARD_FRACTION = float(os.environ.get("HARD_FRACTION", "0.4"))   # of inbound visits, what share finish the GOAL (hard completion).
                                                               # The rest just engage (a soft disposition) and may finish later.
COLD_RATE = float(os.environ.get("COLD_RATE", "0.015"))         # P(a NON-warm member spontaneously shows up inbound this loop):
                                                               # a baseline so warmth ELEVATES inbound (a lift) rather than gating it.
COLD_CAP  = int(os.environ.get("COLD_CAP", "200"))
# deepest engagement status per channel (CHANNEL_FUNNEL in ActionLibrary.java); unknown -> the inbound funnel terminal.
TERMINAL = {"email": "LinkClicked", "sms": "LinkClicked", "push": "Opened", "voice": "Completed", "mail": "Delivered"}
# the inbound "call topic" / live context presented on the serve so getActions returns a HOT-PATH decision (not just the cached
# list): the API merges these facts, re-runs eligibility + scores with the just-given context. daysSinceLogin=0 = the member is
# contacting us right now — a real model feature, so the decision reflects the live contact. Extend with any topic-relevant facts.
INBOUND_CTX = {"operator.activity.daysSinceLogin": {"value": 0, "valueType": "LONG"}}
TOPIC_RATE = float(os.environ.get("TOPIC_RATE", "0.3"))    # fraction of inbound visits that carry a topic (facts) -> HOT PATH
                                                           # (reads gold live); the rest serve from the cached eligibility.

r = redis.Redis(host=RHOST, port=RPORT, decode_responses=True)


def _req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=12) as resp:
        return json.load(resp)


def interact_inbound(entity, aid, ch):
    cold = aid is None                                                     # cold = a member with no warm signal who just shows up
    try:
        q = f"?channel={ch}&n=5" if ch else "?n=5"                         # warm: their channel; cold: best NBAs across channels
        body = {"facts": INBOUND_CTX, "scorer": SCORER, "mode": "kie"} if random.random() < TOPIC_RATE else None
        served = _req("GET", f"/next-action/{entity}{q}", body)            # some calls carry a topic (-> HOT PATH, reads gold);
        actions = served.get("actions") or []                             # the rest serve from the cached eligibility
        if cold:                                                           # cold members act on the model's top recommendation
            if not actions:
                return {"ok": True, "cold": True, "acted": False}
            aid, ch = actions[0].get("actionId"), actions[0].get("channel")
        corr = served.get("correlationId", "")
        # the member ACTS on the served list -> a disposition (the soft engagement), hot-pathed + linked to the serve.
        _req("POST", "/disposition", {"entityId": entity, "actionId": aid, "channel": ch,
                                      "status": TERMINAL.get(ch, "Completed"), "correlationId": corr, "scorer": SCORER})
        hard = random.random() < HARD_FRACTION                             # only SOME finish the goal this visit (hard completion);
        if hard:                                                           # the rest stay eligible + may finish on a later visit
            _req("POST", "/completion", {"entityId": entity, "actionId": aid, "source": "inbound"})
        top = bool(actions) and actions[0].get("actionId") == aid          # (warm) did the model rank the warm action #1?
        return {"ok": True, "cold": cold, "hard": hard, "top": top, "acted": True, "hot": served.get("hotpath", False)}
    except urllib.error.HTTPError as e:
        return {"ok": False, "err": f"http{e.code}"}
    except Exception as e:
        return {"ok": False, "err": type(e).__name__}


def loop_once():
    warm = []                                                              # [(entity, aid, ch)] specific soft-completed actions
    cold = []                                                              # [entity] spontaneous inbound (no warm signal)
    scanned = 0
    for key in r.scan_iter(match="nba:snapshot:*", count=500):
        if scanned >= SCAN:
            break
        scanned += 1
        h = r.hgetall(key)
        entity = h.get("__entityId")
        if not entity:
            continue
        done = {k for k in h if k.startswith("fact:nba.completion.")}       # actions already hard-completed -> skip
        has_soft = False
        for f, v in h.items():
            if not f.startswith("fact:nba.actionstate."):
                continue
            try:
                state = json.loads(v).get("value")                        # snapshot values are JSON {"value":..,"valueType":..}
            except Exception:
                state = v
            if state != "SOFT_COMPLETED":
                continue
            has_soft = True
            rest = f[len("fact:nba.actionstate."):]                        # "{aid}.{ch}"
            if "." not in rest:
                continue
            aid, ch = rest.rsplit(".", 1)
            if f"fact:nba.completion.{aid}" in done:
                continue
            if random.random() < RATE:
                warm.append((entity, aid, ch))
        if not has_soft and random.random() < COLD_RATE:                   # a NON-warm member spontaneously shows up inbound
            cold.append(entity)
    random.shuffle(warm); warm = warm[:CAP]
    random.shuffle(cold); cold = cold[:COLD_CAP]
    jobs = [(e, a, c) for (e, a, c) in warm] + [(e, None, None) for e in cold]
    if not jobs:
        print(f"[inbound-sim] no inbound visitors this loop ({scanned} snapshots scanned)", flush=True)
        return
    with ThreadPoolExecutor(max_workers=12) as ex:
        res = list(ex.map(lambda t: interact_inbound(*t), jobs))
    ok = [r for r in res if r["ok"]]
    w = [r for r in ok if not r.get("cold")]
    c = [r for r in ok if r.get("cold") and r.get("acted")]
    w_hard = sum(1 for r in w if r.get("hard")); w_soft = len(w) - w_hard
    c_hard = sum(1 for r in c if r.get("hard")); c_soft = len(c) - c_hard
    top = sum(1 for r in w if r.get("top"))
    hot = sum(1 for r in ok if r.get("hot"))
    errs = Counter(r["err"] for r in res if not r["ok"])
    print(f"[inbound-sim] inbound visits via real APIs (serve[hot-path]->disposition[->completion]) | "
          f"WARM {len(w)} (soft={w_soft} hard={w_hard}, model ranked warm action #1: {top}/{len(w)}) | "
          f"COLD {len(c)} (soft={c_soft} hard={c_hard}) | hot-path serves: {hot}/{len(ok)}"
          + (f" | errs={dict(errs)}" if errs else ""), flush=True)


print(f"[inbound-sim] up: API={API} redis={RHOST}:{RPORT} rate={RATE} loop={LOOP}s cap={CAP}", flush=True)
while True:
    try:
        loop_once()
    except Exception as e:
        print(f"[inbound-sim] loop error: {type(e).__name__}: {e}", flush=True)
    time.sleep(LOOP)
