#!/usr/bin/env python3
"""NBA conversion sim — Databricks-free OUTBOUND hard-completion simulator.

The local analog of the Databricks source sim's conversions (nba_source_sim.py: "dispatches -> N completions").
Watches DELIVERED actions on nba.member.facts (nba.actionstate.{actionId}.{channel} reaching PRESENTED/
IN_PROCESS/SOFT_COMPLETED) and, for a deterministic fraction of (member, action) pairs, finishes the GOAL via
the REAL API (POST /completion -> nba.completion.{aid}). So outbound members reach HARD_COMPLETED instead of
only EXPIRING — closing the local end-to-end loop with Databricks parked.

The convert/no-convert decision is a stable hash of (member, action), so each pair decides once and is
reproducible; the rest expire (the negative RL label). Together with the journey scorer this exercises the
whole funnel locally: eligible -> score -> route -> dispatch -> convert|expire -> recirculate. A test/dev sim.
"""
import hashlib
import json
import os
import urllib.request

from kafka import KafkaConsumer

BOOT = os.environ.get("NBA_BOOTSTRAP", "nba-redpanda:9092")
MEMBER_FACTS = os.environ.get("NBA_MEMBER_FACTS", "nba.member.facts")
GROUP = os.environ.get("NBA_GROUP", "nba-conversion-sim")
AL = os.environ.get("NBA_ACTION_LIBRARY", "http://nba-action-library:7001")
HARD_FRACTION = float(os.environ.get("HARD_FRACTION", "0.4"))   # share of delivered (member,action) that convert
DELIVERED = {"PRESENTED", "IN_PROCESS", "SOFT_COMPLETED"}        # the action reached/engaged the member


def converts(entity, aid):
    """Stable per-(member,action) coin flip — reproducible; the rest expire (the negative label)."""
    h = int(hashlib.md5(f"{entity}|{aid}".encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
    return h < HARD_FRACTION


def header(msg, key):
    for k, v in (msg.headers or []):
        if k == key:
            return v.decode() if v else None
    return None


def main():
    consumer = KafkaConsumer(
        MEMBER_FACTS, bootstrap_servers=BOOT, group_id=GROUP,
        auto_offset_reset=os.environ.get("NBA_OFFSET_RESET", "latest"),
        enable_auto_commit=True, value_deserializer=lambda b: b)
    decided = set()   # (member, action) decided once — POC dedup (TTL/evict at scale)
    print(f"[conversion-sim] up: {MEMBER_FACTS} -> POST {AL}/completion (HARD_FRACTION={HARD_FRACTION}) — no Databricks", flush=True)

    for msg in consumer:
        if header(msg, "kind") != "state":
            continue
        try:
            f = json.loads(msg.value)
        except Exception:
            continue
        key = f.get("key", "")
        if not key.startswith("nba.actionstate.") or str(f.get("value")) not in DELIVERED:
            continue
        ent = f.get("entityId")
        aid = key[len("nba.actionstate."):].rsplit(".", 1)[0]   # key = nba.actionstate.{actionId}.{channel}
        if not ent or not aid:
            continue
        k = (ent, aid)
        if k in decided:
            continue
        decided.add(k)
        if not converts(ent, aid):
            continue                                            # this pair doesn't convert -> let it EXPIRE
        body = json.dumps({"entityId": ent, "actionId": aid, "source": "conversion-sim"}).encode()
        try:
            req = urllib.request.Request(f"{AL}/completion", data=body,
                                         headers={"Content-Type": "application/json"}, method="POST")
            urllib.request.urlopen(req, timeout=5).read()
            print(f"[conversion-sim] HARD_COMPLETE {ent} {aid}", flush=True)
        except Exception as e:
            print(f"[conversion-sim] completion POST failed {ent} {aid}: {e}", flush=True)


if __name__ == "__main__":
    main()
