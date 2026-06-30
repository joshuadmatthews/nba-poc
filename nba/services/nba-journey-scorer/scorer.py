#!/usr/bin/env python3
"""NBA journey scorer — a Databricks-FREE async scorer for end-to-end local testing of the whole engine.

Drop-in alternative to the Databricks RL scorer (nba_ml_score_rl.py): consumes nba.evaluations and emits
nba.score.{actionId}.{channel} facts onto nba.member.facts (kind=score), EXACTLY like the dbx job — but the
score is a DETERMINISTIC scripted-journey function of the member's state (no model, no Databricks, no tunnel).

It steps each member through their eligible actions in a varied, sensible order:
  - FRESH (un-attempted) actions score highest, spread per-(member,action,channel) so different members
    traverse their actions in different orders (a varied journey, not a fixed ranking);
  - IN-FLIGHT actions (CREATED/IN_PROCESS/PRESENTED/SUPPRESSING) sink, so the router moves on to the next;
  - SOFT-COMPLETED (engaged) and negative (DECLINED/FAILED/EXPIRED) deprioritize;
  - HARD-COMPLETED (goal) sink to the bottom — never re-picked.

So the router's argmax walks the member forward through their funnel. This lets the ENTIRE flywheel run AND
complete locally (facts -> snapshot -> eligibility -> SCORE -> router -> temporal -> dispositions -> recirculate)
with Databricks parked. It is a test/dev scorer — NOT the production RL policy.
"""
import hashlib
import json
import os
import time

# NOTE: `kafka` is imported lazily inside main() so the PURE scoring functions (stable01 / score_action) can be
# imported + unit-tested (test_scorer.py) without the kafka-python dependency installed.

BOOT = os.environ.get("NBA_BOOTSTRAP", "nba-redpanda:9092")
EVALS = os.environ.get("NBA_EVALUATIONS_TOPIC", "nba.evaluations")
MEMBER_FACTS = os.environ.get("NBA_MEMBER_FACTS", "nba.member.facts")
GROUP = os.environ.get("NBA_GROUP", "nba-journey-scorer")
OFFSET = os.environ.get("NBA_OFFSET_RESET", "latest")

IN_FLIGHT = {"CREATED", "IN_PROCESS", "PRESENTED", "SUPPRESSING"}   # occupying the slot -> don't re-pick
NEGATIVE = {"DECLINED", "FAILED", "EXPIRED"}


def stable01(*parts):
    """Deterministic [0,1) from the inputs — gives each (member,action,channel) a stable, varied offset."""
    h = hashlib.md5("|".join(parts).encode()).hexdigest()
    return int(h[:8], 16) / 0xFFFFFFFF


def score_action(nba_id, ca):
    a, ch = ca.get("actionId", ""), ca.get("channel", "")
    base = stable01(nba_id, a, ch)
    st = ca.get("workflowState")
    if ca.get("hardCompleted"):                  return -100.0 + base          # goal reached -> never re-pick
    if ca.get("active") or st in IN_FLIGHT:      return -50.0 + base           # in flight -> move on
    if ca.get("softCompleted"):                  return -10.0 + base * 5.0     # engaged -> deprioritize
    if st in NEGATIVE:                           return -20.0 + base * 3.0     # declined/expired -> back off
    return 10.0 + base * 10.0                                                  # FRESH -> top, spread 10-20 for a varied journey


def header(msg, key):
    for k, v in (msg.headers or []):
        if k == key:
            return v.decode() if v else None
    return None


def main():
    from kafka import KafkaConsumer, KafkaProducer
    consumer = KafkaConsumer(
        EVALS, bootstrap_servers=BOOT, group_id=GROUP,
        auto_offset_reset=OFFSET, enable_auto_commit=True,
        value_deserializer=lambda b: b)
    producer = KafkaProducer(bootstrap_servers=BOOT, acks=1)
    print(f"[journey-scorer] up: {EVALS} -> {MEMBER_FACTS} (group={GROUP}, offset={OFFSET}) "
          f"— scripted journey, NO Databricks", flush=True)

    for msg in consumer:
        # loop-avoidance: score only eligibility-changed evals, skip score-only re-emits (mirrors the dbx scorer).
        if header(msg, "type") == "score":
            continue
        try:
            ev = json.loads(msg.value)
        except Exception:
            continue
        nba_id, ent, et = ev.get("nbaId"), ev.get("entityId"), ev.get("entityType", "OPERATOR")
        if not nba_id or not ent:
            continue
        now = int(time.time() * 1000)
        key = f"{et}:{ent}".encode()                       # key by member so scores co-locate with the member's partition
        n = 0
        for ca in ev.get("channelActions", []):
            if not ca.get("eligible"):
                continue
            fact = {
                "entityType": et, "entityId": ent, "nbaId": nba_id,
                "key": f"nba.score.{ca['actionId']}.{ca['channel']}",
                "value": round(score_action(nba_id, ca), 4), "valueType": "DOUBLE",
                "eventTs": now, "source": "journey-scorer",
            }
            if ev.get("correlationId"):
                fact["correlationId"] = ev["correlationId"]
            producer.send(MEMBER_FACTS, key=key, value=json.dumps(fact).encode(),
                          headers=[("kind", b"score")])
            n += 1
        if n:
            producer.flush()


if __name__ == "__main__":
    main()
