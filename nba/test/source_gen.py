#!/usr/bin/env python3
"""Heterogeneous SOURCE-SYSTEM generator for the NBA demo.

Emits RAW records in DIFFERENT shapes — a CRM export blob, a flat billing row, a product-event envelope, a
support ticket — to `datalake.streaming-inbound`. These are deliberately NOT canonical facts: each source
system speaks its own dialect. The medallion (Databricks `nba_datalake_stream.py`, or the local
`medallion_runner.py` stand-in) is what normalizes them into the one canonical fact vocabulary. That
normalization IS the point of the lake — so the demo drives messy source data, not pre-baked facts.

Usage:  python3 source_gen.py [N_OPERATORS=30] [PREFIX=sg]
"""
import json, random, subprocess, sys, time

RP = "ais-nba-redpanda"
TOPIC = "datalake.streaming-inbound"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 30
PREFIX = sys.argv[2] if len(sys.argv) > 2 else "sg"


def produce(records):
    p = subprocess.Popen(["podman", "exec", "-i", RP, "rpk", "topic", "produce", TOPIC, "-f", "%k\t%v\n"],
                         stdin=subprocess.PIPE, text=True)
    for k, v in records:
        p.stdin.write(f"{k}\t{json.dumps(v, separators=(',', ':'))}\n")
    p.stdin.close()
    return p.wait()


def main():
    recs = []
    for i in range(N):
        eid = f"op-{PREFIX}-{i}"
        now = int(time.time() * 1000)
        # 1) CRM export — a contactId with a nested fields{} blob in the source's OWN snake_case dialect. The
        #    medallion reconciles each field to the canonical rulefact key (days_since_login ->
        #    operator.activity.daysSinceLogin, is_dnc -> operator.profile.isDNC, ...). These drive eligibility.
        recs.append((eid, {"_src": "crm-export", "contactId": eid, "ts": now,
                           "fields": {"days_since_login": random.choice([8, 15, 20, 30]),
                                      "completed_tasks": random.choice([0, 0, 1, 2, 8]),
                                      "viewed_dashboard": random.choice([False, False, True]),
                                      "used_chat": random.choice([True, False]),
                                      "is_dnc": random.choice([False, False, False, True]),
                                      "sms_consent": random.choice([True, False]),
                                      "emails_this_week": 0, "total_comms_this_week": 0}}))
        # 2) billing — a flat account row in a totally different shape.
        recs.append((eid, {"system": "billing", "account": eid, "ts": now,
                           "plan": random.choice(["free", "free", "pro", "enterprise"]),
                           "mrr": random.choice([0, 0, 49, 499]), "seats": random.randint(1, 20)}))
        # 3) product events — a verb + props bag keyed by user.
        recs.append((eid, {"_src": "product-events", "user": eid, "ts": now,
                           "event": random.choice(["login", "feature_used", "invite_sent"]),
                           "props": {"feature": random.choice(["reports", "api", "dashboard"]),
                                     "count": random.randint(1, 9)}}))
        # 4) support — an occasional ticket wrapped one level deep.
        if i % 4 == 0:
            recs.append((eid, {"_src": "support", "ticket": {"account": eid, "csat": random.randint(1, 5),
                                                             "priority": random.choice(["low", "high"]),
                                                             "openTickets": random.randint(0, 3)}}))
    random.shuffle(recs)
    rc = produce(recs)
    print(f"[source-gen] produced {len(recs)} raw records across 4 source-system formats "
          f"for {N} operators (prefix={PREFIX}) -> {TOPIC} (rc={rc})")


if __name__ == "__main__":
    main()
