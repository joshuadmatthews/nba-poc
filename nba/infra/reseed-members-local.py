#!/usr/bin/env python
"""RESEED MEMBERS LOCALLY — run the EXACT baseline-member logic from nba_seed_healthcare_members.py, but emit
straight to the local nba.member.facts (no Databricks / no tunnel). Resets the existing OPERATOR members
(read from the local idmap) back to BASELINE: every digital/care fact not-done, respondedToOutreach=false, contact
counters zeroed — so they re-journey from scratch (plan_welcome -> ... -> STARS) under the real subscribed scorer.

  python nba/infra/reseed-members-local.py [n_members]
"""
import json, os, time, hashlib, random, subprocess, sys

REDIS = "ais-nba-redis"; PANDA = "ais-nba-redpanda"

# Target topic. Default = the lake's raw ingress (datalake.streaming-inbound -> bronze -> ... -> member.facts),
# which needs Databricks. Set NBA_SEED_TOPIC=nba.member.facts to seed the local flywheel DIRECTLY (no Databricks,
# no tunnel) — the snapshot-builder folds these canonical facts straight away. up.ps1 uses the direct target.
SEED_TOPIC = os.environ.get("NBA_SEED_TOPIC", "datalake.streaming-inbound")


def member_facts(eid):                       # EXACT replica of nba_seed_healthcare_members.member_facts
    rng = random.Random(int(hashlib.md5(eid.encode()).hexdigest(), 16))
    cl = lambda v, lo, hi: max(lo, min(hi, v))
    risk = round(rng.uniform(0.1, 0.95), 3)
    age = rng.randint(50, 88)
    dsnp = rng.random() < 0.30
    digital = cl(rng.gauss(0.6 - 0.008 * (age - 50) - (0.22 if dsnp else 0), 0.18), 0.0, 1.0)
    diabetic = (risk > 0.5 and rng.random() < 0.65)
    comorbid = int(round(cl(rng.gauss(risk * 4 + (age - 50) * 0.04, 1.0), 0, 6)))
    er = int(round(cl(rng.gauss(risk * 3 + (1.5 if dsnp else 0), 1.2), 0, 8)))
    pdc = round(cl(rng.gauss(0.86 - (0.15 if dsnp else 0) - 0.03 * comorbid, 0.12), 0.3, 1.0), 2)
    sdoh = rng.random() < (0.45 if dsnp else 0.15)
    gaps = int(round(cl(rng.gauss(3 - digital * 2 + comorbid * 0.3, 1.0), 0, 7)))
    logins = int(round(cl(rng.gauss(digital * 20, 4), 0, 40)))
    pages = int(round(cl(logins * rng.uniform(1.5, 4.0), 0, 160)))
    timeop = int(round(cl(rng.gauss(40 + digital * 70, 25), 5, 240)))
    benview = int(round(cl(pages * rng.uniform(0.0, 0.3), 0, 12)))
    return [
        ("operator.profile.riskScore",          risk,                "DOUBLE"),
        ("operator.profile.diabetic",           diabetic,            "BOOL"),
        ("operator.profile.age",                age,                 "LONG"),
        ("operator.profile.planDSNP",           dsnp,                "BOOL"),
        ("operator.profile.tenureMonths",       rng.randint(1, 120), "LONG"),
        ("operator.profile.sdohBarrier",        sdoh,                "BOOL"),
        ("operator.clinical.comorbidityCount",  comorbid,            "LONG"),
        ("operator.clinical.erVisits12mo",      er,                  "LONG"),
        ("operator.clinical.rxAdherencePDC",    pdc,                 "DOUBLE"),
        ("operator.clinical.openCareGaps",      gaps,                "LONG"),
        ("operator.activity.portalLogins30d",   logins,              "LONG"),
        ("operator.activity.pagesViewed30d",    pages,               "LONG"),
        ("operator.activity.avgTimeOnPageSec",  timeop,              "LONG"),
        ("operator.activity.benefitsPageViews", benview,             "LONG"),
        ("operator.activity.daysSinceLogin",    rng.randint(0, 60),  "LONG"),
        ("operator.profile.smsConsent",         rng.random() < 0.85, "BOOL"),
        ("operator.profile.isDNC",              rng.random() < 0.03, "BOOL"),
        ("operator.comms.totalThisWeek",        0,                   "LONG"),
        ("operator.comms.emailsThisWeek",       0,                   "LONG"),
        ("operator.activity.respondedToOutreach", False,             "BOOL"),
    ]


def main():
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    start = int(sys.argv[2]) if len(sys.argv) > 2 else None
    if start is not None:
        # ADD N brand-new members (fresh hcm id range from `start`) WITHOUT resetting the existing population —
        # their in-progress journeys keep advancing. Use to grow the population (more journey/training data).
        ids = [f"hcm-{i:05d}" for i in range(start, start + N)]
        print(f"seeding {N} NEW members hcm-{start:05d}..hcm-{start + N - 1:05d} -> bronze (existing untouched)")
    else:
        keys = subprocess.run(["podman", "exec", REDIS, "redis-cli", "--scan", "--pattern", "nba:idmap:OPERATOR:*"],
                              capture_output=True, text=True, timeout=30).stdout.split()
        ids = sorted({k.split("OPERATOR:")[-1].strip() for k in keys if k.strip()})
        ids = ids[:N]
        if len(ids) < N:                          # top up with minted hcm-* ids (matches the seed)
            ids += [f"hcm-{i:05d}" for i in range(N - len(ids))]
        print(f"reseeding {len(ids)} members to BASELINE -> bronze")

    now = int(time.time() * 1000)
    rows = []
    for eid in ids:
        for key, val, vt in member_facts(eid):
            body = {"entityType": "OPERATOR", "entityId": eid, "key": key, "value": val,
                    "valueType": vt, "eventTs": now, "source": "seed"}
            rows.append("OPERATOR:" + eid + "\t" + json.dumps(body))

    # emit to the LAKE's raw source ingress (datalake.streaming-inbound), NOT straight to member.facts: these are
    # already-canonical facts, so normalize_inbound passes them through -> bronze -> silver -> gold. out-produce then
    # pushes the gold deltas back to member.facts (the loop), so gold AND the loop reset consistently. Chunked so we
    # don't flood the ingest.
    CH = 2000
    for i in range(0, len(rows), CH):
        subprocess.run(["podman", "exec", "-i", PANDA, "rpk", "topic", "produce", SEED_TOPIC, "-f", "%k\t%v\n"],
                       input="\n".join(rows[i:i + CH]), capture_output=True, text=True, timeout=60)
    dest = "nba.member.facts (local flywheel, direct)" if SEED_TOPIC == "nba.member.facts" else f"{SEED_TOPIC} (bronze)"
    print(f"emitted {len(rows)} baseline facts for {len(ids)} members -> {dest}")


if __name__ == "__main__":
    main()
