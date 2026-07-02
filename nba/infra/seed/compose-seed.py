#!/usr/bin/env python3
"""Network-native seeder for the docker/podman COMPOSE stack.

Talks to Redpanda (kafka-python) and Redis (redis-py) DIRECTLY over the compose network — no `podman/docker
exec`, so it is runtime-agnostic (works identically under Docker Desktop, podman, colima). Seeds, in order:
  1. action/rule DEFINITIONS  -> nba.definitions   (replays infra/seed/definitions.jsonl, key<TAB>value)
  2. Redis definition state    -> nba:rulefacts (lean-filter) + nba:sim:effect  (applies infra/seed/redis-defs.sh)
  3. N demo MEMBERS            -> nba.member.facts  (the EXACT reseed-members-local member_facts logic)

It reads the same canonical seed files the PowerShell path uses, so there is no drift. Idempotent; waits for
Redpanda + Redis to be reachable first. Env: NBA_BOOTSTRAP, NBA_REDIS_HOST, NBA_REDIS_PORT, NBA_SEED_MEMBERS,
NBA_SEED_DIR.
"""
import base64, hashlib, json, os, random, time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import redis

BOOT     = os.environ.get("NBA_BOOTSTRAP", "nba-redpanda:9092")
RHOST    = os.environ.get("NBA_REDIS_HOST", "nba-redis")
RPORT    = int(os.environ.get("NBA_REDIS_PORT", "6379"))
N        = int(os.environ.get("NBA_SEED_MEMBERS", "200"))
SEED_DIR = os.environ.get("NBA_SEED_DIR", "/seed")


def wait_kafka():
    for _ in range(90):
        try:
            KafkaAdminClient(bootstrap_servers=BOOT, request_timeout_ms=3000).close(); return
        except Exception:
            time.sleep(2)
    raise SystemExit(f"[seed] redpanda {BOOT} unreachable")


def wait_redis():
    r = redis.Redis(host=RHOST, port=RPORT, socket_connect_timeout=3)
    for _ in range(90):
        try:
            if r.ping(): return r
        except Exception:
            time.sleep(2)
    raise SystemExit(f"[seed] redis {RHOST}:{RPORT} unreachable")


def member_facts(eid):
    """EXACT replica of reseed-members-local.member_facts (which mirrors nba_seed_healthcare_members)."""
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
        ("operator.profile.riskScore", risk, "DOUBLE"),
        ("operator.profile.diabetic", diabetic, "BOOL"),
        ("operator.profile.age", age, "LONG"),
        ("operator.profile.planDSNP", dsnp, "BOOL"),
        ("operator.profile.tenureMonths", rng.randint(1, 120), "LONG"),
        ("operator.profile.sdohBarrier", sdoh, "BOOL"),
        ("operator.clinical.comorbidityCount", comorbid, "LONG"),
        ("operator.clinical.erVisits12mo", er, "LONG"),
        ("operator.clinical.rxAdherencePDC", pdc, "DOUBLE"),
        ("operator.clinical.openCareGaps", gaps, "LONG"),
        ("operator.activity.portalLogins30d", logins, "LONG"),
        ("operator.activity.pagesViewed30d", pages, "LONG"),
        ("operator.activity.avgTimeOnPageSec", timeop, "LONG"),
        ("operator.activity.benefitsPageViews", benview, "LONG"),
        ("operator.activity.daysSinceLogin", rng.randint(0, 60), "LONG"),
        ("operator.profile.smsConsent", rng.random() < 0.85, "BOOL"),
        ("operator.profile.isDNC", rng.random() < 0.03, "BOOL"),
        ("operator.comms.totalThisWeek", 0, "LONG"),
        ("operator.comms.emailsThisWeek", 0, "LONG"),
        ("operator.activity.respondedToOutreach", False, "BOOL"),
    ]


def seed_definitions(p):
    path = os.path.join(SEED_DIR, "definitions.jsonl")
    if not os.path.exists(path):
        print("  [warn] definitions.jsonl missing — rules-engine will have no rules"); return
    n = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n").rstrip("\r")
            if not line:
                continue
            k, _, v = line.partition("\t")
            p.send("nba.definitions", key=k.encode(), value=v.encode()); n += 1
    p.flush()
    print(f"  definitions: {n} records -> nba.definitions")


def seed_redis(r):
    """Apply infra/seed/redis-defs.sh over the network (its `redis-cli` lines, translated to redis-py)."""
    path = os.path.join(SEED_DIR, "redis-defs.sh")
    if not os.path.exists(path):
        print("  [warn] redis-defs.sh missing — no rulefacts (snapshot lean-filter fails OPEN = snapshot everything)"); return
    ops = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s.startswith("redis-cli del "):
                r.delete(s.split()[2]); ops += 1
            elif s.startswith("redis-cli sadd "):
                parts = s.split(); r.sadd(parts[2], parts[3]); ops += 1
            elif "base64 -d | redis-cli -x set" in s:
                b64 = s.split("echo", 1)[1].strip().split("|", 1)[0].strip()
                key = s.rsplit("set", 1)[1].split(">")[0].strip()
                r.set(key, base64.b64decode(b64)); ops += 1
    print(f"  redis defs: {ops} ops (nba:rulefacts lean-filter + nba:sim:effect)")


def seed_members(p):
    now = int(time.time() * 1000)
    ids = [f"hcm-{i:05d}" for i in range(N)]
    rows = 0
    for eid in ids:
        for key, val, vt in member_facts(eid):
            body = {"entityType": "OPERATOR", "entityId": eid, "key": key, "value": val,
                    "valueType": vt, "eventTs": now, "source": "seed"}
            p.send("nba.member.facts", key=("OPERATOR:" + eid).encode(), value=json.dumps(body).encode()); rows += 1
    p.flush()
    print(f"  members: {rows} baseline facts for {len(ids)} members -> nba.member.facts")


def main():
    print(f"[seed] redpanda={BOOT} redis={RHOST}:{RPORT} members={N}")
    wait_kafka(); r = wait_redis()
    # let the fold consumers (snapshot-builder / rules-engine / action-router) finish joining their groups
    # before we produce, so nothing is missed if a consumer starts at the live edge.
    time.sleep(int(os.environ.get("NBA_SEED_GRACE", "10")))
    p = KafkaProducer(bootstrap_servers=BOOT, linger_ms=20, batch_size=131072, acks=1)
    seed_definitions(p)
    seed_redis(r)
    seed_members(p)
    print("[seed] complete — the snapshot-builder folds these into nba:snapshot:* immediately")


if __name__ == "__main__":
    main()
