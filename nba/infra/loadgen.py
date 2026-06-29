#!/usr/bin/env python3
"""NBA load generator — reliably produce a large burst of member facts to nba.member.facts.

Uses kafka-python with a BOUNDED batch_size (128KB) so produce requests never exceed Redpanda's
kafka_batch_max_bytes (rpk's stdin producer batches the whole pipe into one oversized request and gets
MESSAGE_TOO_LARGE — this does not). Members are fresh (prefix), keys are real nba:rulefacts so the
lean-filter snapshots them, eventTs is monotonic from a base so LWW always accepts (real CREATEs/writes).

Run inside an image that has kafka-python (e.g. the journey-scorer image), script piped on stdin:
  podman run --rm -i --network aiservices_default \
    -e NBA_LOAD_N=1000000 -e NBA_LOAD_MEMBERS=50000 -e NBA_LOAD_PREFIX=lg1_ \
    localhost/nba-journey-scorer:latest python - < nba/infra/loadgen.py
"""
import json
import os
import time

from kafka import KafkaProducer

BOOT = os.environ.get("NBA_BOOTSTRAP", "nba-redpanda:9092")
TOPIC = os.environ.get("NBA_TOPIC", "nba.member.facts")
N = int(os.environ.get("NBA_LOAD_N", "100000"))
M = int(os.environ.get("NBA_LOAD_MEMBERS", "5000"))
PREFIX = os.environ.get("NBA_LOAD_PREFIX", "lg_")
TSBASE = int(os.environ.get("NBA_LOAD_TSBASE", "0")) or int(time.time() * 1000)
# real nba:rulefacts keys so the snapshot lean-filter actually persists them
KEYS = [
    "operator.profile.diabetic", "operator.activity.hraCompleted", "operator.activity.pcpSelected",
    "operator.activity.careTeamEngaged", "operator.comms.totalThisWeek", "operator.activity.respondedToOutreach",
    "operator.activity.medAdherent", "operator.activity.loggedIn",
]

p = KafkaProducer(
    bootstrap_servers=BOOT, linger_ms=20, batch_size=131072, acks=1,
    key_serializer=lambda k: k.encode(), value_serializer=lambda v: v.encode())

t0 = time.time()
for i in range(N):
    m = PREFIX + str(i % M)
    k = KEYS[i % len(KEYS)]
    v = json.dumps({"entityType": "OPERATOR", "entityId": m, "key": k,
                    "value": 1, "valueType": "LONG", "eventTs": TSBASE + i, "source": "load"})
    p.send(TOPIC, key="OPERATOR:" + m, value=v)
p.flush()
dt = time.time() - t0
print(f"produced {N} facts / {M} members prefix={PREFIX} base={TSBASE} in {dt:.1f}s (~{int(N/dt)}/s)", flush=True)
