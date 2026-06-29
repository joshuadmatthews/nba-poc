#!/usr/bin/env python3
"""Multi-shape NBA load generator for per-STAGE throughput tests. NBA_GEN_MODE picks the record shape so each
stage can be driven on its own input topic, in isolation:

  fact     -> nba.member.facts   : member facts (snapshot stage input)            {entityType,entityId,key,value,...}
  snapshot -> nba.snapshots[.shadow] : a member snapshot (rules stage input)      {nbaId,facts:{...qualifying...}}
  eval     -> nba.evaluations[.shadow]: an evaluation w/ an eligible scored action (router stage input)
  router   -> nba.member.facts[.shadow] (header kind=router): a CREATE activation  (state-machine stage input)

Bounded kafka-python batches so produce never trips MESSAGE_TOO_LARGE. Each record is a distinct member so the
downstream does real per-key work (CREATE/new-workflow), maximizing the stage's load.
"""
import json
import os
import time
import uuid

from kafka import KafkaProducer

BOOT = os.environ.get("NBA_BOOTSTRAP", "nba-redpanda:9092")
MODE = os.environ.get("NBA_GEN_MODE", "fact")
TOPIC = os.environ.get("NBA_TOPIC", "nba.member.facts")
N = int(os.environ.get("NBA_LOAD_N", "200000"))
M = int(os.environ.get("NBA_LOAD_MEMBERS", str(N)))   # default: every record a distinct member
PREFIX = os.environ.get("NBA_LOAD_PREFIX", "gen_")
TSBASE = int(os.environ.get("NBA_LOAD_TSBASE", "0")) or int(time.time() * 1000)
ACTION = os.environ.get("NBA_GEN_ACTION", "action_test")
CHANNELS = os.environ.get("NBA_GEN_CHANNELS", "sms,email,push").split(",")
RULEFACTS = ["operator.profile.diabetic", "operator.activity.hraCompleted", "operator.activity.pcpSelected",
             "operator.activity.careTeamEngaged", "operator.activity.respondedToOutreach", "operator.profile.isDNC"]

p = KafkaProducer(bootstrap_servers=BOOT, linger_ms=20, batch_size=131072, acks=1,
                  key_serializer=lambda k: k.encode(), value_serializer=lambda v: v.encode())


def qualifying_facts(ts):
    # facts shape inside a snapshot: key -> {value,valueType,eventTs,source}
    f = {}
    for k in ["operator.profile.diabetic", "operator.activity.respondedToOutreach", "operator.activity.careTeamEngaged"]:
        f[k] = {"value": True, "valueType": "BOOLEAN", "eventTs": ts, "source": "gen"}
    f["operator.profile.isDNC"] = {"value": False, "valueType": "BOOLEAN", "eventTs": ts, "source": "gen"}
    f["operator.comms.totalThisWeek"] = {"value": 0, "valueType": "LONG", "eventTs": ts, "source": "gen"}
    return f


t0 = time.time()
for i in range(N):
    m = PREFIX + str(i % M)
    nbaId = "nbagen_" + m
    ch = CHANNELS[i % len(CHANNELS)]
    ts = TSBASE + i
    if MODE == "fact":
        k = RULEFACTS[i % len(RULEFACTS)]
        v = json.dumps({"entityType": "OPERATOR", "entityId": m, "key": k, "value": True,
                        "valueType": "BOOLEAN", "eventTs": ts, "source": "gen"})
        p.send(TOPIC, key="OPERATOR:" + m, value=v)
    elif MODE == "snapshot":
        v = json.dumps({"nbaId": nbaId, "entityType": "OPERATOR", "entityId": m,
                        "correlationId": str(uuid.uuid4()), "updatedTs": ts, "facts": qualifying_facts(ts)})
        p.send(TOPIC, key=nbaId, value=v)
    elif MODE == "eval":
        v = json.dumps({"nbaId": nbaId, "entityType": "OPERATOR", "entityId": m,
                        "correlationId": str(uuid.uuid4()), "evaluatedAt": ts, "eligibilityChanged": True,
                        "channelActions": [{"actionId": ACTION, "channel": ch, "name": "Test", "ttlSeconds": 3600,
                                            "contentKey": "welcome", "eligible": True, "score": 15.0, "active": False,
                                            "cancellable": False, "hardCompleted": False, "softCompleted": False,
                                            "workflowState": None}]})
        p.send(TOPIC, key=nbaId, value=v)
    elif MODE == "router":
        v = json.dumps({"op": "CREATE", "nbaId": nbaId, "entityType": "OPERATOR", "entityId": m, "memberId": m,
                        "actionId": ACTION, "channel": ch, "name": "Test", "contentKey": "welcome",
                        "ttlSeconds": 3600, "score": 15.0, "correlationId": str(uuid.uuid4()),
                        "source": "gen", "eventTs": ts})
        p.send(TOPIC, key="OPERATOR:" + m, value=v, headers=[("kind", b"router")])
p.flush()
dt = time.time() - t0
print(f"gen mode={MODE} -> {TOPIC}: {N} records / {M} members in {dt:.1f}s (~{int(N/dt)}/s)", flush=True)
