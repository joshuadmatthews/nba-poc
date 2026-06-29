# Databricks notebook source
# SEED HEALTHCARE MEMBERS — emit a BASELINE healthcare member population to nba.facts so the live NBA has real members
# to drive up the engagement ladder (Reached -> Registered -> Assessed -> Engaged in Care -> STARS Compliant). Without a
# healthcare population the whole healthcare layer (fact-driven eligibility + milestones + the trained CQL policy) has
# nothing to act on — the live members were a legacy SaaS population (usedChat/seats/csat), not members.
#
# Each member starts at BASELINE: intrinsic attributes (riskScore, diabetic, daysSinceLogin) + consent (smsConsent,
# isDNC) + zeroed contact counters + respondedToOutreach=false. EVERY digital/care fact is absent/false (= not-done) so
# the scorer MUST dispatch the reach action (plan_welcome), the activation layer DRIVES respondedToOutreach, the next
# milestone's action unlocks, and the member climbs — i.e. the model recreates the hand-crafted journeys on live members
# and then optimizes. Members are varied DETERMINISTICALLY by entityId so the population is diverse (the model learns
# per-member channel/affinity as facts come back). origin=seed so the streaming ingest ACCEPTS it (origin=lake is dropped
# as the gold->facts loop-break — see nba_out_produce). One spark-kafka batch write to nba.facts + nba.member.facts.
#
#   python run_seed_members.py [n_members]        (or run the notebook with widget n_members)

# COMMAND ----------

import json, time, hashlib, random
from pyspark.sql import functions as F

# --- inlined from nba_ml_common so this seed is STANDALONE (the LAKE bundle does not deploy nba_ml_common) ---
for _k, _v in [("src_catalog", "workspace"), ("src_schema", "nba_poc"),
               ("bootstrap", ""), ("sasl_user", ""), ("sasl_pass", "")]:
    try: dbutils.widgets.text(_k, _v)
    except Exception: pass
SRC_NS = f"{dbutils.widgets.get('src_catalog')}.{dbutils.widgets.get('src_schema')}"
def kafka_cfg(key):
    v = ""
    try: v = dbutils.widgets.get(key)
    except Exception: pass
    if not v:
        try: v = dbutils.secrets.get("nba-kafka", key)
        except Exception: pass
    return v
def sasl_opts():
    boot, su, sp = kafka_cfg("bootstrap"), kafka_cfg("sasl_user"), kafka_cfg("sasl_pass")
    if not boot: raise ValueError("kafka bootstrap EMPTY (widget or nba-kafka secret scope)")
    jaas = (f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required '
            f'username="{su}" password="{sp}";')
    return {"kafka.bootstrap.servers": boot, "kafka.security.protocol": "SASL_PLAINTEXT",
            "kafka.sasl.mechanism": "SCRAM-SHA-256", "kafka.sasl.jaas.config": jaas}

dbutils.widgets.text("n_members", "400"); dbutils.widgets.text("reuse_existing", "true")
N = int(dbutils.widgets.get("n_members")); REUSE = dbutils.widgets.get("reuse_existing") == "true"
SASL = sasl_opts(); NOW = int(time.time() * 1000)

# POPULATION: reuse existing OPERATOR members (gold_member_idmap already maps entityId -> nbaId, so the eval pipeline
# already knows them and they evaluate immediately) and mint hcm-* ids to top up to N if there aren't enough.
ids = []
if REUSE:
    ids = [r["entityId"] for r in spark.table(f"{SRC_NS}.gold_member_idmap")
           .where("entityType = 'OPERATOR'").select("entityId").orderBy("entityId").limit(N).collect()]
if len(ids) < N:
    ids = ids + [f"hcm-{i:05d}" for i in range(N - len(ids))]
print(f"seeding {len(ids)} healthcare members (reuse_existing={REUSE}, minted {sum(1 for e in ids if e.startswith('hcm-'))})")


def member_facts(eid):
    rng = random.Random(int(hashlib.md5(eid.encode()).hexdigest(), 16))   # deterministic per member -> stable diversity
    cl = lambda v, lo, hi: max(lo, min(hi, v))
    # LATENT FACTORS drive a CORRELATED population (a real member panel, not independent noise):
    risk = round(rng.uniform(0.1, 0.95), 3)                               # clinical severity
    age = rng.randint(50, 88)                                             # Medicare population
    dsnp = rng.random() < 0.30                                           # dual-eligible (low income)
    digital = cl(rng.gauss(0.6 - 0.008 * (age - 50) - (0.22 if dsnp else 0), 0.18), 0.0, 1.0)  # digital propensity
    diabetic = (risk > 0.5 and rng.random() < 0.65)
    # DERIVED clinical/claims (sicker/older/DSNP -> more burden) + engagement telemetry (tracks digital propensity):
    comorbid  = int(round(cl(rng.gauss(risk * 4 + (age - 50) * 0.04, 1.0), 0, 6)))
    er        = int(round(cl(rng.gauss(risk * 3 + (1.5 if dsnp else 0), 1.2), 0, 8)))
    pdc       = round(cl(rng.gauss(0.86 - (0.15 if dsnp else 0) - 0.03 * comorbid, 0.12), 0.3, 1.0), 2)
    sdoh      = rng.random() < (0.45 if dsnp else 0.15)
    gaps      = int(round(cl(rng.gauss(3 - digital * 2 + comorbid * 0.3, 1.0), 0, 7)))
    logins    = int(round(cl(rng.gauss(digital * 20, 4), 0, 40)))
    pages     = int(round(cl(logins * rng.uniform(1.5, 4.0), 0, 160)))
    timeop    = int(round(cl(rng.gauss(40 + digital * 70, 25), 5, 240)))
    benview   = int(round(cl(pages * rng.uniform(0.0, 0.3), 0, 12)))
    # BASELINE progress: not-yet-reached; digital/care facts stay absent (=0) so the member climbs from scratch.
    return [
        ("operator.profile.riskScore",            risk,                  "DOUBLE"),
        ("operator.profile.diabetic",             diabetic,              "BOOL"),
        ("operator.profile.age",                  age,                   "LONG"),
        ("operator.profile.planDSNP",             dsnp,                  "BOOL"),
        ("operator.profile.tenureMonths",         rng.randint(1, 120),   "LONG"),
        ("operator.profile.sdohBarrier",          sdoh,                  "BOOL"),
        ("operator.clinical.comorbidityCount",    comorbid,              "LONG"),
        ("operator.clinical.erVisits12mo",        er,                    "LONG"),
        ("operator.clinical.rxAdherencePDC",      pdc,                   "DOUBLE"),
        ("operator.clinical.openCareGaps",        gaps,                  "LONG"),
        ("operator.activity.portalLogins30d",     logins,                "LONG"),
        ("operator.activity.pagesViewed30d",      pages,                 "LONG"),
        ("operator.activity.avgTimeOnPageSec",    timeop,                "LONG"),
        ("operator.activity.benefitsPageViews",   benview,               "LONG"),
        ("operator.activity.daysSinceLogin",      rng.randint(0, 60),    "LONG"),
        ("operator.profile.smsConsent",           rng.random() < 0.85,   "BOOL"),
        ("operator.profile.isDNC",                rng.random() < 0.03,   "BOOL"),
        ("operator.comms.totalThisWeek",          0,                     "LONG"),
        ("operator.comms.emailsThisWeek",         0,                     "LONG"),
        ("operator.activity.respondedToOutreach", False,                 "BOOL"),
    ]


rows = []
for eid in ids:
    for key, val, vt in member_facts(eid):
        body = {"entityType": "OPERATOR", "entityId": eid, "key": key, "value": val,
                "valueType": vt, "eventTs": NOW, "source": "seed"}      # NO origin header/field -> ingest accepts it
        rows.append((f"OPERATOR:{eid}", json.dumps(body)))

# Write to nba.member.facts (the stream's INPUT topic) with NO origin header (the stream drops any origin-tagged record).
kdf = spark.createDataFrame(rows, "key string, value string")
kdf.write.format("kafka").options(**SASL).option("topic", "nba.member.facts").save()
print(f"emitted {len(rows)} facts for {len(ids)} members to nba.member.facts")
dbutils.notebook.exit(json.dumps({"members": len(ids), "facts": len(rows)}))
