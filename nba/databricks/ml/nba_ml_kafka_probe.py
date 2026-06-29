# Databricks notebook source
# KAFKA PROBE — does THIS workspace's serverless reach the NBA Kafka tunnel? Tries one produce + one read and
# returns the exact error, so we can tell network-egress (timeout/connection refused) from auth (SASL) from ACL
# (authorization) from a code issue. Pure diagnostic.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, time
ML_NS, SRC_NS = ml_widgets()
out = {"bootstrap": dbutils.widgets.get("bootstrap")}

def root_cause(e):
    msgs = []
    try:
        j = e.java_exception
        while j is not None:
            msgs.append(j.getClass().getName() + ": " + (j.getMessage() or ""))
            j = j.getCause()
    except Exception:
        msgs.append(str(e))
    return " || ".join(msgs)[:1500]

# 0a) RAW SOCKET test — is the broker reachable AT ALL from this serverless compute? (network vs config)
import socket
host, _, port = dbutils.widgets.get("bootstrap").partition(":")
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.settimeout(8)
try:
    s.connect((host, int(port or 9092))); out["socket"] = "CONNECTED (network OK)"
except Exception as e:
    out["socket"] = "FAIL: " + type(e).__name__ + ": " + str(e)
finally:
    try: s.close()
    except Exception: pass

# 0) what compute + what does sasl look like (mechanism, protocol, bootstrap)
o = sasl_opts()
out["security_protocol"] = o.get("kafka.security.protocol"); out["sasl_mechanism"] = o.get("kafka.sasl.mechanism")
try: out["compute"] = spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "?")
except Exception: out["compute"] = "?"

# 1) PRODUCE one tiny message
try:
    (spark.createDataFrame([("probe", json.dumps({"probe": int(time.time())}))], "key string, value string")
     .write.format("kafka").options(**sasl_opts()).option("topic", "nba.model.card").save())
    out["produce"] = "OK"
except Exception as e:
    out["produce"] = "FAIL"; out["produce_error"] = root_cause(e)

# 2) READ a few records (proves consume path independently)
try:
    df = (spark.read.format("kafka").options(**sasl_opts())
          .option("subscribe", "nba.model.card").option("startingOffsets", "earliest")
          .option("endingOffsets", "latest").option("kafka.request.timeout.ms", "15000").load())
    out["read_count"] = df.count(); out["read"] = "OK"
except Exception as e:
    out["read"] = "FAIL"; out["read_error"] = (type(e).__name__ + ": " + str(e))[:600]

print(json.dumps(out, indent=2))
dbutils.notebook.exit(json.dumps(out))
