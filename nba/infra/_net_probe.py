# Databricks notebook source
# Serverless-side network reachability probe: can cloud compute reach the external kafka tunnel at the TCP layer?
import socket, json
res = {}
try:
    res["dns_us_external"] = socket.gethostbyname("<tunnel-endpoint>")
except Exception as e:
    res["dns_us_external"] = f"DNSFAIL {type(e).__name__}: {e}"
for label, host, port in [("fqdn", "<tunnel-endpoint>", 19092), ("literal_ip", "45.55.35.48", 19092)]:
    try:
        s = socket.create_connection((host, port), timeout=12)
        res[label] = f"TCP_OK peer={s.getpeername()}"
        s.close()
    except Exception as e:
        res[label] = f"TCP_FAIL {type(e).__name__}: {e}"
print(json.dumps(res))
dbutils.notebook.exit(json.dumps(res))
