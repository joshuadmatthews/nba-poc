#!/usr/bin/env python3
"""LOCAL medallion runner — the demo stand-in for the Databricks lake.

Consumes `datalake.streaming-inbound` (heterogeneous raw source records), normalizes each into canonical NBA
facts, and emits:
  - nba.facts        : ALL normalized facts  (the all-facts feature stream for a FUTURE ML layer; nobody
                       subscribes yet — it's emitted so the lake->ML(future) edge is real)
  - nba.member.facts : the action-mapped subset (the snapshot-builder's only input)

`normalize_inbound` here MIRRORS the same function in databricks/nba_datalake_stream.py. Production runs the
Databricks job; this exists so source->lake->member.facts flows live on the local System Map. Keep the two
copies in sync when you onboard a new source format.

Run it BEFORE driving source_gen.py (it tails from the end of the topic). Ctrl-C to stop.
"""
import json, subprocess, sys, time

RP = "ais-nba-redpanda"


def _vt(v):
    if isinstance(v, bool): return "BOOLEAN"
    if isinstance(v, int): return "LONG"
    if isinstance(v, float): return "DOUBLE"
    return "STRING"


def _camel(k):
    parts = str(k).replace("-", "_").split("_")
    return parts[0] + "".join(p[:1].upper() + p[1:] for p in parts[1:]) if len(parts) > 1 else parts[0]

# source dialect -> canonical NBA fact key (MIRROR of the Databricks job's CRM_MAP).
CRM_MAP = {
    "dayssincelogin": "operator.activity.daysSinceLogin",
    "completedtasks": "operator.activity.completedTasks",
    "vieweddashboard": "operator.activity.viewedDashboard",
    "usedchat": "operator.activity.usedChat",
    "isdnc": "operator.profile.isDNC",
    "smsconsent": "operator.profile.smsConsent",
    "emailsthisweek": "operator.comms.emailsThisWeek",
    "totalcommsthisweek": "operator.comms.totalThisWeek",
}

def _crm_key(field):
    return CRM_MAP.get(str(field).replace("_", "").replace("-", "").lower())

# Action-mapped fact keys (the rules' factsUsed). In the real lake this comes from dim_definitions; here we
# hardcode the deployed rulefacts so the action-mapped subset -> member.facts is faithful (non-rulefacts like
# operator.plan / operator.lastEvent / operator.csat ride nba.facts ONLY — the future-ML feature stream).
RULEFACTS = {
    "operator.activity.completedTasks", "operator.activity.daysSinceLogin", "operator.activity.viewedDashboard",
    "operator.activity.usedChat", "operator.comms.emailsThisWeek", "operator.comms.totalThisWeek",
    "operator.profile.isDNC", "operator.profile.smsConsent",
}


def normalize_inbound(raw, ts):
    """Heterogeneous raw source record -> 0..N canonical facts. MIRROR of the Databricks job."""
    if not isinstance(raw, dict):
        return []
    src = str(raw.get("_src") or raw.get("system") or raw.get("source") or "unknown").lower()
    ts = int(raw.get("ts") or raw.get("eventTs") or raw.get("timestamp") or ts)
    out = []

    def fact(eid, key, val):
        if eid is None or val is None:
            return
        out.append({"entityType": "OPERATOR", "entityId": str(eid), "key": key, "value": val,
                    "valueType": _vt(val), "eventTs": ts, "source": "lake:" + src})

    if raw.get("contactId") or raw.get("contact_id") or src.startswith("crm"):
        eid = raw.get("contactId") or raw.get("contact_id") or raw.get("id")
        for k, v in (raw.get("fields") or raw.get("attributes") or {}).items():
            fact(eid, _crm_key(k) or ("operator.activity." + _camel(k)), v)
    elif raw.get("account") or raw.get("customer") or src == "billing":
        eid = raw.get("account") or raw.get("customer") or raw.get("customerId")
        for k in ("plan", "mrr", "seats", "status", "trialDaysLeft"):
            if raw.get(k) is not None:
                fact(eid, "operator." + k, raw[k])
    elif raw.get("event") or src.startswith("product") or src.startswith("events"):
        eid = raw.get("user") or raw.get("uid") or raw.get("userId")
        ev, props = raw.get("event"), (raw.get("props") or raw.get("properties") or {})
        if ev:
            fact(eid, "operator.lastEvent", str(ev))
        for k, v in props.items():
            fact(eid, "operator." + _camel(k), v)
    elif raw.get("ticket") or src == "support":
        t = raw.get("ticket") or raw
        eid = t.get("account") or t.get("accountId")
        for k in ("csat", "priority", "openTickets"):
            if t.get(k) is not None:
                fact(eid, "operator." + k, t[k])
    elif raw.get("entityId") and raw.get("key"):
        v = raw.get("value")
        out.append({"entityType": raw.get("entityType", "OPERATOR"), "entityId": str(raw["entityId"]),
                    "key": raw["key"], "value": v, "valueType": raw.get("valueType") or _vt(v),
                    "eventTs": ts, "source": "lake:" + src})
    return out


def main():
    facts_p = subprocess.Popen(["podman", "exec", "-i", RP, "rpk", "topic", "produce", "nba.facts", "-f", "%k\t%v\n"],
                               stdin=subprocess.PIPE, text=True)
    member_p = subprocess.Popen(["podman", "exec", "-i", RP, "rpk", "topic", "produce", "nba.member.facts", "-f", "%k\t%v\n"],
                                stdin=subprocess.PIPE, text=True)
    cons = subprocess.Popen(["podman", "exec", RP, "rpk", "topic", "consume", "datalake.streaming-inbound", "-o", "end", "-f", "%v\n"],
                            stdout=subprocess.PIPE, text=True)
    print("[medallion] tailing datalake.streaming-inbound -> nba.facts + nba.member.facts ...", flush=True)
    n_in = n_f = n_m = 0
    try:
        for line in cons.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except Exception:
                continue
            n_in += 1
            for cf in normalize_inbound(raw, int(time.time() * 1000)):
                key = cf["entityType"] + ":" + cf["entityId"]
                body = json.dumps(cf, separators=(",", ":"))
                facts_p.stdin.write(f"{key}\t{body}\n"); facts_p.stdin.flush(); n_f += 1
                # action-mapped subset -> member.facts (the snapshot-builder's input). Non-rulefacts stay on
                # nba.facts only (future ML). This is the lake's all-facts vs action-mapped split, made real.
                if cf["key"] in RULEFACTS:
                    member_p.stdin.write(f"{key}\t{body}\n"); member_p.stdin.flush(); n_m += 1
            if n_in % 20 == 0:
                print(f"[medallion] inbound={n_in} -> nba.facts={n_f} member.facts={n_m}", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[medallion] STOP: inbound={n_in} -> nba.facts={n_f} member.facts={n_m}", flush=True)


if __name__ == "__main__":
    main()
