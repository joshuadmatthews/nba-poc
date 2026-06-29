#!/usr/bin/env python3
"""Latency benchmark for the synchronous NBA fast path (action-library POST /disposition).
Hits the endpoint N times per eligibility mode, cycling members/actions/channels, and reports per-stage
p50/p95 (server timings) + client round-trip. Stdlib only — run INSIDE a container on the nba network
(reaches nba-action-library:7001), e.g. via the nba-model container which has python.

  NBA_BENCH_MEMBERS=hcm-1,hcm-2,...  NBA_BENCH_N=200  python bench-fastpath.py
"""
import json, os, time, urllib.request

URL = os.environ.get("NBA_BENCH_URL", "http://nba-action-library:7001")
MEMBERS = [m for m in os.environ.get("NBA_BENCH_MEMBERS", "").split(",") if m]
N = int(os.environ.get("NBA_BENCH_N", "200"))
WARM = int(os.environ.get("NBA_BENCH_WARM", "15"))
CHANNELS = ["email", "push", "voice"]                 # skip sms (consent) + mail (no disposition arm)
ACTIONS = ["action_reengage", "action_plan_welcome", "action_hra", "action_med_adherence", "action_pcp_selection"]
STAGES = ["snapshot_ms", "merge_ms", "elig_ms", "score_ms", "total_ms", "rtt_ms"]


def pct(a, p):
    if not a:
        return 0.0
    a = sorted(a)
    return a[min(len(a) - 1, int(round((p / 100.0) * (len(a) - 1))))]


def one(mode, i):
    m = MEMBERS[i % len(MEMBERS)]; ch = CHANNELS[i % len(CHANNELS)]; act = ACTIONS[i % len(ACTIONS)]
    body = json.dumps({"entityId": m, "actionId": act, "channel": ch, "status": "Opened",
                       "mode": mode, "n": 1, "writeback": "none"}).encode()
    req = urllib.request.Request(URL + "/disposition", data=body, headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    try:
        d = json.load(urllib.request.urlopen(req, timeout=10))
    except Exception:
        return None
    rtt = (time.perf_counter() - t0) * 1000.0
    t = d.get("timings", {}); t["rtt_ms"] = rtt; t["_elig"] = d.get("eligibleCount", 0)
    return t


def run(mode):
    for i in range(WARM):                              # warm Drools KieBase / catalog / connections
        one(mode, i)
    rows = [r for i in range(N) if (r := one(mode, i)) is not None]
    elig = [r["_elig"] for r in rows]
    print(f"\n  mode={mode}   ok={len(rows)}/{N}   avg eligible/req={sum(elig)/max(1,len(elig)):.1f}")
    print(f"    {'stage':12} {'p50':>9} {'p95':>9}  (ms)")
    for k in STAGES:
        a = [r.get(k, 0) for r in rows]
        print(f"    {k:12} {pct(a,50):9.2f} {pct(a,95):9.2f}")


if __name__ == "__main__":
    print(f"=== NBA fast-path benchmark — {N} reqs/mode (+{WARM} warmup), {len(MEMBERS)} members, channels={CHANNELS} ===")
    if not MEMBERS:
        raise SystemExit("set NBA_BENCH_MEMBERS=comma,separated,entityIds")
    for mode in ("inproc", "kie"):
        run(mode)
