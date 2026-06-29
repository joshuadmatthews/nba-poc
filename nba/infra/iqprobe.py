#!/usr/bin/env python3
"""IQ read-latency prober — hammer the engine's GET /snapshot/{id} (RocksDB-backed Interactive Query) and
report latency percentiles + error/503 rate. Run in-network (so we measure the read, not podman-exec startup)
with C threads, e.g. piped into the journey-scorer image:
  podman run --rm -i --network aiservices_default -e IQ_URL=http://nba-decision-engine:7020/snapshot/<id> \
    -e N=4000 -e C=4 localhost/nba-journey-scorer:latest python - < nba/infra/iqprobe.py
"""
import os
import threading
import time
import urllib.request

URL = os.environ["IQ_URL"]
N = int(os.environ.get("N", "4000"))
C = int(os.environ.get("C", "4"))
lat = []
errs = [0]
codes = {}
lock = threading.Lock()


def worker(n):
    local, e, lcodes = [], 0, {}
    for _ in range(n):
        t = time.time()
        code = 0
        try:
            r = urllib.request.urlopen(URL, timeout=5)
            r.read()
            code = r.status
        except urllib.error.HTTPError as he:
            code = he.code
        except Exception as ex:
            code = type(ex).__name__
        local.append((time.time() - t) * 1000.0)
        lcodes[code] = lcodes.get(code, 0) + 1
        if code != 200:
            e += 1
    with lock:
        lat.extend(local)
        errs[0] += e
        for k, v in lcodes.items():
            codes[k] = codes.get(k, 0) + v


ths = [threading.Thread(target=worker, args=(N // C,)) for _ in range(C)]
t0 = time.time()
for t in ths:
    t.start()
for t in ths:
    t.join()
dt = time.time() - t0
lat.sort()


def pct(p):
    return lat[min(len(lat) - 1, int(len(lat) * p))] if lat else 0.0


print(f"reqs={len(lat)} rps={int(len(lat)/dt) if dt else 0} "
      f"p50={pct(.5):.1f}ms p95={pct(.95):.1f}ms p99={pct(.99):.1f}ms max={(lat[-1] if lat else 0):.1f}ms "
      f"errs={errs[0]} codes={codes}", flush=True)
