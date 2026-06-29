"""nba-model — local CQL Q-net serving endpoint. The action-library's fast-path /disposition calls /score here to get
per-(action,channel) Q-values in-process (pure numpy), instead of the ~12s cloud scorer round-trip. Model-agnostic:
reads its dims from the qnet artifact. POST /score {facts, candidates} -> [{actionId,channel,score}] + holdScore."""
import time
from typing import Any, Dict, List, Optional
from fastapi import FastAPI
from pydantic import BaseModel
from model import get_qnet

app = FastAPI(title="nba-model", docs_url="/docs")


class ScoreReq(BaseModel):
    facts: Dict[str, Any]                       # snapshot fact map: {factKey: {value,eventTs,...}} OR {factKey: value}
    candidates: List[Dict[str, str]]            # [{actionId, channel}, ...] — the eligible arms to score
    nbaId: Optional[str] = None                 # passthrough for logging/trace only


@app.get("/healthz")
def healthz():
    return get_qnet().health()


@app.post("/score")
def score(req: ScoreReq):
    t0 = time.perf_counter()
    q = get_qnet()
    scores, hold = q.score(req.facts, req.candidates)
    return {"scores": scores, "holdScore": hold, "model_ms": round((time.perf_counter() - t0) * 1000, 3),
            "state_dim": q.state_dim, "nbaId": req.nbaId}
