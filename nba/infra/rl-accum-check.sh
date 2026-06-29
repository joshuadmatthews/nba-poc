#!/usr/bin/env bash
# One-shot RL-adaptation accumulation check (used by the "prove the model adapts" monitor loop).
# Reports: training-data growth (n_real) + policy signal (train_auc) from the latest retrain, the OPE/promote
# decision if available, journey breadth (is the chain still producing data), member count, and system health.
set +e
REPO="197121cd "197121dirname "-e")/../.." 2>/dev/null && pwd)"; cd "$REPO" 2>/dev/null
echo "===== RL ACCUM CHECK  $(date -u '+%Y-%m-%d %H:%M:%SZ') ====="

( unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET DATABRICKS_TOKEN DATABRICKS_HOST
  set -a; source nba/databricks/ml/ml.env 2>/dev/null; set +a
  export DATABRICKS_HOST="$ML_DATABRICKS_HOST" DATABRICKS_CLIENT_ID="$ML_DATABRICKS_CLIENT_ID" DATABRICKS_CLIENT_SECRET="$ML_DATABRICKS_CLIENT_SECRET"
  RID=$(MSYS_NO_PATHCONV=1 timeout 30 databricks api get "/api/2.1/jobs/runs/list?job_id=<job-id>&limit=4" 2>/dev/null | python -c "
import sys,json
for r in json.load(sys.stdin).get('runs',[]):
    if r.get('state',{}).get('result_state')=='SUCCESS': print(r['run_id']); break")
  exitof(){ MSYS_NO_PATHCONV=1 timeout 25 databricks api get "/api/2.1/jobs/runs/get-output?run_id=$1" 2>/dev/null | python -c "import sys,json;print((json.load(sys.stdin).get('notebook_output') or {}).get('result','?'))" 2>/dev/null; }
  TASKS=$(MSYS_NO_PATHCONV=1 timeout 25 databricks api get "/api/2.1/jobs/runs/get?run_id=$RID" 2>/dev/null | python -c "import sys,json;[print(t['task_key']+'@'+str(t['run_id'])) for t in json.load(sys.stdin).get('tasks',[])]")
  for e in $TASKS; do
    tk=${e%%@*}; tid=${e##*@}
    [ "$tk" = "train" ] && echo "  train   : $(exitof $tid)"
    [ "$tk" = "evaluate" ] && echo "  evaluate: $(exitof $tid)"
  done )

NA=$(MSYS_NO_PATHCONV=1 podman exec ais-nba-redis redis-cli eval '
local s={}
for _,k in ipairs(redis.call("keys","nba:snapshot:*")) do
  local h=redis.call("hgetall",k)
  for i=1,#h,2 do
    if string.sub(h[i],1,21)=="fact:nba.actionstate." then
      local a=string.match(string.sub(h[i],22),"^(.-)%.[^.]+$"); if a then s[a]=true end
    end
  end
end
local n=0; for _ in pairs(s) do n=n+1 end; return n' 0 2>/dev/null)
MEM=$(MSYS_NO_PATHCONV=1 podman exec ais-nba-redis redis-cli eval 'local n=0; for _ in ipairs(redis.call("keys","nba:idmap:OPERATOR:*")) do n=n+1 end; return n' 0 2>/dev/null)
UP=$(MSYS_NO_PATHCONV=1 podman ps --format "{{.Names}}" 2>/dev/null | grep -c "ais-nba-")
echo "  journey : ${NA:-?}/15 distinct actions firing | members: ${MEM:-?} | system: ${UP:-?} ais-nba-* up"
