# NBA ML Pipeline (Databricks)

The ML layer for NBA, as a Databricks Asset Bundle that runs in the **ML workspace** and reads the NBA lake's
silver/gold tables from Unity Catalog. NBA is a **contextual-bandit / offline-RL** problem: context = member
features, arms = eligible (action, channel), reward = conversion (`HARD_COMPLETED`).

> **`nba.facts` is not needed here.** The feature store already lives in UC: `silver_snapshots` (point-in-time
> features per decision — no leakage), `gold_member_snapshot` (current features for scoring). When this layer is
> live, the Kafka `nba.facts` topic + the snapshot-builder firehose that feeds it become retire-able.

## Files

| Notebook | Role | Maturity |
|----------|------|----------|
| `nba_ml_common.py` | Feature contract, the heuristic-we-run-today, label policy, kafka produce. `%run`-included by all. | core |
| `nba_ml_simulate.py` | **Digital twin** — synthetic members + ground-truth response `f*` → `sim_examples`. | real |
| `nba_ml_build_training_set.py` | **Point-in-time** real examples: features@decision + terminal label → `training_examples`. | real |
| `nba_ml_train_initial.py` | **Replicate today**: heuristic → `@champion`; first XGBoost → `@challenger`. | real |
| `nba_ml_train.py` | **Nightly retrain** on real+sim → new `@challenger`. | real |
| `nba_ml_evaluate.py` | **Champion/challenger**: off-policy (IPS) + simulator → promote winner. | real (off-policy approximate until propensities logged) |
| `nba_ml_score.py` | **Nightly batch score** eligible set with `@champion` → emit `nba.score.*`. | real |
| `nba_ml_bandit.py` | **RL/online** exploration policy + off-policy value. | phase-2 scaffold |

## How the six requirements map

1. **Nightly batch score** — `nba_ml_score`. **Eligibility from Kafka** (compacted `nba.evaluations`, latest eval per member = the freshest current eligibility); features from `gold_member_snapshot` (UC); scores with `@champion`; **explores in the ML layer** (softmax-sample + boost the winner) and logs propensities; emits `nba.score.{action}.{channel}` (replay-safe `eventTs`, `kind=score`+`origin=lake`). Writes `predictions_log`.
2. **Initial train (replicate today)** — `nba_ml_train_initial` registers the current heuristic *as* `@champion` (a serveable pyfunc → zero behavior change at cutover) and the first learned model as `@challenger`.
3. **Nightly retrain** — `nba_ml_train` on `training_examples` (real, point-in-time, anchored on `DISPATCH`) + `sim_examples` (down-weighted) → new `@challenger`.
4. **RL / learns-as-it-comes** — `nba_ml_bandit`: contextual bandit; consumes logged (context, arm, propensity, reward) tuples, updates per-arm posteriors, anneals the scorer's exploration temperature. **No router change.**
5. **Champion/challenger** — MLflow UC aliases `@champion`/`@challenger`; `nba_ml_evaluate` gates promotion on off-policy IPS (real, using the logged propensity) or simulator value (bootstrap).
6. **Simulation engine** — `nba_ml_simulate`: a digital twin (member generator + ground-truth `f*` + policy rollout) for bootstrap data, fair offline eval, and exploration coverage. `f*` is independent of the trained model (no collapse).

## Exploration lives in the ML layer — the router stays a pure argmax

`nba_ml_score` owns exploration: per (member, channel) eligible pool it softmax-samples a winner and boosts it to
the top so the router's unchanged argmax picks it, and it **logs the propensity** of every candidate to
`predictions_log`. Off-policy IPS is then unbiased with **no router change** — training joins what actually
dispatched (`silver_activations`) to the scorer's logged propensity. `nba_ml_bandit` anneals the temperature
(in `bandit_policy`, read back by the scorer). The only bounded caveat: the scorer's pool can differ from the
router's by suppression/slot-occupancy — a small bias, closable by feeding the scorer `nba:suppressed` or a
1-line router propensity stamp.

## Labels: P(convert | delivered)

A valid label requires the episode reached **PRESENTED or beyond** (the member actually got the offer):
`HARD_COMPLETED`=1; reached PRESENTED/SOFT_COMPLETED/DECLINED but no convert =0; **never delivered = excluded**
(`IN_PROCESS→EXPIRED`, `FAILED`/bounced, `SUPPRESSED`, and `DEBOUNCED` — a streaming artifact). The model learns
propensity *given delivery*; deliverability is a separate concern. `build_training_set` aggregates each episode's
full state set (bounded by the next send) and applies the PRESENTED floor.

## Tables produced (in `nba_ml.core`)

`sim_examples`, `training_examples`, `predictions_log`, `bandit_policy`, + the MLflow UC model `nba_ml.core.nba_propensity` (`@champion`/`@challenger`).

## Run (cost-aware — serverless, spins up + stops)

```
# set the ML workspace host in databricks.yml (target: ml), then:
export BUNDLE_VAR_kafka_bootstrap=<tunnel-endpoint>
export BUNDLE_VAR_kafka_sasl_user=...   BUNDLE_VAR_kafka_sasl_pass=...
databricks bundle deploy -t ml
databricks bundle run -t ml nba_ml_bootstrap      # one-time: sim + heuristic@champion + model@challenger
databricks bundle run -t ml nba_ml_nightly        # build+sim → train → evaluate → score
```

The `nba_ml_nightly` / `nba_ml_score_intraday` / `nba_ml_bandit` jobs ship **PAUSED**; un-pause when ready. Each
run is minutes of serverless compute. The **propensity** scores ride Kafka (no standing endpoint there), so its cost
is just the runs — but the **RL policy IS served real-time** via the `nba-cql` model-serving endpoint for the
synchronous hot path (see *Offline-RL → Serving* below); that endpoint is the one always-on RL piece (currently
parked). See `../shutdown_minimal.py` to idle the workspace.

## Offline-RL journey optimization (sequence-aware next-best-action)

The propensity model above answers *"which arm converts THIS touch?"* — myopic. The RL layer answers *"which arm
maximizes the member's LONG-TERM journey value?"* It optimizes a **milestone ladder** (Activated +1 → Onboarded +3
→ Engaged +6 → Upgraded +15), where order matters: email is a vanity arm (high immediate response, ~no real
progress), push/sms drive task progress, voice is the deep arm (the only path to `usedChat`). A myopic policy chases
email and stalls at 0% Upgraded; the RL policy learns to sequence push→voice and reaches **77% Upgraded**.

**Algorithm — discrete CQL (Conservative Q-Learning), offline.** A Q-network is trained on a fixed logged dataset
(no live interaction) with the Bellman/TD update *plus* a conservative penalty (push Q down on out-of-distribution
actions) so the policy can't over-trust arms the data barely covers. Greedy-on-Q = the served policy.

**Serving (the real-time hot path).** The trained CQL Q-net is served as the **`nba-cql` Databricks model-serving
endpoint** under the `@champion` alias. The action-library's synchronous hot path scores against it (`scorer=dbx`,
`NBA_SERVING_URL`) — so it always serves the current champion with no local model to sync (the in-network
`nba-model` is the `scorer=local` alternative). Promotion is **self-correcting**: the retrain loop's
`nba_ml_rl_serve` step idempotently registers the new qnet (md5-gated), sets `@champion`, and re-points the endpoint
to the new version, so a promoted champion is served automatically. Re-create after a park with
`databricks bundle run -t ml nba_ml_rl_serve`.

**Grounded response head (model-based RL — the data flywheel).** The simulator's land-probability — "does this touch
land + advance the member?" — comes from a **pluggable response head**: the trained **propensity `@champion`** in
production, falling back to `f*` (the generative response model) when no champion exists, so the wiring is present
always and grounds itself as the propensity model is refit on real outcomes. The journey state's 8 features ARE the
propensity feature contract (`nba_ml_common.FEATURE_COLS` + `channel_idx`/`action_idx` → `P(convert|member,arm)`), so
the model drops in with **no adapter**. This is textbook model-based RL: the response model only describes how members
*react* — it is **independent of the policy**, so there is no policy→data→policy collapse — and it improves the
simulator each cycle (real data → fit response head → train policy → run → new real data). Rollouts are **batched**
(`collect_batch` — one response call per timestep across all worlds), parity-verified against the per-episode env.
Loading `@champion` needs **mlflow + scikit-learn**, which live in **serverless environment version 4** (the RL jobs
pin `client: "4"`); no pip install. Proven on serverless: `response_head = propensity@champion` → CQL **76% Upgraded
/ +13.04** vs myopic 0%. (Local cross-check with a controlled trained model — `nba_ml_rl_grounded_local.py` — recovers
`f*` to within ~0.03 per arm and the policy still beats myopic/BC.)

**Closing the loop on real data (the full flywheel).** Four pieces turn "proven in simulation" into "grounded in reality":
1. **Journey reconstruction** (`nba_ml_journey.py` + `nba_ml_build_journey_set.py`) — reads real member sends
   (`silver_activations` DISPATCH) + point-in-time `silver_snapshots` and reconstructs each member's ordered journey
   into the SAME `(state, action, reward, next_state, done)` transitions the CQL trains on (milestones computed from
   the feature snapshots; reward = milestone value gained − send cost). Reconstruction is **proven exact** (0.000000
   diff vs the env over 30k transitions). Ran on the real lake: **106,526 real transitions across 3,026 members** →
   `journey_transitions`. `nba_ml_rl_train` **unions** these with the simulator (sim gives coverage, real gives
   grounding); empty table → graceful sim-only. As real sends accrue, the policy trains on more real data with no code change.
2. **Grounded transition dynamics** (`nba_ml_dynamics_numpy.py` + `nba_ml_fit_dynamics.py`) — a serverless-native
   learned world model `(s,a)→Δstate` fit on `journey_transitions` (reward stays **structural** — the milestone
   ladder is a business definition, not something to hallucinate). Validated: one-step next-state MSE 0.31 and it
   **preserves the value ordering** (myopic −0.57 vs sequencer 15.2, matching truth) — valid for OPE/ranking.
3. **Off-policy evaluation** (`nba_ml_ope.py` + `nba_ml_run_ope.py`) — **FQE** (Fitted Q Evaluation) + **model-based
   OPE** estimate a policy's value from logged journeys *without deploying it*. Proven to recover the truth: CQL
   FQE **18.4** / model-based 18.7 / true 17.2; myopic −1.6 / −0.4 / −0.5 — all three agree CQL ≫ myopic. This is the
   gate you run before promoting/A-B-ing a policy.
4. **Live A/B harness** (`nba_ml_ab.py`) — stateless stable-hash assignment (RL vs control) + a two-proportion z-test
   on real Upgraded-rate. The serving scorer reads `assign_variant(memberId)`; a holdout runs while the policy is live
   so **real outcomes, not a simulator estimate, decide**. Tested: balanced split, significant-lift detection (z=72).

**Serverless-native, ZERO install.** CQL is hand-written in **pure numpy** (`nba_ml_cql_numpy.py` — forward +
manual backprop + Adam + gradient-clip + polyak target). torch is *not* in the serverless base and pip-installing
it (or d3rlpy) trips the immutable-package constraints; numpy is in every base. The 13→256→256→5 net is tiny, so
numpy is exact and fast enough. Validated to reproduce the reference: **d3rlpy → torch → numpy all land ~76–77%
Upgraded** (local cross-checks: `nba_ml_rl_train_local.py` d3rlpy, `nba_ml_rl_torch_local.py` torch,
`nba_ml_rl_numpy_local.py` numpy = the production engine).

| Notebook | Role |
|----------|------|
| `nba_journey_env.py` | The journey **MDP** (numpy): state = 8 features + 4 milestone flags + step; arms = 4 demo channels + NOOP; reward = milestone value gained. Ground-truth world the data is logged from + OPE rolls through. `%run`-loaded + importable. |
| `nba_ml_cql_numpy.py` | **Pure-numpy CQL + BC** (the engine). `%run`-loaded by the trainer. |
| `nba_ml_rl_train.py` | **Production train** (serverless): mixed-quality trajectories → BC + CQL → OPE → export numpy Q-net to the UC volume + `rl_card` to Delta. |
| `nba_ml_rl_promote.py` | **Evaluate-gate + promote**: reads `rl_card`, copies the new Q-net over live `rl_qnet.json` *only if* it clears the bar AND beats myopic. |
| `nba_ml_score_rl.py` | **Live RL scorer**: streams `nba.evaluations`, runs the CQL critic (numpy Q-net, reloaded each drain), emits `Q(s,a)` per eligible arm. Router argmax over Q = the next-best-action. |

**The loop (train → evaluate → promote, no restart):** `nba_ml_rl_train` writes `rl_qnet_dbx.json` + `rl_card`;
`nba_ml_rl_promote` gates and, on pass, copies it to `rl_qnet.json`; the scorer reloads `rl_qnet.json` on its next
drain → live. A failing challenger leaves the champion untouched. Run / schedule:

```
databricks bundle run -t ml nba_ml_rl_train        # one smooth process: train -> gate -> promote
# nba_ml_rl_retrain ships PAUSED (daily 02:00 UTC; 80k-step numpy CQL ≈ 15-20 min serverless) — unpause for auto-improve
```

Proven: CQL **+13.07 return / 77% Upgraded** vs myopic **−0.58 / 0%**, end-to-end on serverless with no install.

## Open decisions (flagged for you)

- **Eligibility source**: live engine (`silver_eval_eligible`, current default) vs. recompute in-lake from `dim_definitions` (fully decouples the nightly batch; port `rulesql.js`).
- **Cutover**: when `@challenger` consistently beats `@champion`, retire the Java stub scorer (and then `nba.facts`).
- **`nba_ml_common` sharing**: `%run` today; package as a wheel when it grows.
