# 11 · ML Pipeline — Personalized Journeys

The ML layer decides the **next-best action per member**: which action, on which channel, right now — to maximize each
member's climb up the milestone ladder (**Reached → Registered → Assessed → Engaged in Care → STARS Compliant**). It
learns in two phases: **recreate** the hand-crafted marketing playbook, then **optimize** it per member by experimenting
on the live population. Implementation lives in [`../databricks/ml/`](../databricks/ml/README.md); this is the map.

> **If you read one thing, read "The fact taxonomy" — it's the whole mental model.**

## Recreate, then optimize (the core idea)

1. **Recreate (baseline).** Hand-crafted "playbook" journeys (5 of them) drive an initial population. The model trains on
   the reconstructed journeys and learns to **reproduce** them — `cql_match 0.81` (its greedy action matches the
   playbook's 81% of the time). This is the safe starting policy: it does what the marketers would have done.
2. **Optimize (personalize).** New members arrive with varied, realistic profiles. The model scores them — starting from
   the recreated baseline, but **diverging** as their facts and responses reveal what works for *them*. It experiments,
   observes outcomes, and learns which **action, order, cadence, and channel** best drive milestone value per profile.

The historic playbook facts don't change how the playbook plays out (it's a fixed recipe). Personalization is **learned
forward**, through experimentation on new members. **One model, full fact set:** it recreates when facts are
absent/baseline and personalizes as the profile fills in — that *is* "the model getting smarter over time."

## The fact taxonomy (the key mental model)

Facts split into two kinds, and the distinction is everything:

| | **Eligibility facts** (progress / state) | **Decision facts** (member attributes) |
|---|---|---|
| **Examples** | respondedToOutreach · registeredForPortal · hraCompleted · pcpSelected · awvCompleted … | riskScore · diabetic · comorbidityCount · erVisits12mo · rxAdherencePDC · openCareGaps · age · planDSNP · sdohBarrier · portalLogins30d · pagesViewed30d · avgTimeOnPageSec … |
| **Who uses them** | the **rules-engine** — they gate *what's available* (the legal next steps) | the **model** — it reasons over them to pick *which* available action/channel fits this member |
| **Change** | flip as the member progresses (point-in-time) | profile attributes gathered from source systems; **emit over time** |

The rules-engine resolves *eligibility* (the journey's legal order) from the progress facts. The model's job is what's
left: given what's available, pick the action/channel that fits **who this member is** — from the decision facts. A
system with only eligibility facts has nothing for the model to decide; the decision facts are where personalization lives.

## The state the model sees

`obs = [decision facts | progress facts | per-channel dispositions | milestone flags | step]` (~50 dims). Decision facts
personalize the **action**; the per-channel dispositions (a member's open/click/answer history) personalize the
**channel**; the milestones + step give the long-game context.

## Data architecture — facts come from source systems → bronze → gold

How a real NBA gets its data, mirrored exactly by the test:

```mermaid
flowchart LR
  SRC["source systems<br/>claims · portal · EHR<br/>(simulated in the test)"] -->|Kafka| BRONZE[("bronze ingress<br/>datalake.streaming-inbound")]
  BRONZE --> SILVER[(silver)] --> GOLD[("gold_member_snapshot<br/>the member's full profile")]
  GOLD --> SCORER["CQL scorer · score-rl<br/>reads GOLD → Q per arm"]
  SCORER -->|nba.score.*| ROUTER["action-router<br/>argmax over scores + states"]
  ROUTER --> SM["state machine (Temporal)<br/>debounce · throttle · DISPATCH"]
  SM --> AL["action-layer<br/>DISPOSITIONS ONLY"]
  AL -->|deliver / open / click| BRONZE
  GOLD --> RECON[reconstruct journeys] --> TRAIN[CQL retrain] -.rl_qnet.json.-> SCORER
```

- **Member facts (attributes AND outcomes) originate from source systems → the bronze-ingress topic**
  (`datalake.streaming-inbound`, generic `{entityId,key,value}` passthrough) → silver → gold. Nothing else owns them.
- **The model reads GOLD** (`gold_member_snapshot`) — the full profile. It does **not** read the rules snapshot.
- **The router uses scores + action-states** — it never sees the decision facts (they only influence the *score*).
- **The rules snapshot (Redis) is LEAN** — eligibility facts only. Keep it that way.
- **The action-layer is DISPOSITIONS-ONLY** — it walks the channel delivery funnel (delivered/opened/clicked/answered)
  and emits dispositions. It never reads or emits member facts.

## The source-system simulator (the test's ground truth)

In production, real source systems report what members *do*. In the test, [`nba_source_sim.py`](../databricks/nba_source_sim.py)
plays them — for **OUTBOUND response only** (the inbound path moved to its own local client; see below). It consumes the
dispatched action, reads the member's facts from gold, applies the **ground-truth response
model** — `convert = sigmoid(f*(facts)) × channel_match(facts)` — and when the member converts, drops onto the bronze ingress:
- the **completion signal** that drives the action's **HARD COMPLETION** (the member did the objective — a
  `nba.completion.{action}` event / the underlying claims-or-portal fact). The **milestone is then DERIVED from the
  action's hard-completion transition** — the simulator does **not** emit the milestone rule facts
  (`respondedToOutreach`/`hraCompleted`/…) directly; those belong to the system, driven by completions, exactly as in
  production (a claims feed reports "a1c lab resulted" → `a1c_test` hard-completes → *that* sets the STARS gap milestone).
- **fresh engagement telemetry** (logins/pages tick up, recency resets) — facts only ML cares about, *emitting over time*.

The response model is **hidden from the NBA model** — it must *learn* it from the outcomes. It encodes the personalization truth:
- **fact → action:** a1c only converts for **diabetics**; med-adherence for diabetic/low-PDC; care-manager for
  high-risk/high-ER/**SDOH**; wellness for low-risk/young; reengage for **lapsed** (high daysSinceLogin).
- **fact → channel:** digitally-engaged members convert on **email/push**; older/low-digital on **voice**.

Shift the response model (`f*`) and the next retrain tracks it — that is the adaptation.

**Warm-lift (stays in the source-sim).** A `SOFT_COMPLETED` outbound touch raises the member's conversion probability on
their next touch (the `warm` param of `convert_prob`) — so the model can learn "soft-complete → higher completion."

## The inbound simulator — a real client, not a shortcut (`nba-inbound-sim`)

A real inbound member is an external **CLIENT**, and the test now models it as one. The source-sim's old direct-fact
inbound shortcut (`generate_inbound`, which wrote a completion fact straight onto the bus) is **REMOVED**. In its place a
small **LOCAL container** — `nba-inbound-sim` (image `localhost/ais-nba-inbound-sim`, on `aiservices_default`) — reads
**warm members** (a current `SOFT_COMPLETED` actionstate in their live `nba:snapshot`) from nba-redis and drives the
**real inbound APIs** on the action-library: serve (`GET /next-action`) → disposition (`POST /disposition`) → completion
(`POST /completion`). Those completions flow the **same proven path** every inbound disposition does (outbox →
`nba.member.facts` → snapshot-builder + the datalake) — so the lake carries the **soft-complete → inbound-completion**
journey and the model learns to surface that action **inbound**, not just outbound.

Behavior knobs (env): soft-engage mostly + **hard-complete sometimes** (`HARD_FRACTION≈0.4`); warm members re-engage
across visits; a **cold baseline** (`COLD_RATE≈0.015`) of non-warm members spontaneously show up too — so warmth is a
**LIFT** on inbound completion, not a gate; some visits carry a "call topic" (`facts` → the serve **HOT-PATHS**), the rest
serve cached (`TOPIC_RATE≈0.3`); `NBA_SCORER=local` (the in-network nba-model) keeps its disposition scoring robust under load.

## The model + the objective

A **Conservative Q-Learning** policy (offline RL, **pure numpy** — torch/d3rlpy can't install on serverless). It learns
`Q(state, arm)`, `arm = (action, channel)`, optimizing **long-term milestone VALUE** (reward = milestone progression
toward STARS), not immediate clicks. That objective gives you **order and cadence for free**: it sequences (picks the
highest-value *eligible* action) and paces (a `hold` arm + over-contact→opt-out means badgering forfeits future value).
The critical learner fix from the start-up era still holds: **obs normalization** (mean/std ride in `rl_qnet.json`) —
without it the raw-scale facts drown the 0–1 flags and the policy collapses to spam.

## The hot-path scorer + self-correcting champion promotion

The hot path scores one of two ways:
- **`scorer=dbx`** — the Databricks **nba-cql serving endpoint** (`NBA_SERVING_URL`). It always serves the `@champion`
  alias, so the current champion is auto-served — there is **no local model to sync**.
- **`scorer=local`** — the in-network **nba-model** service (lower latency).

Promotion is **self-correcting**. The ML retrain loop includes an `rl_serve` step that idempotently **registers the new
qnet** (md5-gated, so a no-op when unchanged), **sets the `@champion` alias** to it, and **re-points the serving
endpoint**. So a promoted champion is served automatically — no manual re-point, no model file to push to the scorer.

## The retrain flywheel (the engine that makes "over time" real)

The model only "finds what works" because it runs a loop:
1. **Reconstruct** the accumulating journeys — `nba_ml_build_journey_set`, **pulling decision facts from gold** (the same
   profile the scorer reads; the lean snapshot doesn't carry them).
2. **Retrain** the CQL on the real outcomes its own decisions produced.
3. **Batch-score** every member with the new model (`nba_ml_score_batch`) — the new policy reaches everyone, not just
   members with fresh live activity. **This batch-after-promotion is the tick** that keeps the flow turning without
   needing to inject facts.

This is also how **new actions** integrate: an action enters the catalog (action-library → `nba.definitions` → the
model's arms), warm-starts from its channel sibling, gets tested by exploration, and the next retrain learns its value.

## Reference policies during data generation

- **Playbook** (`nba_ml_score_playbook`) — prescribes the hand-crafted journey's next step. The recreate baseline + the
  source of the historic journeys.
- **Explorer** (`nba_ml_score_explore`) — uniform-random over eligible arms; the experimentation that reveals what works
  per profile (the optimize signal). New members get explored.
- **RL scorer** (`nba_ml_score_rl`) — the trained CQL policy; the production scorer. Reads `rl_qnet.json`, hot-reloads each
  drain so a fresh retrain auto-deploys.

## Channel-specific dispositions (the action-state signal)

A **disposition** is a channel-specific engagement event the action-layer emits (`nba.disposition.{action}.{channel}`):
`email Delivered→Opened→LinkClicked · sms Delivered→LinkClicked · push Delivered→Opened · voice Answered→Completed`;
opt-outs `Unsubscribe/STOP/Declined/Dismissed`. They (a) drive the [state machine](03-state-machine.md) soft/hard
completion, and (b) are **ML features** — the model learns each one's effect, and the scorer feeds the latest live
disposition per channel into the obs, so a channel a member engaged on rises on their next decision.

## Compliance is a hard floor

Consent (`smsConsent`), DNC (`isDNC`), and opt-out (Unsubscribe/STOP latch a durable per-channel opt-out in Redis →
that channel goes permanently ineligible) gate **eligibility** — personalization always operates inside the rules. The
model can't choose an action it isn't allowed to send.

## Proving it — the tests that matter

The goal is a real ML layer, **proven with tests**, not a demo:

| Claim | Test | Status |
|---|---|---|
| **Recreate** the playbook | `cql_match` (greedy == logged playbook action), in `nba_ml_rl_train` | ✅ **0.81** (was 0.27 on hollow data) |
| **Channel routing** | `route_to_engaged` — positive disposition on a channel → policy routes there | built into the train eval |
| **Per-member personalization** | distinct profiles → distinct preferred actions (`diabetic_low_adherence→med_adherence`, `high_risk_complex_sdoh→care_manager`, `low_risk_young→wellness`, `lapsed→reengage`) | built into the train eval; proving as the profiled population climbs |
| **Ground-truth fact-dependence** | outcomes concentrate on the right profiles (diabetic among a1c/med converters ≫ population; high-risk among care converters) | verifiable on the lake |
| Disposition / normalization / adaptation mechanics | `nba_ml_disposition_tests.py`, `_ground_test.py`, `nba_ml_test_adaptation.py` | ✅ (channel + state mechanics) |

## The two streaming bridges (both ride the external tunnel; both can silently break)

1. **Datalake stream** (`nba_datalake_stream`) — host Kafka → `silver_*` → gold. Dies → the lake freezes, retrains see
   stale data. Health: silver `max(eventTs)` ≈ now.
2. **CQL scoring stream** (`nba_ml_score_rl`) — `nba.evaluations` → `nba.score.*`. Dies → NBA decisions stop. Crashed once
   on an empty Kafka `bootstrap` — falls back to the `nba-kafka` secret scope (`kafka_cfg`) now, so a param-less deploy
   self-heals. `nba_ml_health_check.py` asserts both (silver fresh · scorer running · bootstrap non-empty).

## Cost

Serverless jobs only — each spins up, runs minutes, stops. The hot-path scorer can ride a **standing model-serving
endpoint** (the nba-cql endpoint, `scorer=dbx`) or the in-network nba-model (`scorer=local`, no Databricks cost). The
other things that run continuously while live: the datalake stream, the batch-score tick, and the source-system sim — all small.

### Databricks parked (minimum spend) — operational note

As of this session, all Databricks compute is **parked** for minimum spend:
- SQL warehouse **STOPPED** (auto-starts on the first gold query).
- Lakebase instance **STOPPED** (parked; data kept).
- Custom serving endpoints (`nba-cql`, `nba-propensity`) **DELETED**.
- Retrain schedules (`nba-ml-rl-retrain`, `nba-ml-retrain-loop`) **PAUSED**; source-sim run cancelled.
- Foundation-model APIs (`databricks-*`) left running (pay-per-token, no idle cost).

**To resume:** un-park Lakebase → run `nba-ml-rl-serve` (re-creates `nba-cql` + the `@champion` alias) → un-pause the
retrains. The warehouse auto-starts on the first gold query.

## Implementation map

- **State / facts:** `nba_ml_common.py` (`FEATURE_KEYS` = the two fact layers), `nba_journey_env.py` (`FEATURE_COLS`,
  milestones, dispositions).
- **Data gen / ground truth:** `nba_source_sim.py` (source-system sim), `infra/seed-healthcare-nba.py` (fact-driven
  actions + milestones), `nba_seed_healthcare_members.py` (correlated realistic population).
- **Reconstruct + train:** `nba_ml_build_journey_set.py` (+ gold decision-fact overlay), `nba_ml_rl_train.py`,
  `nba_ml_cql_numpy.py`.
- **Serve:** `nba_ml_score_rl.py` (CQL), `nba_ml_score_playbook.py` (recreate), `nba_ml_score_explore.py` (experiment),
  `nba_ml_score_batch.py` (the retrain tick).

## Current state + open items

- ✅ **Recreate proven** — `cql_match 0.81` on 759k real reconstructed transitions.
- ✅ **Source-system architecture live** — facts (attributes + outcomes) flow from the sim → bronze → gold; the model
  reads gold; the router uses scores+states; the snapshot is lean; the action-layer is dispositions-only.
- ✅ **Rich decision-fact model** — profile + claims + engagement telemetry in the state; correlated realistic population.
- ▶️ **Personalization proof in progress** — the whole population now carries the rich profile and is climbing with
  fact-dependent outcomes; reconstruct (gold overlay) → retrain → the full-personalization eval closes it out.
- ▶️ **Make the flywheel continuous** — schedule reconstruct → retrain → batch-score on a cadence (the pieces are wired).
