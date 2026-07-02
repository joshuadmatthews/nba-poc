# Inbound journey — served actions ride the standard lifecycle state machine

**Status: implemented.** An inbound serve (the member came to a surface and `GET /next-action` presented an
action) now walks the **same** lifecycle as an outbound send — the standard `ChannelActionWorkflow`
(classic/KStreams) / `StateMachineFn` (Flink), keyed exactly like any outbound: `(nbaId, actionId, channel)`.
There is **no separate inbound state machine** — inbound is just an entry point into the one journey, so it
gets the full state model, disposition walk, soft/hard completion, **and TTL expiration** for free.

## How it works
```
GET /next-action ──(response returns immediately — hot path untouched)──▶ member sees the action
      │
      ├─ hot-patch: hset nba:snapshot fact:nba.actionstate.{action}.{ch} = IN_PROCESS   (UI reads it NOW)
      └─ emit kind=router CREATE {preDispatched:true, trackingId:"nba-ca:{nbaId}:{aid}:{ch}|{corr}"}
              │
              ▼ (async, to the side)
      state-machine bridge starts the STANDARD workflow  nba-ca:{nbaId}:{aid}:{ch}
              │  preDispatched -> skips debounce/throttle/DISPATCH (the member was already presented the action)
              ▼
        IN_PROCESS ──[dispositions]──▶ PRESENTED ──▶ SOFT_COMPLETED ──▶ HARD_COMPLETED
              │                                                            (goal, via the rules engine + router bridge)
              └────────────── TTL, no hard completion ──────────────▶ EXPIRED

POST /dispositions {entityId, actionId, channel, status, correlationId}
      ├─ outbox -> member.facts (kind=disposition, value = RAW status, trackingId routes to the workflow)
      └─ hot-patch: hset the disposition fact into the snapshot (UI immediacy; LWW reconciles)
```

## Design principles
1. **The action API emits router + member facts; the state machine runs to the side.** `nextAction` emits a
   `kind=router` CREATE with `preDispatched:true` (`ActionLibrary.startInboundJourney`) — fire-and-forget,
   *after* the response is sent, so the serve's latency is untouched. `POST /dispositions` emits the member
   fact via the transactional outbox, now carrying the `trackingId` that routes it to the journey workflow.
2. **`preDispatched` reuses the batch machinery.** The workflow's existing pre-dispatched path
   (`ChannelActionWorkflowImpl`: emit `IN_PROCESS` + track dispositions; Flink `StateMachineFn` CREATE with
   `preDispatched` → TRACKING) skips debounce/throttle/dispatch — correct for inbound, where presentation
   already happened at the API. Nothing else in the flow changes.
3. **The sender reports RAW; the state machine classifies.** Inbound dispositions carry the raw funnel status
   (`Presented`/`Accepted`/`Completed`); `DispositionClassifier` (identical in the Temporal worker and the
   Flink engine — pinned by twin unit tests) maps raw → canonical (`PRESENTED`; `Bounced…`→`FAILED`;
   `STOP…`→`DECLINED`; `Cancelled`→`SUPPRESSED`, `AlreadySent`→`SUPPRESS_FAILED`). Soft/hard completion stay
   with the rules engine (off the raw value + the goal), bridged in by the router — same as outbound.
4. **Expiration is the standard TTL.** A served-but-never-completed inbound reaches `EXPIRED` exactly like an
   undispositioned outbound — `ttlSeconds` rides the served action (from the eval) onto the CREATE.
5. **The hot path is unaffected; the UI doesn't wait.** The workflow is async (fact → bridge → workflow →
   state fact → snapshot, seconds), so the API optimistically hot-patches `nba.actionstate.*` = `IN_PROCESS`
   on serve and the disposition fact on `POST /dispositions` straight into the Redis snapshot — the very next
   read shows them. The workflow's authoritative state facts reconcile via event-time LWW (the same
   optimistic-write + durable-reconcile split as the existing fast-path write-through). Skipped when
   `NBA_SNAPSHOT_SOURCE=kstreams` (the snapshot isn't Redis-authored there), like every other write-through.

## Semantics + edge cases
- **Dedup with an in-flight outbound.** Same key ⇒ same deterministic workflowId (`nba-ca:{nbaId}:{aid}:{ch}`).
  If an outbound journey is already live on that action-channel, the inbound CREATE attaches/no-ops
  (`USE_EXISTING`; Flink: `phase != DONE` dedup) — one journey per (member, action, channel), never two. The
  serve only emits for a **freshly-eligible** top action (`state == "eligible"`), not `active`/`completed` ones.
- **`nba.actionstate.{action}.{channel}`** is the SAME namespace outbound uses — intentionally: it IS the same
  journey, so the rules engine's `active`/`workflowState` enrichment, the timeline, and the eval flags all work
  on inbound instances with zero new plumbing (this supersedes the earlier draft's separate `nba.inboundstate.*`
  idea — a distinct namespace made sense for a *parallel* tracker, not for the unified journey).
- **Which serves start a journey:** the TOP served action per serve (the same one the `INBOUND_SERVE` tracking
  event records as the attempt). The `INBOUND_SERVE`/`INBOUND_DISPOSITION` analytics events on `nba.activations`
  are unchanged and continue to carry the serve→disposition correlation for the lake/timeline.
- **Cross-flavor equivalence:** the Flink engine mirrors the pre-dispatched path natively (same CREATE fact),
  covered by `StateMachineFnTest.preDispatchedInbound*`.
