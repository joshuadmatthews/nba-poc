# Hard & Soft Completion — design spec

Status: **Phase 1 (behavior) — in build.** Phase 2 (lake training-label table) deferred.

## Why

The NBA pipeline only knew **disposition** today (delivered / suppressed / failed) — that's *delivery*, not
*outcome*. The model has no supervised label, and an action keeps cycling on its TTL forever because
nothing represents "the member actually did the thing we wanted." We add two explicit, separate signals:

- **Soft completion** — the member *engaged* with a touch to the channel's bar (email Read/Clicked, voice
  Answered/Completed, …). Positive signal, but the goal isn't met. Does **not** retire the action; it
  governs the re-send cooldown and feeds the model an engagement feature.
- **Hard completion** — the member *did the thing in the source system* (CRM fact flips, attributable to
  an action we sent). **Retires** the action for that member (latched, permanent), and is the supervised
  outcome label for training.

## Locked decisions

- **Hard completion is action-level config over member facts.** An action carries a `completion` condition
  tree (same schema as `inclusion`/`exclusion`), evaluated per member. It can also be signalled directly by
  a `nba.completion.{actionId}` member fact (API or lake detector) — either path latches.
- **Permanent + default-exclude, via a checkbox.** First time completion is satisfied, it's **latched**
  permanently (`nba:completed:{nbaId}` HSETNX — the milestone pattern). The action carries
  `autoExcludeOnCompletion` (default **true**): when true the system injects the exclusion automatically.
  Turn it off and completion is still **tracked/latched** (for the label and for your own rules to
  reference) — only the *automatic* gate is removed. Authoring eligibility rules is always available
  regardless.
- **Hard completion retires the whole action**, every channel (member-level / channel-agnostic). Variants
  are A/B content under the action — they ride the label row, they don't change the exit grain.
- **Two TTLs, split by concern:**
  - **soft TTL** = outbound re-send cooldown. **Channel-default, action-override, hidden.** (This is the
    existing per-channel `ttlSeconds` → renamed `softTtlSeconds`; same behavior.)
  - **hard TTL** = completion-wait / attribution window — how long the state machine stays open watching for
    completion before `expired`. **Authored at the action level** (`hardTtlSeconds`). Measured last-touch.
- **Soft completion criteria** = the channel's funnel terminal stage by default (`CHANNEL_FUNNEL`),
  per-action override, hidden unless expanded.
- **Explicit states everywhere** (see table) and the **rules engine evaluates both** completions each pass.

## Hard completion lives in the eligibility layer (milestone-sibling), NOT the workflow

Hard completion is **exactly the milestone pattern, scoped per-action and wired to exclusion** — a
rules-engine/eligibility concept, not a Temporal state. The rules engine already latches milestones
(`HSETNX nba:milestones:{nbaId}`) and rides them on every eval; hard completion is its sibling
(`HSETNX nba:completed:{nbaId}[actionId]`) that *also* excludes the action. So the Temporal workflow is
**untouched in Phase 1** — a completed action flips to `eligible: false` and drops off the eval once its
workflow reaches the terminal `HARD_COMPLETED` state, so the router + inbound serve never see it again
(retired, every channel). The durable, permanent record of completion is that terminal `HARD_COMPLETED`
workflow state itself (it rides up as a state transition into the lake and IS the supervised ML training label).

Completion is surfaced **explicitly on the eval** (so Command Center + the model see it), and the rules
engine evaluates **both** soft and hard each pass:

| Signal | Grain | How the rules engine decides it | Surfaced as | Effect |
|---|---|---|---|---|
| **hard completed** | **action** (all channels) | `completion` tree passes **or** `nba.completion.{actionId}` truthy → `HSETNX nba:completed` (permanent, milestone-style) | `hardCompleted` flag on the action's `channelActions[]` entry; the durable record is the terminal `HARD_COMPLETED` workflow state (rides up into the lake as the ML label) | **retire** via auto-exclude (when `autoExcludeOnCompletion != false`) |
| **soft completed** | **(action, channel)** | the disposition funnel fact reached the channel's bar (`CHANNEL_FUNNEL` terminal, or per-action override) | `softCompleted` on the ChannelAction | informational + engagement feature; re-send still guarded by the channel's `ttlSeconds` |

Existing Temporal `nba.actionstate.*` delivery lifecycle (`CREATED → IN_PROCESS → PRESENTED → … →
EXPIRED`) is unchanged by this spec. The dispatched touch + the per-channel TTL remain the soft re-send
cooldown.

## Inbound members reach completion through the real API

The outbound path latches hard completion off the `completion` tree (a source-system fact flips). An **inbound**
member — one who shows up on a channel rather than being dispatched to — reaches hard completion through the **real
`POST /completion` API**, which emits the `nba.completion.{actionId}` member fact (via the outbox) that the rules
engine then latches exactly as it would any other completion signal.

The inbound flow is **mostly SOFT** — an inbound contact is primarily a **disposition** (engagement on the served
action), and only **sometimes** does it carry through to **hard completion** (the member actually does the thing). So
inbound looks like: serve → disposition (soft engagement) → *sometimes* `POST /completion` (hard). Because that
completion rides the same outbox → `nba.member.facts` path as any disposition, the soft-engagement-then-completion
journey is visible in the lake and the model can learn to surface that action inbound.

## TTLs

- **soft TTL = the existing per-channel `ttlSeconds`** (re-send cooldown the workflow already sleeps). No
  wire change; Command Center relabels it "re-send after" and treats it as channel-default + action-override.
- **hard TTL = new action-level `hardTtlSeconds`** = the completion attribution window ("how long do we
  wait for hard"). Stored on the action now; **consumed in Phase 2** (the label join: a completion within
  `hardTtlSeconds` of a `sent` is *attributed*; later = organic). The permanent exclusion latch does not
  depend on it.

## Where each piece lives (Phase 1)

- **action-library** (`ActionLibrary.java`): action doc gains `completion` (tree), `hardTtlSeconds`, and
  `autoExcludeOnCompletion` (default true); per-channel `softCompletion` override optional. `collectFacts`
  also walks `completion`; upsert adds `nba.completion.{id}` to `factsUsed` so the explicit signal survives
  the lean snapshot. New `POST /completion {entityId, actionId, source?}` → emits
  `nba.completion.{actionId}=completed` member fact via the outbox (mirrors `/dispositions`). All doc-only
  fields → no DB migration. **This is the path an INBOUND member takes to hard completion** — see below.
- **rules-engine** (`RulesEngine.java`): each eval — latch hard completion (`completion` tree passes OR
  `nba.completion.{actionId}` truthy) into `nba:completed:{nbaId}` (HSETNX, beside the milestone latch);
  derive soft completion per (action,channel) from the disposition funnel facts; **auto-exclude** completed
  actions from `hits` when `autoExcludeOnCompletion != false` (post-eligibility filter, like the
  `CHANNEL_HOT` skip); surface `softCompleted`/`hardCompleted` on each `channelActions[]` entry (the
  `hardCompleted` flag added to `fullSig` so a fresh completion emits) — there is no separate `completed[]` block.
  It also computes, per eval, the **transition arrays** — `newCompleted[]` (actions whose completion criterion
  just became true: `byCriterion && !already-signalled`) and `newMilestones[]` (milestone trees that just
  passed: `treePass && fact-absent`) — and hands them to the action-router on the eval message; the rules-engine
  **no longer publishes the durable facts itself**.
- **action-router**: owns the **publish** of the durable `nba.completion.{actionId}` / `nba.milestone.{id}`
  facts, straight from those transition arrays (no diffing, no redundant eligibility-cache read). It strips the
  transient `newCompleted`/`newMilestones` before persisting the single eligibility object (`nba:eligibility:{nbaId}`);
  the perpetual `completed[]`/`milestones[]` stay on the object. (Was: the rules-engine emitted these.)
- **snapshot-builder**: no code change — completion facts ride because action-library puts them in
  `factsUsed` → `nba:rulefacts`, which the snapshot lean-filter already honors.
- **nba-temporal**: **no change in Phase 1.**
- **command-center**: author the hard goal (`completion` tree) + `autoExcludeOnCompletion` checkbox +
  `hardTtlSeconds`; soft criteria + re-send TTL behind a hidden per-channel override. BFF surfaces
  the per-entry `softCompleted`/`hardCompleted` flags on `channelActions[]` (no `completed[]` block).

## Phase 2 (deferred)

Lake goal-detector job (watch the goal fact in gold, emit `nba.completion.*` with attribution) + the
training-label table: `silver_snapshots ⋈ disposition(sent) ⋈ completion-within-hardTTL` →
`gold_action_completion` (features-at-send-time, action, channel, variant → converted?, time-to-convert).
