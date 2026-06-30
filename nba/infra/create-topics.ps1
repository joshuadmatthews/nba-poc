<#
.SYNOPSIS
  Create the NBA data-layer Kafka topics on ais-nba-redpanda (idempotent).
.DESCRIPTION
  All compacted (cleanup.policy=compact). The two FACT topics (nba.facts, nba.member.facts) are keyed by
  MEMBERID ("{entityType:entityId}") — NOT by fact key — so every fact for a member lands on the same
  partition: one snapshot-builder replica owns a member and never races another on Redis. Compaction then
  just bounds growth (latest message per member); these topics are TRANSPORT, not a per-fact store — the
  authoritative per-fact state lives in the Redis snapshot + the gold lake (and a cold rebuild re-emits from
  gold via nba_fact_reconcile, not by replaying the fact topics). The state topics keep their own keys:
  nba.snapshots by nbaId, nba.definitions by def/THROTTLE key, nba.activations by (nbaId,action,channel).

  SCALE-OUT: 1 partition / 1 replica today, but the keying is already correct — raising the partition count
  (rpk topic add-partitions) is the ONLY change needed to run multiple snapshot-builder replicas, because
  members already hash to stable partitions. No producer/consumer code change.
  Consumer DLQs are created per-consumer when those services are built.
#>
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'

$topics = @(
  # nba.facts RETIRED — was the all-facts ML feature firehose; ML reads features from Unity Catalog now,
  # nothing consumes it. Don't recreate it. (The lake no longer emits it; see nba_datalake_stream.emit_inbound.)
  'nba.member.facts',  # curated subset some action cares about; snapshot-builder input
  'nba.snapshots',     # snapshot-builder output: per-NBAID current state
  'nba.evaluations',   # rules-engine output: eligible channelActions[] per member (was relying on broker auto-create)
  'nba.activations',   # action-router output: CREATE/SUPPRESS per (member,action,channel)
  'nba.definitions'    # latest action/rule defs + THROTTLE:{channel} level (broadcast to every rules-engine instance)
)
foreach ($t in $topics) {
  podman exec ais-nba-redpanda rpk topic create $t -p 1 -r 1 -c cleanup.policy=compact 2>$null
}
# nba.definitions carries latest-per-key state (defs + the broadcast throttle level), so it MUST be
# compacted — keeps only the latest THROTTLE:{channel} so a fresh rules-engine pod replays the current
# level on init, and the high-frequency throttle stream never accumulates. Enforce if it pre-exists
# (auto-created topics default to cleanup.policy=delete).
podman exec ais-nba-redpanda rpk topic alter-config nba.definitions --set cleanup.policy=compact 2>$null
# nba.activations carries two op-families: the router's CREATE/SUPPRESS (-> temporal bridge) and
# the state machine's DISPATCH/CANCEL (-> action layer). The action layer's verdict comes back as
# a member fact (nba.disposition.* on nba.member.facts) which the state machine routes to the
# workflow. No separate requests/dispositions topics.

# Consumer DLQs — one per CONSISTENCY consumer (read-only/peek consumers like the BFF eventstream
# are intentionally excluded — they store nothing, so a poison record is just dropped). Named
# nba.dlq.{consumer} so the command-center DLQ panel tails them by prefix. DELETE policy (retain a
# week) so poison stays replayable; each record is an envelope {consumer, topic(source), partition,
# offset, key, value, headers, error, dlqTs} → replay re-produces the EXACT original to its SOURCE
# topic and idempotency (event-time LWW / Temporal workflow-id dedup) makes the replay safe.
$dlqs = @(
  'nba.dlq.snapshot-builder',      # snapshot-builder (nba.member.facts -> Redis snapshots)
  'nba.dlq.action-router',         # action-router (nba.evaluations -> CREATE/SUPPRESS)
  'nba.dlq.action-layer',          # action-layer (nba.activations DISPATCH/CANCEL)
  'nba.dlq.temporal-disposition',  # nba-temporal disposition consumer (nba.member.facts)
  'nba.dlq.temporal-bridge'        # nba-temporal activation bridge (nba.member.facts)
)
foreach ($t in $dlqs) {
  podman exec ais-nba-redpanda rpk topic create $t -p 1 -r 1 -c cleanup.policy=delete -c retention.ms=604800000 2>$null
}
Write-Host "--- NBA topics ---"
podman exec ais-nba-redpanda rpk topic list
