<#
.SYNOPSIS
  Build + (re)run the NBA decision engine (Kafka Streams port of the snapshot stream-processor).
  POC container — safe to recreate. -Build rebuilds the image.

  -Mode shadow (default): consumes nba.member.facts in its OWN consumer group, emits to *.shadow topics,
     serves the snapshot via IQ on :7020 — drives NOTHING, so it runs safely ALONGSIDE snapshot-builder
     for diffing + latency measurement.
  -Mode authoritative: emits the REAL nba.snapshots/definitions/facts (the cutover) — STOP snapshot-builder
     first, and flip the readers (action-library NBA_SNAPSHOT_SOURCE=kstreams).
#>
[CmdletBinding()]
param([switch]$Build, [ValidateSet('shadow','authoritative')][string]$Mode = 'shadow',
      [string]$OffsetReset = 'latest')
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-decision-engine'
$IMG  = 'localhost/nba-decision-engine:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-decision-engine',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=16',
  '-p', '7020:7020',                                   # IQ snapshot endpoint, published for the latency harness
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', 'NBA_REDIS_HOST=nba-redis',                    # idmap resolver only (NOT the snapshot — that lives in state)
  '-e', "NBA_DECISION_ENGINE_MODE=$Mode",
  '-e', 'NBA_ENGINE_ADVERTISED=nba-decision-engine:7020',
  '-e', 'NBA_METRICS_PORT=9410',
  # shadow latency test: start at the live edge (latest) so we don't replay 2 days of history before measuring.
  # A real cutover wants 'earliest' to rebuild every member's snapshot into state (parity with snapshot-builder).
  '-e', "NBA_OFFSET_RESET=$OffsetReset",
  $IMG
)
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
