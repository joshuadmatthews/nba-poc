<#
.SYNOPSIS
  Build + (re)run the LOCAL inbound member simulator (POC container — safe to recreate).
  Warm members (a SOFT_COMPLETED actionstate in their live snapshot) proactively come inbound and complete the
  action through the REAL inbound APIs on nba-action-library (serve -> disposition -> completion). No tunnel:
  it reads warmth from nba-redis and calls the local API directly.
#>
[CmdletBinding()]
param([switch]$Build)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-inbound-sim'
$IMG  = 'localhost/ais-nba-inbound-sim:latest'

# tiny image — always build (no -Build footgun); the switch is kept for call-site symmetry with siblings.
podman build -t $IMG -f (Join-Path $here 'Containerfile') $here

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-inbound-sim',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=17',   # after action-library (wave 16); loop self-heals if the API lags
  '-e', 'NBA_API_BASE=http://nba-action-library:7001',
  '-e', 'NBA_REDIS_HOST=nba-redis',
  '-e', 'INBOUND_RATE=0.25',
  '-e', 'HARD_FRACTION=0.4',          # share of inbound visits that finish the goal (hard); the rest re-engage (soft)
  '-e', 'COLD_RATE=0.015',            # baseline: non-warm members who spontaneously show up inbound (warmth = a lift, not a gate)
  '-e', 'TOPIC_RATE=0.3',             # fraction of visits carrying a topic (facts) -> hot path/gold read; rest serve cached
  '-e', 'LOOP_SECONDS=45',
  '-e', 'NBA_SCORER=local',          # disposition scoring via in-network nba-model (fast/free); set dbx to exercise the live hot path

  $IMG
)
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
