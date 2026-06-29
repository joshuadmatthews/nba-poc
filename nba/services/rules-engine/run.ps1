<#
.SYNOPSIS
  Build + (re)run the NBA Rules Engine (Drools). POC container — safe to recreate.
  -Build rebuilds the image. Consumes nba.definitions (+ nba.snapshots in step 2).
#>
[CmdletBinding()]
param([switch]$Build, [ValidateSet('embedded','kie')][string]$Mode = 'embedded', [int]$ScoreTtlSeconds = 0)
# -ScoreTtlSeconds: expire nba.score.* facts older than this in the eval (0 = carry-forever). See SNAPSHOT.md.
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-rules-engine'
$IMG  = 'localhost/ais-nba-rules-engine:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-rules-engine',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=17',
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', 'NBA_REDIS_HOST=nba-redis',
  # embedded (default) = inline KieSession; kie = offload the Drools eval to ais-nba-kie-server:7010
  '-e', "NBA_RULES_MODE=$Mode",
  '-e', 'NBA_KIE_URL=http://nba-kie-server:7010',
  # DEV/POC: channel-saturation (THROTTLE_HOT) self-expires after 5 min instead of "until midnight" (the
  # prod default, value 0). Keeps the saturation test from poisoning email for the rest of the day, and makes
  # a rules-engine restart ignore stale same-day saturation events replayed off the compacted definitions topic.
  '-e', 'NBA_THROTTLE_HOT_TTL_SECONDS=300',
  # SCORE TTL: treat an nba.score.* fact older than this as absent at eval time (router won't act on a stale
  # score; the eligible-but-unscored eval re-triggers the scorer). 0 = carry-forever (default). See SNAPSHOT.md.
  '-e', "NBA_SCORE_TTL_SECONDS=$ScoreTtlSeconds",
  $IMG
)
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
