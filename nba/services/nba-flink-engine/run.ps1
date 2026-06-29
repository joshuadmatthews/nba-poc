<#
.SYNOPSIS
  Build + (re)run the NBA Flink engine — the whole NBA spine (snapshot/rules/score/route + the lifecycle state
  machine that replaces Temporal) as one embedded Apache Flink job. Third reference impl. Additive + mode-gated:
  shadow (default) writes .shadow topics and drives nothing; authoritative writes the real topics + Redis mirrors.
  POC container, safe to recreate.
#>
[CmdletBinding()]
param([switch]$Build, [ValidateSet('shadow','authoritative')][string]$Mode = 'shadow', [int]$Parallelism = 1)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-flink-engine'
$IMG  = 'localhost/nba-flink-engine:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

# Shadow mode reads .shadow sibling topics that don't exist until something writes them; the Flink KafkaSources
# fail (UnknownTopicOrPartition -> job failover loop) if a source topic is missing. Pre-create them (idempotent).
if ($Mode -eq 'shadow') {
  foreach ($t in @('nba.member.facts.shadow','nba.snapshots.shadow','nba.evaluations.shadow',
                   'nba.activations.shadow','nba.facts.shadow','nba.dlq.flink-engine.shadow')) {
    podman exec ais-nba-redpanda rpk topic create $t 2>$null | Out-Null
  }
}

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-flink-engine',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=16',
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', 'NBA_REDIS_HOST=nba-redis',
  '-e', "NBA_FLINK_MODE=$Mode",
  '-e', "NBA_FLINK_PARALLELISM=$Parallelism",
  $IMG
)
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
