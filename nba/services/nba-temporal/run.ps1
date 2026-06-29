<#
.SYNOPSIS
  Build + (re)run the NBA Temporal worker. POC container — safe to recreate.
  -Build rebuilds the image. -DebounceSeconds sets the settle window (prod = 60).
  Consumes nba.activations and drives ChannelAction workflows on ais-nba-temporal.
#>
[CmdletBinding()]
param([switch]$Build, [int]$DebounceSeconds = 60, [string]$FaultInject, [int]$Concurrency = 1)   # -FaultInject: DLQ test hook; -Concurrency: parallel workflow-starts (1 = legacy serial)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-temporal-worker'
$IMG  = 'localhost/ais-nba-temporal-worker:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-temporal-worker',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=20',
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', 'NBA_REDIS_HOST=nba-redis',
  '-e', 'NBA_TEMPORAL=nba-temporal:7233',
  '-e', "NBA_DEBOUNCE_SECONDS=$DebounceSeconds",
  '-e', "NBA_BRIDGE_CONCURRENCY=$Concurrency"
)
if ($FaultInject) { $ARGS += @('-e', "NBA_FAULT_INJECT=$FaultInject") }
$ARGS += $IMG
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
