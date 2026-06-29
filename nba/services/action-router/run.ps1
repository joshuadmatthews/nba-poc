<#
.SYNOPSIS
  Build + (re)run the NBA Action Router. POC container — safe to recreate.
  -Build rebuilds the image. Two listeners: hash-owner (nba.evaluations -> Redis
  nba:eval:hash) and activation-emitter (nba.snapshots -> CREATE on nba.activations).
#>
[CmdletBinding()]
param([switch]$Build, [string]$FaultInject)   # -FaultInject <substring>: DLQ test hook, any record containing it throws
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-action-router'
$IMG  = 'localhost/ais-nba-action-router:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-action-router',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=19',
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', 'NBA_REDIS_HOST=nba-redis'
)
if ($FaultInject) { $ARGS += @('-e', "NBA_FAULT_INJECT=$FaultInject") }
$ARGS += $IMG
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
