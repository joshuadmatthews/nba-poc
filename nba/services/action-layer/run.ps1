<#
.SYNOPSIS
  Build + (re)run the NBA unified action layer (stub). POC container — safe to recreate.
  -Build rebuilds the image. -SendDelayMs is the simulated send latency (the window in which
  a cancel can still win). Consumes nba.action.requests, emits nba.dispositions.
#>
[CmdletBinding()]
param([switch]$Build, [int]$SendDelayMs = 4000, [string]$FaultInject)   # -FaultInject <substring>: DLQ test hook
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-action-layer'
$IMG  = 'localhost/ais-nba-action-layer:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-action-layer',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=21',
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', "NBA_SEND_DELAY_MS=$SendDelayMs"
)
if ($FaultInject) { $ARGS += @('-e', "NBA_FAULT_INJECT=$FaultInject") }
$ARGS += $IMG
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
