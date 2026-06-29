<#
.SYNOPSIS
  Build + (re)run the NBA journey scorer — a Databricks-FREE async scorer for local end-to-end testing.
  Consumes nba.evaluations, emits scripted nba.score.{action}.{channel} facts. POC container, safe to recreate.

  Run it INSTEAD OF the Databricks RL scorer to let the whole flywheel score + complete with Databricks parked.
  -Build rebuilds the image.
#>
[CmdletBinding()]
param([switch]$Build, [string]$OffsetReset = 'latest')
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-journey-scorer'
$IMG  = 'localhost/nba-journey-scorer:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-journey-scorer',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=17',
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', "NBA_OFFSET_RESET=$OffsetReset",
  $IMG
)
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
