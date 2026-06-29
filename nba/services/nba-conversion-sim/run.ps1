<#
.SYNOPSIS
  Build + (re)run the NBA conversion sim — Databricks-free outbound hard-completion simulator. Watches delivered
  actions on nba.member.facts and finishes the GOAL (POST /completion) for a fraction of (member,action) pairs,
  so outbound members reach HARD_COMPLETED locally instead of only EXPIRING. POC container, safe to recreate.
#>
[CmdletBinding()]
param([switch]$Build, [double]$HardFraction = 0.4, [string]$OffsetReset = 'latest')
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-conversion-sim'
$IMG  = 'localhost/nba-conversion-sim:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-conversion-sim',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=18',
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', 'NBA_ACTION_LIBRARY=http://nba-action-library:7001',
  '-e', "HARD_FRACTION=$HardFraction",
  '-e', "NBA_OFFSET_RESET=$OffsetReset",
  $IMG
)
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
