<#
.SYNOPSIS
  Build + (re)run the NBA snapshot-builder (POC container — safe to recreate, nothing
  shared). Consumes nba.member.facts, writes per-NBAID snapshots to nba-redis, emits
  nba.snapshots. -Build rebuilds the image first.
#>
[CmdletBinding()]
param([switch]$Build)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-snapshot-builder'
$IMG  = 'localhost/ais-nba-snapshot-builder:latest'

if ($Build) {
  podman build -t $IMG -f (Join-Path $here 'Containerfile') $here
}

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=16',
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', 'NBA_REDIS_HOST=nba-redis',
  $IMG
)
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
