<#
.SYNOPSIS
  Build + (re)run the NBA Command Center UI (React + nginx). POC container.
  Published on host :8091. nginx proxies /graphql -> nba-bff. -Build rebuilds.
#>
[CmdletBinding()]
param([switch]$Build)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-command-center'
$IMG  = 'localhost/ais-nba-command-center:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-command-center',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=17',
  '-p', '8490:80',
  $IMG
)
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
Write-Host "Command Center UI -> http://localhost:8490" -ForegroundColor Cyan
