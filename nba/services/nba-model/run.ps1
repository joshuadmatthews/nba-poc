<#
.SYNOPSIS
  Build + (re)run the NBA model endpoint (local CQL Q-net serving, pure numpy + FastAPI). POC container — safe to
  recreate. Serves POST /score for the action-library fast path. -Build rebuilds the image.
  -QnetPath <file> bakes a specific qnet artifact (default: stage from nba/databricks/ml/rl_qnet.json, the legacy
  fallback). For the live healthcare model, fetch /Volumes/<ml_ns>/ckpt/rl_qnet.json and pass it here, or RO-mount it.
#>
[CmdletBinding()]
param([switch]$Build, [string]$QnetPath = '')
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-model'
$IMG  = 'localhost/ais-nba-model:latest'

# Stage the Q-net artifact into the build context (baked into the image). Default = the repo's legacy fallback.
$src = if ($QnetPath) { $QnetPath } else { Join-Path $here '..\..\databricks\ml\rl_qnet.json' }
if (-not (Test-Path -LiteralPath $src)) { throw "Q-net artifact not found: $src (pass -QnetPath, or fetch from the ML volume)" }
Copy-Item -LiteralPath $src -Destination (Join-Path $here 'rl_qnet.json') -Force

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-model',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=16',
  $IMG
)
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
Write-Host "nba-model up -> POST http://nba-model:7011/score   GET /healthz" -ForegroundColor Cyan
