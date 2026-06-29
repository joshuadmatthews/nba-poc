<#
.SYNOPSIS
  Build + (re)run the NBA KIE Server (Drools decision service). POC container — safe to recreate.
  Hosts the KieBase (built from nba.definitions) + serves POST /evaluate. The rules engine offloads
  its inline KieSession execution here when NBA_RULES_MODE=kie. -Build rebuilds the image.
  Run multiple replicas (-Replicas N) to scale evaluation for load tests.
#>
[CmdletBinding()]
param([switch]$Build, [int]$Replicas = 1)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$IMG  = 'localhost/ais-nba-kie-server:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

# One primary container on the network alias nba-kie-server; extra replicas share the alias (DNS round-robins).
for ($i = 0; $i -lt $Replicas; $i++) {
  $NAME = if ($i -eq 0) { 'ais-nba-kie-server' } else { "ais-nba-kie-server-$i" }
  $ARGS = @(
    '-d', '--name', $NAME,
    '--network', 'aiservices_default', '--network-alias', 'nba-kie-server',
    '--restart', 'unless-stopped', '--label', 'ais.boot.wave=17',
    '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
    $IMG
  )
  podman rm -f $NAME 2>$null | Out-Null
  podman run @ARGS | Out-Null
}
Start-Sleep -Seconds 3
podman ps --filter "name=ais-nba-kie-server" --format '{{.Names}}  {{.Status}}'
Write-Host "POST http://nba-kie-server:7010/evaluate  (toggle the rules engine with -Mode kie)" -ForegroundColor Cyan
