<#
.SYNOPSIS
  Isolated Redis for the NBA POC (id-map memberId->NBAID + per-NBAID snapshots).
.DESCRIPTION
  Own instance (not the shared ais-redis) so POC keys can't collide with production
  cache. Reach it as nba-redis:6379. -DryRun prints.
#>
[CmdletBinding()]
param([switch]$DryRun)
$ErrorActionPreference = 'Stop'

$NAME = 'ais-nba-redis'
$IMG  = 'docker.io/library/redis:7-alpine'
$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-redis',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=15',
  '-v', 'ais-nba-redis-data:/data',
  $IMG,
  'redis-server', '--appendonly', 'yes', '--maxmemory', '256mb', '--maxmemory-policy', 'noeviction'
)

if ($DryRun) { Write-Host "DRY RUN:"; Write-Host ("podman run " + ($ARGS -join ' ')); exit 0 }

podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
