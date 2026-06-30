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
  # 8gb (~1.7M members @ ~4.6KB) -- a FAIR ceiling vs the disk-backed engines (RocksDB/Flink get the whole disk).
  # The old 256mb capped the central state store at ~55k members on a 32GB VM, which made the throughput comparison
  # unfair (it OOM'd classic at a tiny member count while the disk engines had unbounded state). See PERFORMANCE.md §3/§6.
  'redis-server', '--appendonly', 'yes', '--maxmemory', '8gb', '--maxmemory-policy', 'noeviction'
)

if ($DryRun) { Write-Host "DRY RUN:"; Write-Host ("podman run " + ($ARGS -join ' ')); exit 0 }

podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
