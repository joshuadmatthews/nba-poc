<#
.SYNOPSIS
  Isolated Postgres for the NBA Action Library (action/rule definitions + outbox).
  POC creds (nba/nba) — isolated instance, not production. Reach as nba-postgres:5432.
#>
[CmdletBinding()]
param([switch]$DryRun)
$ErrorActionPreference = 'Stop'
$NAME = 'ais-nba-postgres'
$IMG  = 'docker.io/library/postgres:16-alpine'
$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-postgres',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=15',
  '-e', 'POSTGRES_USER=nba', '-e', 'POSTGRES_PASSWORD=nba', '-e', 'POSTGRES_DB=actionlib',
  '-v', 'ais-nba-postgres-data:/var/lib/postgresql/data',
  $IMG,
  '-c', 'wal_level=logical'    # logical replication for the Debezium state-machine outbox
)
if ($DryRun) { Write-Host "DRY RUN:"; Write-Host ("podman run " + ($ARGS -join ' ')); exit 0 }
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 4
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
