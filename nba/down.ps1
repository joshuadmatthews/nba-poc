<#
.SYNOPSIS
  Tear down the local NBA stack — stop + remove every ais-nba-* container.
.DESCRIPTION
  Data volumes (Redpanda/Redis/Postgres) are PRESERVED by default so a later `up.ps1` keeps your
  seeded members/definitions. Pass -Volumes for a full wipe (fresh stack next time). Pass -Keep
  <substr> to spare matching containers (e.g. -Keep redpanda,redis,postgres,temporal to drop only
  the app tier while leaving infra up).
.EXAMPLE
  pwsh nba/down.ps1                 # stop+remove all app+infra containers, keep data
  pwsh nba/down.ps1 -Volumes        # also delete data volumes (full reset)
  pwsh nba/down.ps1 -Keep redpanda,redis,postgres,temporal   # drop only the app tier
#>
[CmdletBinding()]
param([switch]$Volumes, [string[]]$Keep = @())
$ErrorActionPreference = 'SilentlyContinue'

$names = podman ps -a --filter 'name=ais-nba-' --format '{{.Names}}' | Sort-Object
if (-not $names) { Write-Host 'No ais-nba-* containers found.'; }
foreach ($n in $names) {
  if ($Keep | Where-Object { $n -like "*$_*" }) { Write-Host "kept    $n"; continue }
  podman rm -f $n | Out-Null
  Write-Host "removed $n"
}
if ($Volumes) {
  foreach ($v in @('ais-nba-redpanda-data', 'ais-nba-redis-data', 'ais-nba-postgres-data')) {
    podman volume rm $v 2>$null | Out-Null
    Write-Host "removed volume $v"
  }
  Write-Host 'Data volumes wiped — next up.ps1 starts from a clean slate.'
}
Write-Host 'NBA stack down.'
