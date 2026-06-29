<#
.SYNOPSIS
  Run the NBA Temporal dev server (self-contained: embedded DB + Web UI). POC container.
.DESCRIPTION
  temporalio/temporal `server start-dev` — frontend gRPC on 7233 (workers connect via the
  nba-temporal:7233 network alias), Web UI on 8233 (forwarded to the host). Namespace
  `default` is auto-created. In-memory state for the POC (restart = fresh workflows).
#>
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$NAME = 'ais-nba-temporal'

podman rm -f $NAME 2>$null | Out-Null
podman run -d --name $NAME `
  --network aiservices_default --network-alias nba-temporal `
  --restart unless-stopped --label ais.boot.wave=16 `
  -p 8233:8233 `
  docker.io/temporalio/temporal:latest `
  server start-dev --ip 0.0.0.0 --log-level warn | Out-Null
Start-Sleep -Seconds 6

# Custom search attributes the ChannelAction workflows set at START (saFor -> NbaActionId/NbaChannel) and the
# suppression batch op filters on. start-dev is IN-MEMORY, so they vanish on every restart -- and a missing key
# makes EVERY workflow start fail INVALID_ARGUMENT('search attribute not defined'), silently DLQ'ing the whole
# state-machine layer. Register idempotently here, retrying until the frontend accepts, so a restart self-heals.
foreach ($i in 1..15) {
  podman exec $NAME temporal operator search-attribute create --name NbaActionId --type Keyword 2>$null | Out-Null
  podman exec $NAME temporal operator search-attribute create --name NbaChannel  --type Keyword 2>$null | Out-Null
  $have = podman exec $NAME temporal operator search-attribute list 2>$null | Select-String -Pattern 'NbaActionId','NbaChannel'
  if (($have | Measure-Object).Count -ge 2) { Write-Host "search attributes registered: NbaActionId, NbaChannel"; break }
  Start-Sleep -Seconds 2
}

podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
