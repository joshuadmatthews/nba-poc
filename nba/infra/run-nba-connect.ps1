<#
.SYNOPSIS
  Run Kafka Connect (Debezium) for the NBA state-machine outbox. POC container.
.DESCRIPTION
  Distributed Connect worker (REST on 8083) configured for EXACTLY-ONCE source support. The
  Debezium Postgres connector (registered separately) CDC-tails the outbox tables and publishes
  to nba.member.facts / nba.activations. Internal Connect topics live on ais-nba-redpanda.
#>
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$NAME = 'ais-nba-connect'

podman rm -f $NAME 2>$null | Out-Null
podman run -d --name $NAME `
  --network aiservices_default --network-alias nba-connect `
  --restart unless-stopped --label ais.boot.wave=23 `
  -p 8083:8083 `
  -e BOOTSTRAP_SERVERS=ais-nba-redpanda:9092 `
  -e GROUP_ID=nba-connect `
  -e CONFIG_STORAGE_TOPIC=nba_connect_configs `
  -e OFFSET_STORAGE_TOPIC=nba_connect_offsets `
  -e STATUS_STORAGE_TOPIC=nba_connect_status `
  -e CONFIG_STORAGE_REPLICATION_FACTOR=1 `
  -e OFFSET_STORAGE_REPLICATION_FACTOR=1 `
  -e STATUS_STORAGE_REPLICATION_FACTOR=1 `
  -e OFFSET_STORAGE_PARTITIONS=1 `
  -e STATUS_STORAGE_PARTITIONS=1 `
  -e KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter `
  -e VALUE_CONVERTER=org.apache.kafka.connect.storage.StringConverter `
  -e CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT=enabled `
  quay.io/debezium/connect:2.7 | Out-Null
Start-Sleep -Seconds 12
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'

# Register the Debezium outbox connector (idempotent: PUT updates if it already exists). The
# connector config also persists in the Connect config topic across restarts.
$cfg = Get-Content (Join-Path $PSScriptRoot 'nba-outbox-connector.json') -Raw | ConvertFrom-Json
$body = $cfg.config | ConvertTo-Json -Depth 10
for ($i=0; $i -lt 20; $i++) { try { Invoke-RestMethod http://localhost:8083/ -TimeoutSec 2 | Out-Null; break } catch { Start-Sleep 2 } }
try { Invoke-RestMethod -Method Put -Uri "http://localhost:8083/connectors/nba-outbox/config" -ContentType 'application/json' -Body $body | Out-Null; Write-Host "connector nba-outbox registered" } catch { Write-Host "connector register: $_" }
