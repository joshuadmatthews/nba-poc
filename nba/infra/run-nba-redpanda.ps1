<#
.SYNOPSIS
  Isolated single-node Redpanda for the NBA POC (Kafka :9092 + Schema Registry :8081).
.DESCRIPTION
  Mirrors ais-cdc-redpanda's image/flags but on its OWN broker + volume so the POC can
  never wobble the medallion CDC broker (the one that took the Cortex cascade down on
  disk thrash). Internal-only (no host ports) — reach it from other containers as
  nba-redpanda:9092. Lighter footprint (1 core / 1G) since it's a POC.
  Re-runnable (the container is brand-new POC state, nothing shared). -DryRun prints.
  After run: nba/infra/create-topics.ps1
#>
# -ExternalAdvertise <host:port>: add a SECOND Kafka listener (EXTERNAL://0.0.0.0:19092) advertised as that
# host:port, so an off-network client (e.g. Databricks serverless via a external TCP tunnel -> :19092) can speak
# NATIVE Kafka to us — incl. record HEADERS, which the pandaproxy REST path can't carry. Internal services keep
# using nba-redpanda:9092 unchanged. The advertised host:port MUST equal the tunnel's public endpoint.
[CmdletBinding()]
param([switch]$DryRun, [string]$ExternalAdvertise = "")
$ErrorActionPreference = 'Stop'

$NAME = 'ais-nba-redpanda'
$IMG  = 'mirror.gcr.io/redpandadata/redpanda:v24.2.7'
# Listener config. The --kafka-addr flags REBUILD the kafka_api list and drop any per-listener
# authentication_method set via --set, so when we need SASL we define the WHOLE list via --set instead
# (no --kafka-addr flags) — INTERNAL anonymous, EXTERNAL sasl. SCRAM users are created after start via the
# admin API (no broker creds needed for that).
if ($ExternalAdvertise) {
  $ehost, $eport = $ExternalAdvertise.Split(':')
  # Build the kafka_api list via INDEXED SCALAR --set (each a plain key=value — survives PowerShell/podman
  # arg passing, unlike whole-list/JSON values). No --kafka-addr (it would rebuild the list and drop the
  # per-listener authentication_method). INTERNAL stays anonymous; EXTERNAL (the public tunnel) requires SASL.
  $listenerArgs = @(
    '--set','redpanda.kafka_api[0].name=internal',     '--set','redpanda.kafka_api[0].address=0.0.0.0','--set','redpanda.kafka_api[0].port=9092','--set','redpanda.kafka_api[0].authentication_method=none',
    '--set','redpanda.kafka_api[1].name=external',      '--set','redpanda.kafka_api[1].address=0.0.0.0','--set','redpanda.kafka_api[1].port=19092','--set','redpanda.kafka_api[1].authentication_method=sasl',
    '--set','redpanda.advertised_kafka_api[0].name=internal','--set','redpanda.advertised_kafka_api[0].address=nba-redpanda','--set','redpanda.advertised_kafka_api[0].port=9092',
    '--set','redpanda.advertised_kafka_api[1].name=external','--set',"redpanda.advertised_kafka_api[1].address=$ehost",'--set',"redpanda.advertised_kafka_api[1].port=$eport",
    '--set','redpanda.sasl_mechanisms=[SCRAM-SHA-256]'
  )
} else {
  $listenerArgs = @(
    '--kafka-addr=PLAINTEXT://0.0.0.0:9092',
    '--advertise-kafka-addr=PLAINTEXT://nba-redpanda:9092'
  )
}
$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-redpanda',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=15',
  '-v', 'ais-nba-redpanda-data:/var/lib/redpanda/data',
  $IMG,
  'redpanda', 'start',
  '--smp=1', '--memory=1G', '--reserve-memory=0M', '--overprovisioned',
  '--node-id=0', '--check=false'
) + $listenerArgs

if ($DryRun) { Write-Host "DRY RUN:"; Write-Host ("podman run " + ($ARGS -join ' ')); exit 0 }

podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 6
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
Write-Host "NEXT: nba/infra/create-topics.ps1" -ForegroundColor Cyan
