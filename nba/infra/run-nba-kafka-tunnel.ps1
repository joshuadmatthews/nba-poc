<#
.SYNOPSIS
  Public TCP tunnel exposing the NBA Redpanda EXTERNAL Kafka listener (:19092) so Databricks SERVERLESS
  can speak NATIVE Kafka to us — including record HEADERS, which the pandaproxy REST path can't carry.

.WHY
  Serverless egress is FQDN-allowlist gated. With <tunnel-endpoint> allowlisted, a external TCP tunnel endpoint
  (<tunnel-endpoint>:<port>) is reachable, and spark-kafka can read with includeHeaders=true. The EXTERNAL
  listener REQUIRES SASL/SCRAM (run-nba-redpanda.ps1 -ExternalAdvertise), so the public exposure is
  authenticated; the INTERNAL listener (nba-redpanda:9092) stays anonymous for in-network services.

.FLOW (2-step, until a stable reserved TCP port is provisioned)
  1) run-nba-kafka-tunnel.ps1            -> prints the assigned endpoint  <tunnel-endpoint>:<PORT>
  2) run-nba-redpanda.ps1 -ExternalAdvertise <tunnel-endpoint>:<PORT>   (advertise MUST equal the endpoint)
  A reserved endpoint (external port reserve) makes the port stable so the advertise address never drifts.

.NOTES
  - external ACCESS_TOKEN is read from an existing external container (never hardcoded).
  - The SASL user (dbx-ingest) + password live in the gitignored nba/databricks/databricks.env
    (KAFKA_SASL_USER / KAFKA_SASL_PASS / KAFKA_BOOTSTRAP_EXTERNAL); production -> OpenBao.
  - -IpWhitelist <cidr,...> restricts who can reach the tunnel (defense in depth; serverless egress IPs).
#>
[CmdletBinding()]
param(
    [string]$Backend = "ais-nba-redpanda:19092",   # the EXTERNAL listener
    [string]$Region = "us",
    # Stable reserved TCP endpoint (external endpoint reserve --port 19092 --region us) so the advertise address
    # survives a tunnel restart. redpanda MUST advertise this exact host:port: run-nba-redpanda.ps1 -ExternalAdvertise <tunnel-endpoint>
    [string]$ReservedEndpoint = "<tunnel-endpoint>",
    [string]$IpWhitelist = "",
    [string]$AccessToken = "",
    [string]$Image = "docker.io/localxpose/localxpose:latest"
)
$ErrorActionPreference = 'Stop'
$NAME = 'ais-nba-kafka-tunnel'

function Get-LoclxToken {
    param([string]$Override)
    if ($Override) { return $Override }
    foreach ($c in @("ais-external-sig", "ais-external", "ais-nba-kafka-tunnel")) {
        try { $envLines = podman inspect $c --format '{{range .Config.Env}}{{println .}}{{end}}' 2>$null } catch { continue }
        if ($envLines) {
            $l = $envLines | Where-Object { $_ -like 'ACCESS_TOKEN=*' } | Select-Object -First 1
            if ($l) { return $l.Substring('ACCESS_TOKEN='.Length) }
        }
    }
    throw "No external ACCESS_TOKEN found and none passed via -AccessToken."
}
$token = Get-LoclxToken -Override $AccessToken

$tunnelArgs = @('tunnel', 'tcp', '--to', $Backend)
if ($ReservedEndpoint) { $tunnelArgs += @('--reserved-endpoint', $ReservedEndpoint) } else { $tunnelArgs += @('--region', $Region) }
if ($IpWhitelist) { $tunnelArgs += @('--ip-whitelist', $IpWhitelist) }

podman rm -f $NAME 2>$null | Out-Null
podman run -d --name $NAME --network aiservices_default --restart unless-stopped --label 'ais.boot.wave=16' `
    -e "ACCESS_TOKEN=$token" $Image @tunnelArgs | Out-Null
Start-Sleep -Seconds 7
$endpoint = (podman logs $NAME 2>$null | Select-String -Pattern 'external\.io:\d+' | Select-Object -Last 1)
Write-Host "[$NAME] -> $Backend" -ForegroundColor Cyan
Write-Host "  endpoint: $endpoint" -ForegroundColor Green
Write-Host "  NEXT: run-nba-redpanda.ps1 -ExternalAdvertise <that <tunnel-endpoint>:PORT>" -ForegroundColor DarkGray
