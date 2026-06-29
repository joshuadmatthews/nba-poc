<#
.SYNOPSIS
  Public HTTPS tunnel exposing the NBA Redpanda HTTP Proxy (pandaproxy / Kafka REST)
  so Databricks SERVERLESS can produce/consume our Kafka topics directly — no bridges.

.WHY
  Serverless can't speak native Kafka (9092) to our local Redpanda, and its egress
  firewall only carries HTTPS to allowlisted FQDNs. Redpanda's pandaproxy IS the Kafka
  API over HTTP, so we tunnel it out on a STABLE reserved external domain (<proxy-tunnel-endpoint>)
  — NOT behind Cloudflare, so no 1010 bot-block. Serverless then:
    OUT: POST /topics/nba.facts        (produce gold deltas)
    IN : POST /consumers + GET records (consume raw -> bronze)
  Auth = external --key-auth: every request must carry  X-Token: <KeyAuth>.

  Standalone tunnel (its OWN netns) — independent of the LiveKit external tunnels
  (ais-external / ais-external-sig), so recreating it never touches RC calling.

.NOTES
  - external ACCESS_TOKEN is read from an existing external container (set at first creation),
    never hardcoded — same pattern as scripts/recreate-external.ps1.
  - -KeyAuth must MATCH the token Databricks sends (KAFKA_PROXY_TOKEN in the gitignored
    nba/databricks/databricks.env). Treat it as a secret; for production it belongs in vault.
  - The reserved domain <proxy-tunnel-endpoint> was created with:  external domain reserve --subdomain nbakafka --region us
#>
[CmdletBinding()]
param(
    [string]$KeyAuth = "",                         # required; else read from nba/databricks/databricks.env
    [string]$AccessToken = "",                     # optional override; else read from an existing external container
    [string]$ReservedDomain = "<proxy-tunnel-endpoint>",
    [string]$Backend = "nba-redpanda:8082",        # pandaproxy on the POC Redpanda (network-alias)
    [string]$Image = "docker.io/localxpose/localxpose:latest",
    [switch]$DryRun
)
$ErrorActionPreference = 'Stop'
$NAME = 'ais-nba-pandaproxy-tunnel'

# key-auth: prefer the param, else pull KAFKA_PROXY_TOKEN from the gitignored databricks.env
if (-not $KeyAuth) {
    $envFile = Join-Path $PSScriptRoot '..\databricks\databricks.env'
    if (Test-Path $envFile) {
        $line = Get-Content $envFile | Where-Object { $_ -match '^KAFKA_PROXY_TOKEN=' } | Select-Object -First 1
        if ($line) { $KeyAuth = $line.Substring('KAFKA_PROXY_TOKEN='.Length).Trim() }
    }
}
if (-not $KeyAuth) { throw "No -KeyAuth and no KAFKA_PROXY_TOKEN in nba/databricks/databricks.env. Refusing to expose pandaproxy unauthenticated." }

# external ACCESS_TOKEN from an existing external container (never hardcoded)
function Get-LoclxToken {
    param([string]$Override)
    if ($Override) { return $Override }
    foreach ($c in @("ais-external-sig", "ais-external")) {
        $envLines = podman inspect $c --format '{{range .Config.Env}}{{println .}}{{end}}' 2>$null
        if ($LASTEXITCODE -eq 0 -and $envLines) {
            $l = $envLines | Where-Object { $_ -like 'ACCESS_TOKEN=*' } | Select-Object -First 1
            if ($l) { return $l.Substring('ACCESS_TOKEN='.Length) }
        }
    }
    throw "No external ACCESS_TOKEN found in ais-external-sig/ais-external and none passed via -AccessToken."
}
$token = Get-LoclxToken -Override $AccessToken

$ARGS = @(
    '-d', '--name', $NAME,
    '--network', 'aiservices_default',
    '--restart', 'unless-stopped', '--label', 'ais.boot.wave=16',
    '-e', "ACCESS_TOKEN=$token",
    $Image,
    'tunnel', 'http', '--to', $Backend, '--reserved-domain', $ReservedDomain,
    '--key-auth', $KeyAuth, '--https-redirect'
)

if ($DryRun) { Write-Host "DRY RUN (key-auth masked):"; Write-Host ("podman run " + (($ARGS -replace [regex]::Escape($KeyAuth), '***') -join ' ')); exit 0 }

podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 5
$state = (podman inspect $NAME --format '{{.State.Status}}' 2>$null)
Write-Host "[$NAME] state=$state  https://$ReservedDomain -> $Backend  (auth: X-Token header)" -ForegroundColor Cyan
Write-Host "Verify: curl -s -H 'X-Token: <KeyAuth>' https://$ReservedDomain/topics" -ForegroundColor DarkGray
