<#
.SYNOPSIS
  Build + (re)run the NBA Command Center BFF (Node Apollo GraphQL). POC container.
  -Build rebuilds the image. Reaches nba-action-library:7001.
#>
[CmdletBinding()]
param([switch]$Build)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-bff'
$IMG  = 'localhost/ais-nba-bff:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

# Databricks creds from the gitignored env (analytics tabs go dark without them). nginx proxies the UI's
# /graphql + /topology + /stream to THIS container (network-alias nba-bff) — it is the BFF the UI uses.
$dbx = @{}
$envFile = Join-Path $here '..\..\..\databricks\databricks.env'
if (Test-Path $envFile) { Get-Content $envFile | ForEach-Object { if ($_ -match '^(DATABRICKS_\w+)=(.*)$') { $dbx[$Matches[1]] = $Matches[2].Trim() } } }
if (-not $dbx.DATABRICKS_HOST) { Write-Warning "No Databricks creds in $envFile - analytics tabs will show 'lake not configured'." }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-bff',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=17',
  '-e', 'PORT=4000', '-e', 'NBA_ACTIONLIB_URL=http://nba-action-library:7001', '-e', 'NBA_LAKE_NS=workspace.nba_poc',
  # Analytics read off Lakebase Postgres (scale-to-zero) instead of the SQL warehouse — the WHOLE UI then runs
  # with the warehouse stopped. Remove these three to fall back to the warehouse (databricks.js). DBX creds
  # below are still needed to OAuth-mint the Lakebase credential.
  '-e', 'NBA_LAKEBASE_HOST=<your-lakebase-host>',
  '-e', 'NBA_LAKEBASE_USER=<your-sp-client-id>',
  '-e', 'NBA_LAKEBASE_INSTANCE=nba-lakebase'
)
foreach ($k in 'DATABRICKS_HOST', 'DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET') { if ($dbx[$k]) { $ARGS += @('-e', "$k=$($dbx[$k])") } }
$ARGS += $IMG
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 3
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
