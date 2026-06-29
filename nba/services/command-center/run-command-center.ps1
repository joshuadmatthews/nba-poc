<#
.SYNOPSIS
  Run the NBA Command Center: build + run the BFF (GraphQL over the Action Library + the Databricks
  datalake) and build the React UI. UI serves same-origin behind nginx; the BFF answers /graphql.

.DESCRIPTION
  BFF resolvers split two ways:
    - authoring (actions/rules)  -> Action Library REST (nba-action-library:7001), live NBA pipeline
    - analytics + rule funnel    -> Databricks lake (gold/silver) via the SQL warehouse
  Databricks creds come from the gitignored nba/databricks/databricks.env (vault in production).
  The BFF runs on aiservices_default so it reaches the (container-internal) Action Library, and
  publishes :4000 so the UI dev server / nginx can proxy /graphql to it.
  -DryRun prints the podman command. -SkipUiBuild skips the vite build.
#>
[CmdletBinding()]
param([switch]$DryRun, [switch]$SkipUiBuild)
$ErrorActionPreference = 'Stop'
# IMPORTANT: the UI's nginx proxies /graphql + /topology + /stream to network-alias nba-bff. This MUST
# deploy that container (ais-nba-bff / nba-bff), or the UI keeps using a stale BFF. (bff/run.ps1 is the
# canonical single-service deploy; this wrapper also builds the UI.)
$NAME = 'ais-nba-bff'
$IMG = 'ais-nba-bff:local'
$here = $PSScriptRoot

# Databricks creds from the gitignored env (never hardcode)
$envFile = Join-Path $here '..\..\databricks\databricks.env'
$dbx = @{}
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object { if ($_ -match '^(DATABRICKS_\w+)=(.*)$') { $dbx[$Matches[1]] = $Matches[2].Trim() } }
}
if (-not $dbx.DATABRICKS_HOST) { Write-Warning "No Databricks creds in $envFile — analytics tabs will show 'lake not configured'." }

$ARGS = @(
    '-d', '--name', $NAME,
    '--network', 'aiservices_default', '--network-alias', 'nba-bff',
    '-p', '4000:4000', '--restart', 'unless-stopped', '--label', 'ais.boot.wave=16',
    '-e', 'PORT=4000', '-e', 'NBA_ACTIONLIB_URL=http://nba-action-library:7001', '-e', 'NBA_LAKE_NS=workspace.nba_poc',
    # Analytics read off Lakebase Postgres (scale-to-zero) — the whole UI runs with the SQL warehouse stopped.
    # Remove these to fall back to the warehouse. DBX creds below still mint the Lakebase credential.
    '-e', 'NBA_LAKEBASE_HOST=<your-lakebase-host>',
    '-e', 'NBA_LAKEBASE_USER=<your-sp-client-id>',
    '-e', 'NBA_LAKEBASE_INSTANCE=nba-lakebase'
)
foreach ($k in 'DATABRICKS_HOST', 'DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET') { if ($dbx[$k]) { $ARGS += @('-e', "$k=$($dbx[$k])") } }
$ARGS += @($IMG)

if ($DryRun) {
    $masked = ($ARGS -replace 'DATABRICKS_CLIENT_SECRET=.*', 'DATABRICKS_CLIENT_SECRET=***')
    Write-Host "podman build -t $IMG `"$here\bff`""; Write-Host ("podman run " + ($masked -join ' ')); exit 0
}

Write-Host "[command-center] building BFF image…" -ForegroundColor Cyan
podman build -t $IMG (Join-Path $here 'bff') | Out-Null

if (-not $SkipUiBuild) {
    Write-Host "[command-center] building UI…" -ForegroundColor Cyan
    Push-Location (Join-Path $here 'ui')
    npm install --no-audit --no-fund | Out-Null
    npm run build | Out-Null
    Pop-Location
    Write-Host "[command-center] UI built -> ui/dist (serve behind nginx, proxy /graphql -> ${NAME}:4000)" -ForegroundColor DarkGray
}

podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 5
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
$lakeState = if ($dbx.DATABRICKS_HOST) { 'configured' } else { 'NOT configured' }
Write-Host "[command-center] BFF up on nba-command-center-bff:4000 (host :4000)  lake: $lakeState" -ForegroundColor Cyan
