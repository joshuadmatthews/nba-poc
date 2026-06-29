<#
.SYNOPSIS
  Build + (re)run the NBA Action Library API (POC container — safe to recreate).
  -Build rebuilds the image. Reaches nba-postgres + (step B) nba-redpanda.
#>
[CmdletBinding()]
param([switch]$Build)
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$NAME = 'ais-nba-action-library'
$IMG  = 'localhost/ais-nba-action-library:latest'

if ($Build) { podman build -t $IMG -f (Join-Path $here 'Containerfile') $here }

# Databricks SP creds (gitignored env) — used to OAuth-mint a Lakebase DB credential for the online feature store.
$dbx = @{}
$envFile = Join-Path $here '..\..\databricks\databricks.env'
if (Test-Path $envFile) { Get-Content $envFile | ForEach-Object { if ($_ -match '^(DATABRICKS_\w+)=(.*)$') { $dbx[$Matches[1]] = $Matches[2].Trim() } } }

# ML-workspace SP creds (gitignored) — the nba-cql serving endpoint lives in the ML workspace (different from the LAKE one
# above), so the hot-path dbx scorer mints its own token here. NBA_SERVING_URL points at the nba-cql endpoint.
$ml = @{}
$mlEnvFile = Join-Path $here '..\..\databricks\ml\ml.env'
if (Test-Path $mlEnvFile) { Get-Content $mlEnvFile | ForEach-Object { if ($_ -match '^(ML_DATABRICKS_\w+)=(.*)$') { $ml[$Matches[1]] = $Matches[2].Trim() } } }

$ARGS = @(
  '-d', '--name', $NAME,
  '--network', 'aiservices_default', '--network-alias', 'nba-action-library',
  '--restart', 'unless-stopped', '--label', 'ais.boot.wave=16',
  '-e', 'NBA_DB_URL=jdbc:postgresql://nba-postgres:5432/actionlib',
  '-e', 'NBA_BOOTSTRAP=nba-redpanda:9092',
  '-e', 'NBA_REDIS_HOST=nba-redis',
  '-e', 'NBA_KIE_URL=http://nba-kie-server:7010',     # fast-path eligibility (mode=kie)
  '-e', 'NBA_MODEL_URL=http://nba-model:7011',        # fast-path local Q-net scoring
  # LAKEBASE online feature store: read gold features straight from Postgres on the disposition (prod pattern).
  '-e', 'NBA_LAKEBASE_HOST=<your-lakebase-host>',
  '-e', 'NBA_LAKEBASE_USER=<your-sp-client-id>',
  '-e', 'NBA_LAKEBASE_INSTANCE=nba-lakebase',
  # goldFeatures reads the rich features STRAIGHT from gold via this serverless SQL warehouse — no Redis cache.
  # (Lakebase's continuous synced table is blocked by the rootless metastore; this is the live source until that's fixed.)
  '-e', 'NBA_DBX_WAREHOUSE=<warehouse-id>',
  '-e', 'NBA_LAKE_NS=workspace.nba_poc'
)
foreach ($kv in @{ 'NBA_DBX_HOST' = 'DATABRICKS_HOST'; 'NBA_DBX_CLIENT_ID' = 'DATABRICKS_CLIENT_ID'; 'NBA_DBX_CLIENT_SECRET' = 'DATABRICKS_CLIENT_SECRET' }.GetEnumerator()) {
  if ($dbx[$kv.Value]) { $ARGS += @('-e', "$($kv.Key)=$($dbx[$kv.Value])") }
}
# ML workspace + nba-cql serving endpoint for the hot-path dbx scorer (the default scorer).
if ($ml['ML_DATABRICKS_HOST']) {
  $ARGS += @('-e', "NBA_ML_HOST=$($ml['ML_DATABRICKS_HOST'])")
  $ARGS += @('-e', "NBA_SERVING_URL=$($ml['ML_DATABRICKS_HOST'])/serving-endpoints/nba-cql/invocations")
}
if ($ml['ML_DATABRICKS_CLIENT_ID'])     { $ARGS += @('-e', "NBA_ML_CLIENT_ID=$($ml['ML_DATABRICKS_CLIENT_ID'])") }
if ($ml['ML_DATABRICKS_CLIENT_SECRET']) { $ARGS += @('-e', "NBA_ML_CLIENT_SECRET=$($ml['ML_DATABRICKS_CLIENT_SECRET'])") }
$ARGS += $IMG
podman rm -f $NAME 2>$null | Out-Null
podman run @ARGS | Out-Null
Start-Sleep -Seconds 4
podman ps --filter "name=$NAME" --format '{{.Names}}  {{.Status}}'
