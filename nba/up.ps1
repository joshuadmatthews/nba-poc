<#
.SYNOPSIS
  ONE-COMMAND local bring-up of the NBA POC flywheel (no Databricks, no cloud, no tunnel).
.DESCRIPTION
  Boots the full local Next-Best-Action stack on podman in dependency order, creates the Kafka topics,
  seeds a working set of action/rule definitions + N demo members, and smoke-tests that facts flow through
  to snapshots and evaluations. Everything is local: the Databricks ML/lake integration is OFF and the
  local stand-ins (nba-model + nba-journey-scorer + nba-conversion-sim) close the loop instead.

  FIRST RUN: pass -Build to compile every service image from source (~10-15 min). Later runs reuse the
  cached images (~2-3 min). Re-running is safe - every step is idempotent (podman rm -f + run).

  Tear down with:  pwsh nba/down.ps1   (add -Volumes to wipe data too)
.PARAMETER Build     Rebuild all service container images from source first.
.PARAMETER Members   Number of demo members to seed (default 200).
.PARAMETER Engines   Also start the reference engines (decision-engine + flink-engine, shadow) used by the load tests.
.PARAMETER SkipSeed  Boot only; do not seed definitions / members.
.EXAMPLE
  pwsh nba/up.ps1 -Build          # first time (compiles images)
  pwsh nba/up.ps1                 # subsequent boots
  pwsh nba/up.ps1 -Engines        # also bring up the KStreams + Flink reference engines
#>
[CmdletBinding()]
param([switch]$Build, [int]$Members = 200, [switch]$Engines, [switch]$SkipSeed)
$ErrorActionPreference = 'Stop'
$here  = $PSScriptRoot
$infra = Join-Path $here 'infra'
$svc   = Join-Path $here 'services'
$bs    = @(); if ($Build) { $bs = @('-Build') }   # array (possibly empty) to splat; an if-expr @() collapses to $null
$t0    = Get-Date

function Step($m) { Write-Host "`n=== $m ===" -ForegroundColor Cyan }
function Wait-For($name, [scriptblock]$test, $timeoutSec = 120) {
  $sw = [Diagnostics.Stopwatch]::StartNew()
  while ($sw.Elapsed.TotalSeconds -lt $timeoutSec) {
    try { if (& $test) { Write-Host "  [ok]   $name" -ForegroundColor Green; return } } catch {}
    Start-Sleep 3
  }
  throw "[timeout] $name not healthy after ${timeoutSec}s - check: podman logs $name"
}
function Running($c) { (podman ps --filter "name=$c" --filter 'status=running' --format '{{.Names}}') -eq $c }

# ---- 0) prerequisites ---------------------------------------------------------------------------
Step '0/6  prerequisites'
podman info *> $null
if ($LASTEXITCODE) { throw 'podman is not available / not running. Install podman (or docker + alias) and start the machine.' }
if (-not (podman network ls --format '{{.Name}}' | Select-String -Quiet '^aiservices_default$')) {
  podman network create aiservices_default | Out-Null; Write-Host '  created network aiservices_default'
} else { Write-Host '  network aiservices_default present' }

# ---- 1) infra (wave 15) + topics + temporal (wave 16) -------------------------------------------
Step '1/6  infra: Redpanda + Postgres + Redis'
& (Join-Path $infra 'run-nba-redpanda.ps1') | Out-Null   # infra uses upstream images; no -Build
& (Join-Path $infra 'run-nba-postgres.ps1') | Out-Null
& (Join-Path $infra 'run-nba-redis.ps1')    | Out-Null
Wait-For 'ais-nba-redpanda' { podman exec ais-nba-redpanda rpk cluster info *> $null; $LASTEXITCODE -eq 0 }
Wait-For 'ais-nba-postgres' { podman exec ais-nba-postgres pg_isready -q *> $null; $LASTEXITCODE -eq 0 }
Wait-For 'ais-nba-redis'    { (podman exec ais-nba-redis redis-cli ping) -match 'PONG' }

Step '   topics'
& (Join-Path $infra 'create-topics.ps1') | Out-Null
Write-Host '  data + DLQ topics created'

Step '   Temporal (registers NbaActionId/NbaChannel search attributes on boot)'
& (Join-Path $infra 'run-nba-temporal.ps1') | Out-Null
Wait-For 'ais-nba-temporal' { podman exec ais-nba-temporal temporal operator search-attribute list 2>$null | Select-String -Quiet 'NbaActionId' }

# ---- 2) app tier (waves 16-21) ------------------------------------------------------------------
Step '2/6  services (snapshot -> rules -> score -> route -> state-machine -> action-layer)'
& (Join-Path $svc 'nba-model\run.ps1')          @bs | Out-Null   # wave 16  (local CQL Q-net for the inbound fast path)
& (Join-Path $svc 'action-library\run.ps1')     @bs | Out-Null   # wave 16  (REST authoring + inbound serve)
& (Join-Path $svc 'snapshot-builder\run.ps1')   @bs | Out-Null   # wave 16  (member.facts -> Redis snapshots)
& (Join-Path $svc 'rules-engine\run.ps1')       @bs | Out-Null   # wave 17  (embedded Drools; no kie-server needed)
& (Join-Path $svc 'nba-journey-scorer\run.ps1') @bs | Out-Null   # wave 17  (Databricks-free scorer)
& (Join-Path $svc 'nba-conversion-sim\run.ps1') @bs | Out-Null   # wave 18  (completes a fraction of delivered actions)
& (Join-Path $svc 'action-router\run.ps1')      @bs | Out-Null   # wave 19  (evaluations -> CREATE/SUPPRESS)
& (Join-Path $svc 'nba-temporal\run.ps1')       @bs | Out-Null   # wave 20  (debounced state-machine bridge -> workflows)
& (Join-Path $svc 'action-layer\run.ps1')       @bs | Out-Null   # wave 21  (dispatch -> dispositions)
foreach ($c in 'ais-nba-snapshot-builder', 'ais-nba-rules-engine', 'ais-nba-action-router', 'ais-nba-temporal-worker', 'ais-nba-action-layer') {
  Wait-For $c { Running $c }
}
if ($Engines) {
  Step '   reference engines (shadow): decision-engine (KStreams) + flink-engine'
  & (Join-Path $svc 'nba-decision-engine\run.ps1') @bs -Mode shadow | Out-Null
  & (Join-Path $svc 'nba-flink-engine\run.ps1')    @bs -Mode shadow | Out-Null
}

# ---- 3) seed: definitions (topic + Redis) + demo members ----------------------------------------
if (-not $SkipSeed) {
  Step "3/6  seed: action/rule definitions + $Members demo members"
  # Definitions onto nba.definitions by replaying a captured working set (no CDC/Postgres dependency for the demo).
  # Copy the file IN and redirect inside the container (avoids PowerShell stdin-pipe encoding adding a BOM / EOF).
  $defs = Join-Path $infra 'seed\definitions.jsonl'
  if (Test-Path $defs) {
    podman cp $defs ais-nba-redpanda:/tmp/nba-defs.jsonl | Out-Null
    podman exec ais-nba-redpanda sh -c "rpk topic produce nba.definitions -f '%k\t%v\n' < /tmp/nba-defs.jsonl" | Out-Null
    Write-Host "  replayed $((Get-Content $defs).Count) definition records -> nba.definitions"
  } else { Write-Host '  [warn] seed/definitions.jsonl missing - rules-engine will have no rules' }
  # Redis definition state (rulefacts lean-filter + action catalog + sim params).
  $rdefs = Join-Path $infra 'seed\redis-defs.sh'
  if (Test-Path $rdefs) {
    podman cp $rdefs ais-nba-redis:/tmp/nba-redis-defs.sh | Out-Null
    podman exec ais-nba-redis sh /tmp/nba-redis-defs.sh
  }
  # Demo members straight to nba.member.facts (the snapshot-builder folds them immediately).
  $env:NBA_SEED_TOPIC = 'nba.member.facts'
  python (Join-Path $infra 'reseed-members-local.py') $Members 2>&1 | Select-Object -Last 1 | ForEach-Object { Write-Host "  $_" }
  $env:NBA_SEED_TOPIC = $null
} else { Step '3/6  seed: SKIPPED (-SkipSeed)' }

# ---- 4) smoke: confirm the flywheel turned -------------------------------------------------------
Step '4/6  smoke: facts -> snapshots -> evaluations'
Start-Sleep 10
$snaps = @(podman exec ais-nba-redis redis-cli --scan --pattern 'nba:snapshot:*').Count
$evalHwm = (podman exec ais-nba-redpanda rpk topic describe nba.evaluations -p 2>$null | Select-String '^\d' | ForEach-Object { ($_ -split '\s+')[5] }) -as [int]
Write-Host "  Redis snapshots: $snaps   |   nba.evaluations high-watermark: $evalHwm"
if ($snaps -gt 0) { Write-Host '  [ok] snapshot-builder is folding member facts' -ForegroundColor Green }
else { Write-Host '  [warn] no snapshots yet - give it a few more seconds, or check: podman logs ais-nba-snapshot-builder' -ForegroundColor Yellow }

# ---- 5) summary ----------------------------------------------------------------------------------
Step '5/6  up'
podman ps --filter 'name=ais-nba-' --format '{{.Names}}\t{{.Status}}' | Sort-Object
$mins = [int]((Get-Date) - $t0).TotalSeconds
Write-Host "`nNBA stack up in ${mins}s." -ForegroundColor Green
Write-Host @"

  Temporal UI    : http://localhost:8233   (workflows: ChannelActionWorkflow)
  Command Center : start separately -> pwsh nba/services/command-center/run.ps1   (UI on :8490)

  Next:
    - Watch the loop:   podman logs -f ais-nba-temporal-worker      (activate / dispatch lines)
    - Run load tests:   bash nba/infra/run-loadtests.sh            (regenerates the throughput matrix)
    - Tear down:        pwsh nba/down.ps1                           (add -Volumes to wipe data)
"@
