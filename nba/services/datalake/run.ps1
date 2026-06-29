<#
.SYNOPSIS
  RETIRED — the in-pod NBA datalake Redis-counter service is no longer used.

.WHY
  Comms-frequency counting moved to the LAKE. Delta is transactional and already holds every send,
  so there is zero reason for a Redis counter (a dual write that can't be kept consistent). The
  Databricks notebook nba/databricks/nba_comms_count.py now counts per-member weekly comms straight
  from silver_fact_history and native-produces operator.comms.totalThisWeek / {channel}sThisWeek with
  origin=lake. Run it with:  python nba/databricks/run_kafka_jobs.py comms [triggered|continuous]

  The container (ais-nba-datalake) has been removed and its Redis keys (nba:dl:*) deleted. This stub
  stays so an old habit can't silently resurrect the Redis-counter dual write. The Java source is kept
  for history only; it is not built or run.
#>
[CmdletBinding()]
param([switch]$Build)
Write-Host "ais-nba-datalake is RETIRED — comms counting now lives in Databricks (nba_comms_count.py)." -ForegroundColor Yellow
Write-Host "Run:  python nba/databricks/run_kafka_jobs.py comms" -ForegroundColor DarkGray
exit 0
