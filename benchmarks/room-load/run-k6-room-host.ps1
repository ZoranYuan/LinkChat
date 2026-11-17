param(
  [string]$FixtureFile = "",
  [string]$ApiBaseUrl = "http://192.168.1.6:8081/api/v1",
  [string]$PrometheusRemoteWriteUrl = "http://127.0.0.1:9090/api/v1/write",
  [string]$RunId = "room-host-smoke",
  [int]$Vus = 2,
  [int]$RatePerUser = 1,
  [int]$RampSeconds = 1,
  [int]$WarmupSeconds = 2,
  [int]$DurationSeconds = 3,
  [int]$DrainSeconds = 5,
  [int]$ProbeEvery = 1,
  [switch]$SkipReadinessCheck,
  [switch]$SkipPrometheusRemoteWrite
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$scriptFile = Join-Path $PSScriptRoot 'k6-room-host.js'

if (-not $FixtureFile) { throw '每次压测请先准备全新的账号和房间 fixture，再通过 -FixtureFile 指定。' }
if (-not (Test-Path -LiteralPath $FixtureFile)) {
  throw "找不到 fixture 文件：$FixtureFile"
}
$FixtureFile = (Resolve-Path -LiteralPath $FixtureFile).Path

$k6 = (Get-Command k6 -ErrorAction SilentlyContinue).Source
if (-not $k6 -and (Test-Path 'C:\Program Files\k6\k6.exe')) { $k6 = 'C:\Program Files\k6\k6.exe' }
if (-not $k6) { throw '找不到 k6，请先安装 k6 或将其加入 PATH。' }

$fixture = Get-Content -LiteralPath $FixtureFile -Raw | ConvertFrom-Json
if (-not $fixture.conversationId) { throw 'fixture 缺少 conversationId。' }
if (@($fixture.users).Count -lt $Vus) { throw "fixture 用户数不足：需要 $Vus，实际 $(@($fixture.users).Count)" }

$healthUrl = ($ApiBaseUrl -replace '/api/v1/?$', '') + '/readyz'
if (-not $SkipReadinessCheck) {
  try {
    $health = Invoke-WebRequest -Uri $healthUrl -TimeoutSec 4 -UseBasicParsing
    if ($health.StatusCode -ne 200) { throw "HTTP $($health.StatusCode)" }
    Write-Host "服务就绪：$healthUrl"
  } catch {
    throw "后端未通过就绪检查 ($healthUrl)：$($_.Exception.Message)。已停止，未启动 k6。"
  }
}

$env:FIXTURE_FILE = $FixtureFile
$env:ROOM_USERS = "$Vus"
$env:RATE_PER_USER = "$RatePerUser"
$env:RAMP_SECONDS = "$RampSeconds"
$env:WARMUP_SECONDS = "$WarmupSeconds"
$env:DURATION_SECONDS = "$DurationSeconds"
$env:DRAIN_SECONDS = "$DrainSeconds"
$env:PROBE_EVERY = "$ProbeEvery"
$env:RUN_ID = $RunId
$env:CONVERSATION_ID = [string]$fixture.conversationId
$env:API_BASE_URL = $ApiBaseUrl.TrimEnd('/')

$arguments = @('run', '--tag', "run_id=$RunId")
if (-not $SkipPrometheusRemoteWrite) {
  $env:K6_PROMETHEUS_RW_SERVER_URL = $PrometheusRemoteWriteUrl
  $env:K6_PROMETHEUS_RW_TREND_STATS = 'p(95),p(99),avg'
  $arguments += @('-o', 'experimental-prometheus-rw')
}
$arguments += $scriptFile

Write-Host "k6: $k6"
Write-Host "API: $($env:API_BASE_URL)"
Write-Host "场景：$Vus VU；每人 $RatePerUser 条/秒；正式发送 $DurationSeconds 秒；run_id=$RunId"
& $k6 @arguments
exit $LASTEXITCODE
