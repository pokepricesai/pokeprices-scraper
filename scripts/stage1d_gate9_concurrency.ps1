# scripts/stage1d_gate9_concurrency.ps1
#
# Gate 9: prove the DB-backed run lock is fail-fast.
#
# T1 (holder): acquire lock, hold for 30s, release. Started in background.
# T2 (contender): full pipeline with all steps skipped so its only real
#                 work is acquiring the lock. Started ~4s after T1.
#                 MUST exit code 4 within a few seconds.
#
# Success criteria: T2 exit == 4  AND  T2 wall < 15s.

$root = Split-Path -Parent $PSScriptRoot
$holderScript = Join-Path $PSScriptRoot 'stage1d_hold_lock.py'
$runPipeline  = Join-Path $PSScriptRoot 'run_pipeline.ps1'
$t1Log = Join-Path $root "gate9_t1.log"
$t2Log = Join-Path $root "gate9_t2.log"

Write-Host "=== Gate 9: DB-backed concurrency lock (fail-fast) ==="

# Preload env from .env.local for both children.
Get-Content -LiteralPath (Join-Path $root "..\pokeprices-web\.env.local") | ForEach-Object {
    if ($_ -match '^([A-Za-z0-9_]+)\s*=\s*(.*)$') {
        $k = $Matches[1]; $v = $Matches[2]
        if ($v -match '^"(.*)"$' -or $v -match "^'(.*)'$") { $v = $Matches[1] }
        Set-Item -Path "Env:$k" -Value $v
    }
}
if (-not $env:SUPABASE_SERVICE_KEY -and $env:SUPABASE_SERVICE_ROLE_KEY) { $env:SUPABASE_SERVICE_KEY = $env:SUPABASE_SERVICE_ROLE_KEY }
if (-not $env:SUPABASE_URL -and $env:NEXT_PUBLIC_SUPABASE_URL)         { $env:SUPABASE_URL         = $env:NEXT_PUBLIC_SUPABASE_URL         }
$env:MTG_DAILY_INGEST_ENABLED      = 'true'
$env:MTG_PRICING_INGESTION_ENABLED = 'true'

# Launch holder (T1) as a detached process; it holds the lock 30s.
Write-Host "Starting T1 (holder)..."
$t1 = Start-Process -PassThru -NoNewWindow -RedirectStandardOutput $t1Log `
    -FilePath "python" -ArgumentList @($holderScript, '--hold-seconds', '30', '--label', 'gate9_holder')

# Give the holder ~4 s to actually acquire.
Start-Sleep -Seconds 4

# T2: contender via the pipeline with everything skipped.
Write-Host "Launching T2 (contender)..."
$t2Start = Get-Date
& powershell.exe -ExecutionPolicy Bypass -File $runPipeline `
    --verbose --skip-prices --skip-scryfall --skip-identifiers `
    --skip-current-refresh --skip-gap-check `
    *> $t2Log
$t2ExitCode = $LASTEXITCODE
$t2WallSec  = ((Get-Date) - $t2Start).TotalSeconds
Write-Host ("T2 exit code = {0} after {1:N1}s" -f $t2ExitCode, $t2WallSec)

# Wait for holder to finish releasing.
$t1.WaitForExit()
Write-Host "--- T1 output ---"
Get-Content $t1Log

Write-Host "--- T2 output (tail) ---"
if (Test-Path $t2Log) {
  Get-Content $t2Log | Select-Object -Last 25
}

# Verify success criteria
$pass = ($t2ExitCode -eq 4) -and ($t2WallSec -lt 15)
$verdict = if ($pass) { "PASS" } else { "FAIL" }
Write-Host ("Gate 9 verdict: {0} (expected exit=4 + wall<15s; got exit={1} wall={2:N1}s)" -f $verdict, $t2ExitCode, $t2WallSec)
if ($pass) { exit 0 } else { exit 1 }
