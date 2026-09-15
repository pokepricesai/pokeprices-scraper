# scripts/run_pipeline.ps1
#
# Loads SUPABASE_URL / SUPABASE_SERVICE_KEY from ../pokeprices-web/.env.local
# and runs mtg_daily_pipeline.py with the passed args. Sets the two
# ingestion enable flags too. Used by the Stage 1D manual gates.

$envFile = Join-Path $PSScriptRoot "..\..\pokeprices-web\.env.local"
if (-not (Test-Path -LiteralPath $envFile)) {
  Write-Error "env file not found at $envFile"
  exit 2
}
Get-Content -LiteralPath $envFile | ForEach-Object {
  if ($_ -match '^([A-Za-z0-9_]+)\s*=\s*(.*)$') {
    $key = $Matches[1]
    $val = $Matches[2]
    if ($val -match '^"(.*)"$' -or $val -match "^'(.*)'$") { $val = $Matches[1] }
    Set-Item -Path "Env:$key" -Value $val
  }
}
if (-not $env:SUPABASE_SERVICE_KEY -and $env:SUPABASE_SERVICE_ROLE_KEY) {
  $env:SUPABASE_SERVICE_KEY = $env:SUPABASE_SERVICE_ROLE_KEY
}
if (-not $env:SUPABASE_URL -and $env:NEXT_PUBLIC_SUPABASE_URL) {
  $env:SUPABASE_URL = $env:NEXT_PUBLIC_SUPABASE_URL
}
$env:MTG_DAILY_INGEST_ENABLED = 'true'
$env:MTG_PRICING_INGESTION_ENABLED = 'true'

$pipeline = Join-Path $PSScriptRoot "..\mtg_daily_pipeline.py"
& python $pipeline @args
exit $LASTEXITCODE
