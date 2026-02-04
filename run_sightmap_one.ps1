param(
  [Parameter(Mandatory=$true)]
  [string]$EnvFile
)

if (-not (Test-Path $EnvFile)) {
  Write-Host "❌ Env file not found: $EnvFile"
  exit 1
}

Get-Content $EnvFile | ForEach-Object {
  if ($_ -and $_ -notmatch '^\s*#' -and $_ -match '=') {
    $name, $value = $_ -split '=', 2
    [System.Environment]::SetEnvironmentVariable($name.Trim(), $value.Trim())
  }
}

Write-Host "✅ Loaded env: $EnvFile"
Write-Host "▶ Running SightMap snapshot..."

node .\push_sightmap_snapshot_to_supabase_v2.js
