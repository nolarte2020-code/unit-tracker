param(
  [Parameter(Mandatory = $true)]
  [string]$EnvFile
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $EnvFile)) {
  Write-Host Env file not found
  Write-Host $EnvFile
  exit 1
}

# Clear env bleed-over
Get-ChildItem Env:* | ForEach-Object {
  if ($_.Name -match "^RENTCAFE_" -or $_.Name -match "^(EVENT_SOURCE|SNAPSHOT_DATE)$") {
    Remove-Item ("Env:" + $_.Name) -ErrorAction SilentlyContinue
  }
}

# Load env vars from file
Get-Content $EnvFile | ForEach-Object {
  $line = $_.Trim()
  if (-not $line) { return }
  if ($line.StartsWith("#")) { return }

  $parts = $line -split "=", 2
  if ($parts.Length -eq 2) {
    $key = $parts[0].Trim()
    $val = $parts[1].Trim()
    [System.Environment]::SetEnvironmentVariable($key, $val, "Process")
  }
}

Write-Host Loaded env
Write-Host $EnvFile
Write-Host RENTCAFE_URL:
Write-Host $env:RENTCAFE_URL
Write-Host RENTCAFE_URLS:
Write-Host $env:RENTCAFE_URLS
Write-Host RENTCAFE_PROPERTY_ID:
Write-Host $env:RENTCAFE_PROPERTY_ID
Write-Host Running RentCafe snapshot v6

node .\push_rentcafe_snapshot_to_supabase_v6.js

if ($LASTEXITCODE -ne 0) {
  Write-Host RentCafe runner failed
  exit $LASTEXITCODE
}

Write-Host RentCafe runner finished successfully
