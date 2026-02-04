
param(
  [Parameter(Mandatory = $true)]
  [string]$EnvFile
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $EnvFile)) {
  Write-Host "Env file not found:"
  Write-Host $EnvFile
  exit 1
}

# Clear env bleed-over (ProspectPortal + shared)
Get-ChildItem Env:* | ForEach-Object {
  if ($_.Name -match "^PROSPECTPORTAL_" -or $_.Name -match "^(PROPERTY_ID|EVENT_SOURCE|SNAPSHOT_DATE)$") {
    Remove-Item ("Env:" + $_.Name) -ErrorAction SilentlyContinue
  }
}

# Load env vars
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

Write-Host "Loaded env"
Write-Host $EnvFile
Write-Host "PROPERTY_ID:"
Write-Host $env:PROPERTY_ID
Write-Host "PROSPECTPORTAL_URL:"
Write-Host $env:PROSPECTPORTAL_URL
Write-Host "PROSPECTPORTAL_SNIFF_JSON:"
Write-Host $env:PROSPECTPORTAL_SNIFF_JSON

Write-Host "Running ProspectPortal snapshot v3"
node .\push_prospectportal_snapshot_to_supabase_v3.js

if ($LASTEXITCODE -ne 0) {
  Write-Host "ProspectPortal runner failed"
  exit $LASTEXITCODE
}

Write-Host "ProspectPortal runner finished successfully"
