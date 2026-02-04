# run_rentcafe_all.ps1
# Runs all RentCafe .env files and prints a summary (success/fail + units extracted).
# Uses push_rentcafe_snapshot_to_supabase_v7.js

$ErrorActionPreference = "Stop"

$JsFile = ".\push_rentcafe_snapshot_to_supabase_v7.js"
if (-not (Test-Path $JsFile)) {
  Write-Host "JS file not found:"
  Write-Host $JsFile
  exit 1
}

$envFiles = Get-ChildItem -Path . -Filter ".env.rentcafe.*" | Where-Object { -not $_.PSIsContainer }

if ($envFiles.Count -eq 0) {
  Write-Host "No .env.rentcafe.* files found in current directory."
  exit 0
}

# Log file
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = ".\rentcafe_all_run_$timestamp.log"

Write-Host "Running RentCafe ALL properties (v6) - hardened"
Write-Host "Found $($envFiles.Count) env files"
Write-Host "Log:"
Write-Host $logFile
Write-Host ""

# Summary table
$results = @()

function Clear-RentCafeEnv {
  Get-ChildItem Env:* | ForEach-Object {
    if ($_.Name -match "^RENTCAFE_" -or $_.Name -match "^(EVENT_SOURCE|SNAPSHOT_DATE|SKIP|SKIP_REASON)$") {
      Remove-Item ("Env:" + $_.Name) -ErrorAction SilentlyContinue
    }
  }
}

function Load-EnvFile($path) {
  Get-Content $path | ForEach-Object {
    $line = $_.Trim()
    if (-not $line) { return }
    if ($line.StartsWith("#")) { return }

    $parts = $line -split "=", 2
    if ($parts.Length -eq 2) {
      $key = $parts[0].Trim()
      $val = $parts[1].Trim()

      # strip optional surrounding quotes
      if ($val.StartsWith('"') -and $val.EndsWith('"')) { $val = $val.Trim('"') }
      if ($val.StartsWith("'") -and $val.EndsWith("'")) { $val = $val.Trim("'") }

      [System.Environment]::SetEnvironmentVariable($key, $val, "Process")
    }
  }
}

foreach ($envFile in $envFiles) {
  Write-Host "=============================="
  Write-Host "Processing:"
  Write-Host $envFile.Name
  Write-Host "=============================="

  Clear-RentCafeEnv
  Load-EnvFile $envFile.FullName

  # Optional env-controlled skip
  if ($env:SKIP -eq "1" -or $env:SKIP -eq "true") {
    Write-Host "⏭ SKIPPED (SKIP=1) $($env:SKIP_REASON)"
    Add-Content -Path $logFile -Value "`n[SKIP] $($envFile.Name) SKIP=1 Reason=$($env:SKIP_REASON)`n"

    $results += [PSCustomObject]@{
      EnvFile = $envFile.Name
      Status = "SKIPPED"
      ExitCode = 0
      SnapshotDate = $null
      UnitsExtracted = $null
      PropertyId = $env:RENTCAFE_PROPERTY_ID
    }
    Write-Host ""
    continue
  }

  # Quick sanity display
  Write-Host "RENTCAFE_PROPERTY_ID:"
  Write-Host $env:RENTCAFE_PROPERTY_ID
  Write-Host "RENTCAFE_URL:"
  Write-Host $env:RENTCAFE_URL
  Write-Host "RENTCAFE_URLS:"
  Write-Host $env:RENTCAFE_URLS
  Write-Host ""

  $output = @()
  $exitCode = 1
  $status = "FAILED"

  try {
    # Run JS and capture output (DO NOT let a single failure stop the loop)
    $output = node $JsFile 2>&1 | Tee-Object -FilePath $logFile -Append

    $exitCode = $LASTEXITCODE
    $status = if ($exitCode -eq 0) { "SUCCESS" } else { "FAILED" }
  }
  catch {
    # PowerShell threw a terminating error (common with Node stderr + $ErrorActionPreference=Stop)
    $exitCode = 1
    $status = "FAILED"

    $msg = $_.Exception.Message
    Write-Host "❌ Exception (continuing): $msg" -ForegroundColor Red
    Add-Content -Path $logFile -Value "`n[EXCEPTION] $($envFile.Name): $msg`n"
  }

  # Parse units extracted + snapshot date from output
  $unitsExtracted = $null
  $snapshotDate = $null

  foreach ($line in $output) {
    if ($line -match "Units extracted:\s+(\d+)") {
      $unitsExtracted = [int]$Matches[1]
    }
    if ($line -match "Diff results for\s+(\d{4}-\d{2}-\d{2})") {
      $snapshotDate = $Matches[1]
    }
  }

  $results += [PSCustomObject]@{
    EnvFile = $envFile.Name
    Status = $status
    ExitCode = $exitCode
    SnapshotDate = $snapshotDate
    UnitsExtracted = $unitsExtracted
    PropertyId = $env:RENTCAFE_PROPERTY_ID
  }

  Write-Host ""
  Write-Host "Result:"
  Write-Host $status
  Write-Host "Units extracted:"
  Write-Host $unitsExtracted
  Write-Host ""
}

# Print summary
Write-Host "========================================"
Write-Host "SUMMARY"
Write-Host "========================================"

$results | Sort-Object Status, EnvFile | Format-Table -AutoSize EnvFile, Status, ExitCode, SnapshotDate, UnitsExtracted, PropertyId

# Totals
$successCount = ($results | Where-Object { $_.Status -eq "SUCCESS" }).Count
$failCount = ($results | Where-Object { $_.Status -eq "FAILED" }).Count
$skipCount = ($results | Where-Object { $_.Status -eq "SKIPPED" }).Count

Write-Host ""
Write-Host "Total SUCCESS:"
Write-Host $successCount
Write-Host "Total FAILED:"
Write-Host $failCount
Write-Host "Total SKIPPED:"
Write-Host $skipCount
Write-Host ""
Write-Host "Log saved to:"
Write-Host $logFile

# Exit non-zero if any failed (useful for scheduling)
if ($failCount -gt 0) {
  exit 1
}
exit 0
