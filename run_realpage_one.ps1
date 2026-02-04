param(
  [Parameter(Mandatory=$true)]
  [string]$EnvFile
)

$ErrorActionPreference = "Stop"

function Load-DotEnv($path) {
  if (!(Test-Path $path)) { throw "Env file not found: $path" }

  Get-Content $path | ForEach-Object {
    $line = $_.Trim()
    if (!$line -or $line.StartsWith("#")) { return }
    $idx = $line.IndexOf("=")
    if ($idx -lt 1) { return }

    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim()

    # strip optional quotes
    if ($v.StartsWith('"') -and $v.EndsWith('"')) { $v = $v.Substring(1, $v.Length-2) }
    if ($v.StartsWith("'") -and $v.EndsWith("'")) { $v = $v.Substring(1, $v.Length-2) }

    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

Write-Host "Loaded env"
Write-Host $EnvFile

Load-DotEnv $EnvFile

Write-Host "PROPERTY_ID:"
Write-Host $env:PROPERTY_ID
Write-Host "REALPAGE_URL:"
Write-Host $env:REALPAGE_URL

Write-Host "Running RealPage snapshot v1"
node .\push_realpage_snapshot_to_supabase_v1.js

if ($LASTEXITCODE -eq 0) {
  Write-Host "RealPage runner finished successfully"
} else {
  Write-Host "RealPage runner failed"
  exit $LASTEXITCODE
}
