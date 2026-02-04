param (
  [string]$EnvFile
)

if (-not $EnvFile) {
  Write-Host "❌ Please provide an env file"
  exit 1
}

Get-Content $EnvFile | ForEach-Object {
  if ($_ -and $_ -notmatch '^#') {
    $name, $value = $_ -split '=', 2
    [System.Environment]::SetEnvironmentVariable($name, $value)
  }
}

node run_sightmap.js
