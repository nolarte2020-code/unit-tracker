# run_sightmap_all.ps1
$ErrorActionPreference = "Stop"

# Use a unique lock for THIS script (prevents collision with rentcafe runner)
$lockFile = ".run_sightmap_all.lock"

if (Test-Path $lockFile) {
  Write-Host "❌ Already running (lock exists: $lockFile). Exiting."
  exit 0
}

# Create lock immediately
New-Item -ItemType File -Path $lockFile -Force | Out-Null

function Get-EnvValueFromFile($filePath, $key) {
  $line = Get-Content $filePath | Where-Object { $_ -match "^\s*$key\s*=" } | Select-Object -First 1
  if (-not $line) { return $null }
  $val = $line -replace "^\s*$key\s*=\s*", ""
  return $val.Trim()
}

# ✅ Full SightMap env file list (from your latest inventory)
$envFiles = @(
  ".env.sightmap.5600wilshire_essex",
  ".env.sightmap.allegro_essex",
  ".env.sightmap.arborsatpr_essex",
  ".env.sightmap.ashton_essex",
  ".env.sightmap.avalon_oak_creek",
  ".env.sightmap.avery_essex",
  ".env.sightmap.avondale_essex",
  ".env.sightmap.blossomplaza",
  ".env.sightmap.camarillooaks_essex",
  ".env.sightmap.caminoruiz_essex",
  ".env.sightmap.columbiasquare",
  ".env.sightmap.desmondapts",
  ".env.sightmap.emerson_essex",
  ".env.sightmap.hacienda_essex",
  ".env.sightmap.hillcrestpark_essex",
  ".env.sightmap.meadowood_essex",
  ".env.sightmap.metloftsapts",
  ".env.sightmap.miraclemile_essex",
  ".env.sightmap.muse_essex",
  ".env.sightmap.regency_essex",
  ".env.sightmap.reveal_essex",
  ".env.sightmap.sofi_warnercenter",
  ".env.sightmap.theblakela_essex",
  ".env.sightmap.thedylan_essex",
  ".env.sightmap.thegarey",
  ".env.sightmap.thehuxley_essex",
  ".env.sightmap.thepearl",
  ".env.sightmap.thepointeatwarnercenter",
  ".env.sightmap.tierravista_essex",
  ".env.sightmap.tiffanycourt_essex",
  ".env.sightmap.velaonox_essex",
  ".env.sightmap.vert",
  ".env.sightmap.wallaceonsunset_essex",
  ".env.sightmap.wilshirelabrea_essex"
)

try {
  foreach ($f in $envFiles) {
    Write-Host ""
    Write-Host "==============================="
    Write-Host "Running SightMap for $f"
    Write-Host "==============================="

    # Optional but helpful: fail fast if someone misspelled a file
    if (-not (Test-Path $f)) {
      throw "Env file not found: $f"
    }

    # Node will use this to load the env file
    $env:dotenv_config_path = $f

    # 1) Push snapshot
    node -r dotenv/config .\push_sightmap_snapshot_to_supabase_v2.js
    if ($LASTEXITCODE -ne 0) { throw "Snapshot push failed for $f" }

    # 2) Diff only THIS property
    $propId = Get-EnvValueFromFile $f "SIGHTMAP_PROPERTY_ID"
    if (-not $propId) { throw "Could not read SIGHTMAP_PROPERTY_ID from $f" }

    $env:PROPERTY_ID = $propId
    $env:EVENT_SOURCE = "snapshot"

    node -r dotenv/config .\diff_snapshots_to_unit_events.js
    if ($LASTEXITCODE -ne 0) { throw "Diff failed for $f" }

    # Cleanup per loop
    Remove-Item Env:PROPERTY_ID -ErrorAction SilentlyContinue
    Remove-Item Env:EVENT_SOURCE -ErrorAction SilentlyContinue
  }

  Write-Host ""
  Write-Host "DONE: All SightMap properties processed."
}
finally {
  # Always remove lock even if script fails
  if (Test-Path $lockFile) {
    Remove-Item $lockFile -Force
  }

  # Also clear dotenv path (nice hygiene)
  Remove-Item Env:dotenv_config_path -ErrorAction SilentlyContinue
}
