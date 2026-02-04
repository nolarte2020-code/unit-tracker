Write-Host "==============================="
Write-Host "Running ALL PLATFORMS"
Write-Host "==============================="

.\run_rentcafe_all.ps1
if ($LASTEXITCODE -ne 0) { throw "RentCafe batch failed" }

.\run_sightmap_all.ps1
if ($LASTEXITCODE -ne 0) { throw "SightMap batch failed" }

Write-Host ""
Write-Host " All properties processed across all platforms."
