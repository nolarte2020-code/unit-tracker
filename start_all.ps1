# start_all.ps1 (runs scrapers + starts React dev server)
$ErrorActionPreference = "Stop"

$root = "C:\Users\wiz30\unit-tracker\app"

# 1) Start scrapers (new window)
Start-Process powershell.exe -ArgumentList @(
  "-NoProfile",
  "-ExecutionPolicy", "Bypass",
  "-NoExit",
  "-Command",
  "cd `"$root`"; .\run_all.ps1"
)

# 2) Start React dev server (new window)
Start-Process powershell.exe -ArgumentList @(
  "-NoProfile",
  "-ExecutionPolicy", "Bypass",
  "-NoExit",
  "-Command",
  "cd `"$root`"; npm run dev"
)

# Optional: open browser (uncomment if you want)
# Start-Process "http://localhost:5173"
