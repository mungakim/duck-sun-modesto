# Run forecast and push to GitHub
# Usage: .\run_and_push.ps1

$ErrorActionPreference = "Stop"

Write-Host "=== Running forecast ===" -ForegroundColor Cyan
& .\venv\Scripts\python.exe -m duck_sun.scheduler

if ($LASTEXITCODE -ne 0) {
    Write-Host "Forecast failed!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Committing outputs ===" -ForegroundColor Cyan
git add outputs/ reports/

$changes = git diff --cached --quiet; $hasChanges = $LASTEXITCODE -ne 0

if ($hasChanges) {
    $date = Get-Date -Format "yyyy-MM-dd"
    git commit -m "Forecast: $date"

    Write-Host ""
    Write-Host "=== Pushing to GitHub ===" -ForegroundColor Cyan
    git push
} else {
    Write-Host "No new files to commit"
}

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
