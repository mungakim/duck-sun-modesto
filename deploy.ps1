# Full deploy pipeline: push code to GitHub, build exe, sign, copy to X: network drive.
# Usage: .\deploy.ps1
#
# This is the "ship a new version" script. Run it after committing code changes.
# It does NOT run the daily forecast - for that, use .\run_and_push.ps1.

$ErrorActionPreference = "Stop"

# STEP 1: Push pending code commits to GitHub
Write-Host "=== STEP 1/2: Syncing code to GitHub ===" -ForegroundColor Cyan
$branch = (git rev-parse --abbrev-ref HEAD).Trim()
Write-Host "Current branch: $branch" -ForegroundColor Gray

# Does the branch exist on the remote yet?
git ls-remote --exit-code --heads origin $branch *> $null
$branchExistsOnRemote = ($LASTEXITCODE -eq 0)
$global:LASTEXITCODE = 0

if (-not $branchExistsOnRemote) {
    Write-Host "Branch $branch not on remote; pushing with -u" -ForegroundColor Yellow
    git push -u origin $branch
} else {
    git fetch origin $branch *> $null
    $aheadStr = (git rev-list --count "origin/$branch..HEAD").Trim()
    $ahead = [int]$aheadStr
    if ($ahead -gt 0) {
        Write-Host "$ahead local commit(s) ahead of origin/$branch; pushing..." -ForegroundColor Yellow
        git push origin $branch
    } else {
        Write-Host "origin/$branch is up to date - nothing to push" -ForegroundColor Gray
    }
}

# STEP 2: Build + sign + deploy exe
Write-Host ""
Write-Host "=== STEP 2/2: Building + signing + deploying exe to X: ===" -ForegroundColor Cyan
& .\build_exe.ps1
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "exe build/deploy failed - see above for errors." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "================================================" -ForegroundColor Green
Write-Host "  DEPLOY COMPLETE" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host "  Code pushed to: origin/$branch" -ForegroundColor Gray
Write-Host "  exe deployed to: X:\Operatns\Pwrsched\Weather\DuckSunForecast.exe" -ForegroundColor Gray
Write-Host "  Next scheduled run on X: will use the new version." -ForegroundColor Gray
