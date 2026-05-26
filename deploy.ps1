# Full deploy pipeline: push code to GitHub, build exe, sign, copy to X: network drive.
# Usage: .\deploy.ps1
#
# This is the "ship a new version" script. Run it after committing code changes.
# It does NOT run the daily forecast - for that, use .\run_and_push.ps1.
#
# Note: we intentionally leave $ErrorActionPreference at its default ('Continue').
# git writes progress / "From <url>" lines to stderr even on a successful fetch.
# With $ErrorActionPreference = 'Stop' those lines surface as NativeCommandError
# and abort the script even though git itself returned exit code 0.
# We check $LASTEXITCODE explicitly after each git call instead.

function Invoke-GitOrDie {
    param(
        [Parameter(Mandatory = $true)][string[]]$Args,
        [string]$ErrorMessage = "git command failed"
    )
    & git @Args
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: $ErrorMessage (exit $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }
}

# STEP 1: Push pending code commits to GitHub
Write-Host "=== STEP 1/2: Syncing code to GitHub ===" -ForegroundColor Cyan
$branch = (& git rev-parse --abbrev-ref HEAD).Trim()
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: git rev-parse failed - not inside a git repo?" -ForegroundColor Red
    exit 1
}
Write-Host "Current branch: $branch" -ForegroundColor Gray

# Does the branch exist on the remote yet? (silent check; ls-remote sets exit code only)
& git ls-remote --exit-code --heads origin $branch 2>&1 | Out-Null
$branchExistsOnRemote = ($LASTEXITCODE -eq 0)
$global:LASTEXITCODE = 0

if (-not $branchExistsOnRemote) {
    Write-Host "Branch $branch not on remote; pushing with -u" -ForegroundColor Yellow
    Invoke-GitOrDie -Args @('push', '-u', 'origin', $branch) -ErrorMessage "git push -u failed"
} else {
    # Fetch is chatty on stderr ("From <url>") even on success; swallow output and ignore exit noise
    & git fetch origin $branch 2>&1 | Out-Null
    $global:LASTEXITCODE = 0

    $aheadStr = (& git rev-list --count "origin/$branch..HEAD").Trim()
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: git rev-list failed" -ForegroundColor Red
        exit 1
    }
    $ahead = [int]$aheadStr
    if ($ahead -gt 0) {
        Write-Host "$ahead local commit(s) ahead of origin/$branch; pushing..." -ForegroundColor Yellow
        Invoke-GitOrDie -Args @('push', 'origin', $branch) -ErrorMessage "git push failed"
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
