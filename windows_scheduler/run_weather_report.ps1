#Requires -Version 5.1
<#
.SYNOPSIS
    Duck Sun Modesto - Daily Weather Report Scheduler
    Runs the weather scraper and auto-pushes results to GitHub.

.DESCRIPTION
    This script is designed to run via Windows Task Scheduler every morning.
    It fetches weather data from 11 sources (including scraped sites that block
    datacenter IPs), generates a PDF report, and pushes to GitHub.

.NOTES
    Author: Duck Sun Modesto
    Requires: Git installed and authenticated via Windows Credential Manager
#>

# ============================================================================
# CONFIGURATION - UPDATE THIS PATH TO YOUR PROJECT LOCATION
# ============================================================================
$ProjectDir = "C:\Projects\duck-sun-modesto"
# ============================================================================

$ErrorActionPreference = "Stop"
$ScriptStartTime = Get-Date

# Create logs directory if it doesn't exist
$LogDir = Join-Path $ProjectDir "logs"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# Log file with date
$LogFile = Join-Path $LogDir "scheduler_$(Get-Date -Format 'yyyy-MM-dd').log"

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogLine = "[$Timestamp] [$Level] $Message"
    Add-Content -Path $LogFile -Value $LogLine

    # Also write to console with color
    switch ($Level) {
        "ERROR"   { Write-Host $LogLine -ForegroundColor Red }
        "WARNING" { Write-Host $LogLine -ForegroundColor Yellow }
        "SUCCESS" { Write-Host $LogLine -ForegroundColor Green }
        default   { Write-Host $LogLine }
    }
}

function Invoke-GitPushWithRetry {
    param(
        [int]$MaxRetries = 4,
        [int[]]$DelaySeconds = @(2, 4, 8, 16)
    )

    $originalErrorPref = $ErrorActionPreference

    for ($i = 0; $i -lt $MaxRetries; $i++) {
        Write-Log "Git push attempt $($i + 1)/$MaxRetries..."
        $ErrorActionPreference = "Continue"
        $output = & git push origin main 2>&1
        $pushExitCode = $LASTEXITCODE
        $ErrorActionPreference = $originalErrorPref

        if ($pushExitCode -eq 0) {
            Write-Log "Git push successful" "SUCCESS"
            return $true
        }

        Write-Log "Git push failed: $output" "WARNING"
        if ($i -lt $MaxRetries - 1) {
            $delay = $DelaySeconds[$i]
            Write-Log "Retrying in $delay seconds..."
            Start-Sleep -Seconds $delay
        }
    }

    Write-Log "Git push failed after $MaxRetries attempts" "ERROR"
    return $false
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

try {
    Write-Log "=" * 60
    Write-Log "Duck Sun Modesto - Scheduled Run Starting"
    Write-Log "=" * 60

    # Validate project directory
    if (-not (Test-Path $ProjectDir)) {
        throw "Project directory not found: $ProjectDir`nPlease update the `$ProjectDir variable in this script."
    }

    # Change to project directory
    Set-Location $ProjectDir
    Write-Log "Working directory: $ProjectDir"

    # Validate Python venv exists
    $PythonExe = Join-Path $ProjectDir "venv\Scripts\python.exe"
    if (-not (Test-Path $PythonExe)) {
        throw "Python executable not found: $PythonExe`nMake sure the virtual environment is set up."
    }
    Write-Log "Python: $PythonExe"

    # Validate git is available
    $gitVersion = git --version 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Git is not installed or not in PATH"
    }
    Write-Log "Git: $gitVersion"

    # Pull latest changes first (avoid conflicts)
    Write-Log "Pulling latest changes from origin..."
    try {
        $ErrorActionPreference = "Continue"
        $pullOutput = & git pull origin main 2>&1
        $ErrorActionPreference = "Stop"
        $pullOutput | ForEach-Object { Write-Log "  $_" }
        if ($LASTEXITCODE -ne 0) {
            Write-Log "Git pull failed (non-fatal, continuing...)" "WARNING"
        }
    } catch {
        Write-Log "Git pull warning: $_" "WARNING"
    }

    # ========================================================================
    # STEP 1: Run the weather report scheduler
    # ========================================================================
    Write-Log ""
    Write-Log "STEP 1: Running weather report scheduler..."
    Write-Log "-" * 40

    # Run the scheduler
    $process = Start-Process -FilePath $PythonExe `
        -ArgumentList "-m", "duck_sun.scheduler" `
        -WorkingDirectory $ProjectDir `
        -NoNewWindow `
        -Wait `
        -PassThru `
        -RedirectStandardOutput (Join-Path $LogDir "scheduler_stdout.log") `
        -RedirectStandardError (Join-Path $LogDir "scheduler_stderr.log")

    # Check exit code
    if ($process.ExitCode -ne 0) {
        $stderr = Get-Content (Join-Path $LogDir "scheduler_stderr.log") -Raw
        Write-Log "Scheduler stderr: $stderr" "WARNING"
        throw "Weather scheduler failed with exit code $($process.ExitCode)"
    }

    Write-Log "Weather scheduler completed successfully" "SUCCESS"

    # ========================================================================
    # STEP 2: Commit and push to GitHub
    # ========================================================================
    Write-Log ""
    Write-Log "STEP 2: Committing and pushing to GitHub..."
    Write-Log "-" * 40

    # Configure git user (for this repo only)
    $ErrorActionPreference = "Continue"
    & git config user.name "DuckSunBot" 2>&1 | Out-Null
    & git config user.email "ducksunbot@local" 2>&1 | Out-Null
    $ErrorActionPreference = "Stop"

    # Stage the output files (excluding logs - they're gitignored)
    $filesToStage = @(
        "reports/*.pdf",
        "reports/**/*.pdf",
        "outputs/*.json",
        "verification.db",
        "LEADERBOARD.md"
    )

    $stagedAny = $false
    $ErrorActionPreference = "Continue"
    foreach ($pattern in $filesToStage) {
        $files = Get-ChildItem -Path $pattern -ErrorAction SilentlyContinue -Recurse
        if ($files) {
            & git add $pattern 2>&1 | Out-Null
            $stagedAny = $true
            Write-Log "Staged: $pattern ($($files.Count) files)"
        }
    }
    $ErrorActionPreference = "Stop"

    # Check if there are any changes to commit
    $ErrorActionPreference = "Continue"
    $status = & git status --porcelain 2>&1
    $ErrorActionPreference = "Stop"

    if ([string]::IsNullOrWhiteSpace($status)) {
        Write-Log "No changes to commit - all up to date" "SUCCESS"
    }
    else {
        # Create commit message
        $today = Get-Date -Format "yyyy-MM-dd"
        $commitMsg = [char]0x2600 + " Forecast: $today"  # Sun emoji + date

        Write-Log "Changes detected, creating commit..."
        $ErrorActionPreference = "Continue"
        $commitOutput = & git commit -m $commitMsg 2>&1
        $commitExitCode = $LASTEXITCODE
        $ErrorActionPreference = "Stop"
        $commitOutput | ForEach-Object { Write-Log "  $_" }

        if ($commitExitCode -ne 0) {
            Write-Log "Git commit failed (may be no changes)" "WARNING"
        }
        else {
            Write-Log "Commit created: $commitMsg" "SUCCESS"

            # Push with retry
            $pushSuccess = Invoke-GitPushWithRetry
            if (-not $pushSuccess) {
                throw "Failed to push to GitHub after all retries"
            }
        }
    }

    # ========================================================================
    # SUMMARY
    # ========================================================================
    $duration = (Get-Date) - $ScriptStartTime
    Write-Log ""
    Write-Log "=" * 60
    Write-Log "SUCCESS - Completed in $($duration.TotalSeconds.ToString('F1')) seconds" "SUCCESS"
    Write-Log "=" * 60

    exit 0
}
catch {
    Write-Log "FATAL ERROR: $_" "ERROR"
    Write-Log $_.ScriptStackTrace "ERROR"

    $duration = (Get-Date) - $ScriptStartTime
    Write-Log ""
    Write-Log "=" * 60
    Write-Log "FAILED after $($duration.TotalSeconds.ToString('F1')) seconds" "ERROR"
    Write-Log "=" * 60

    exit 1
}
