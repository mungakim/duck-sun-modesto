@echo off
REM ============================================================================
REM Duck Sun Modesto - Daily Weather Report Scheduler (Batch Version)
REM Fallback script if PowerShell gives issues.
REM ============================================================================

REM ============================================================================
REM CONFIGURATION - UPDATE THIS PATH TO YOUR PROJECT LOCATION
REM ============================================================================
set PROJECT_DIR=C:\Users\YourUsername\duck-sun-modesto
REM ============================================================================

echo ============================================================
echo Duck Sun Modesto - Scheduled Run Starting
echo %date% %time%
echo ============================================================

REM Change to project directory
cd /d "%PROJECT_DIR%"
if errorlevel 1 (
    echo ERROR: Could not change to project directory: %PROJECT_DIR%
    exit /b 1
)

echo Working directory: %CD%

REM Pull latest changes
echo Pulling latest changes...
git pull origin main

REM Run the weather scheduler
echo Running weather scheduler...
"%PROJECT_DIR%\venv\Scripts\python.exe" -m duck_sun.scheduler
if errorlevel 1 (
    echo ERROR: Weather scheduler failed
    exit /b 1
)

echo Weather scheduler completed successfully

REM Stage files
echo Staging output files...
git add reports\*.pdf 2>nul
git add reports\**\*.pdf 2>nul
git add outputs\*.json 2>nul
git add verification.db 2>nul
git add LEADERBOARD.md 2>nul

REM Check for changes
git diff --cached --quiet
if errorlevel 1 (
    REM There are staged changes, commit them
    echo Creating commit...

    REM Get today's date in YYYY-MM-DD format
    for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
    set TODAY=%datetime:~0,4%-%datetime:~4,2%-%datetime:~6,2%

    git config user.name "DuckSunBot"
    git config user.email "ducksunbot@local"
    git commit -m "Forecast: %TODAY%"

    echo Pushing to GitHub...
    git push origin main
    if errorlevel 1 (
        echo WARNING: First push attempt failed, retrying...
        timeout /t 2 /nobreak >nul
        git push origin main
        if errorlevel 1 (
            echo WARNING: Second push attempt failed, retrying...
            timeout /t 4 /nobreak >nul
            git push origin main
            if errorlevel 1 (
                echo ERROR: Git push failed after retries
                exit /b 1
            )
        )
    )
    echo Push successful!
) else (
    echo No changes to commit - all up to date
)

echo ============================================================
echo SUCCESS - Completed
echo ============================================================
exit /b 0
