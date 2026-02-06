@echo off
title Duck Sun Forecast
cd /d "%~dp0"

echo ============================================
echo   Duck Sun Modesto - Daily Forecast
echo ============================================
echo.

:: Make sure we're on main branch for forecast commits
echo Switching to main branch...
git checkout main
git pull origin main

:: Run the forecast and push to GitHub
echo.
echo Running forecast...
call .\venv\Scripts\python.exe -m duck_sun.scheduler
if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Forecast failed!
    pause
    exit /b 1
)

echo.
echo Committing to GitHub...
git add outputs/ reports/
git diff --cached --quiet
if %ERRORLEVEL% neq 0 (
    for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set TODAY=%%i
    git commit -m "Forecast: %TODAY%"
    git push origin main
) else (
    echo No new files to commit
)

echo.
echo ============================================
echo   Done!
echo ============================================
echo.
echo Review output above for any errors.
pause
