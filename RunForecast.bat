@echo off
title Duck Sun Forecast
cd /d "%~dp0"

echo ============================================
echo   Duck Sun Modesto - Daily Forecast
echo ============================================
echo.

:: Run the forecast and push to GitHub
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
    for /f "tokens=*" %%i in ('powershell -command "Get-Date -Format 'yyyy-MM-dd'"') do set TODAY=%%i
    git commit -m "Forecast: %TODAY%"
    git push
) else (
    echo No new files to commit
)

echo.
echo Opening Excel report...

:: Find and open the latest xlsx file
for /f "delims=" %%F in ('powershell -command "Get-ChildItem -Path 'reports' -Filter '*.xlsx' -Recurse | Sort-Object LastWriteTime -Descending | Select-Object -First 1 -ExpandProperty FullName"') do (
    echo Opening: %%F
    start "" "%%F"
)

echo.
echo ============================================
echo   Done!
echo ============================================
timeout /t 3
