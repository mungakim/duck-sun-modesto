@echo off
TITLE Duck Sun Modesto - Consensus Triangulation System
COLOR 0B
CLS

ECHO ========================================================
ECHO    MODESTO SOLAR TRIANGULATION ENGINE
ECHO    Duck Sun Modesto: Uncanny Edition
ECHO ========================================================
ECHO    [SOURCES] Open-Meteo + NWS + Met.no + METAR
ECHO.

:: Change to script directory
cd /d "%~dp0"

:: Check for VENV
IF EXIST "venv\Scripts\activate.bat" (
    ECHO [INFO] Activating virtual environment...
    CALL venv\Scripts\activate.bat
) ELSE (
    ECHO [INFO] Running with system Python...
)

:: Install/Check Requirements (Fast check, suppress output)
pip install -r requirements.txt >NUL 2>&1

:: Run the Uncanny Edition main script
python main.py

:: Auto-open the latest generated PDF report
ECHO.
ECHO [INFO] Opening latest PDF forecast...
FOR /F "delims=" %%I IN ('DIR "reports\*.pdf" /B /O:D 2^>NUL') DO SET LAST_PDF=%%I
IF DEFINED LAST_PDF (
    START "" "reports\%LAST_PDF%"
    ECHO [SUCCESS] Opened PDF: %LAST_PDF%
) ELSE (
    ECHO [INFO] No PDF found, trying markdown...
    FOR /F "delims=" %%I IN ('DIR "reports\*.md" /B /O:D') DO SET LAST_REPORT=%%I
    IF DEFINED LAST_REPORT (
        START "" "reports\%LAST_REPORT%"
        ECHO [SUCCESS] Opened: %LAST_REPORT%
    ) ELSE (
        ECHO [WARNING] No reports found in reports\ directory
    )
)

ECHO.
ECHO    Briefing Generated. Press any key to exit.
PAUSE >NUL
