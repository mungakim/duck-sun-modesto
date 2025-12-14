@echo off
REM Duck Sun Modesto - Daily Solar Forecaster
REM Runs with interactive prompts for MID Gas Burn and PGE Citygate Price

cd /d "C:\Professional Projects\duck-sun-modesto"
call venv\Scripts\activate
python main.py
pause
