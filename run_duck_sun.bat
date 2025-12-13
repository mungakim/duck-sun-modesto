@echo off
cd /d "C:\Professional Projects\duck-sun-modesto"
call venv\Scripts\activate
python -m duck_sun.scheduler
pause
