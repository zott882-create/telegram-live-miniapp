@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist .venv (
  py -m venv .venv
)
call .venv\Scripts\activate.bat
python -m pip install -r requirements.txt
if "%API_FOOTBALL_KEY%"=="" (
  set /p API_FOOTBALL_KEY=Введите API-Football key: 
)
set API_FOOTBALL_DEMO=0
python combined_app.py
pause
