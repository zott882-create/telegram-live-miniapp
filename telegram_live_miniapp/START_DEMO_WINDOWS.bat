@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist .venv (
  py -m venv .venv
)
call .venv\Scripts\activate.bat
python -m pip install -r requirements.txt
set API_FOOTBALL_DEMO=1
python combined_app.py
pause
