@echo off
set HOST=0.0.0.0
if "%PORT%"=="" set PORT=8080
python app.py
pause
