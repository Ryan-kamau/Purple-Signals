@echo off
cd /d "%~dp0.."
if not exist logs mkdir logs

echo ===== %date% %time% ===== >> logs\signals.log
venv\Scripts\python.exe -m scripts.signal_scheduler >> logs\signals.log 2>&1
exit /b %ERRORLEVEL%