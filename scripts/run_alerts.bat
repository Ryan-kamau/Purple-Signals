@echo off
cd /d "%~dp0.."
if not exist logs mkdir logs

echo ===== %date% %time% ===== >> logs\alerts.log
venv\Scripts\python.exe -m scripts.alert_scheduler >> logs\alerts.log 2>&1
exit /b %ERRORLEVEL%