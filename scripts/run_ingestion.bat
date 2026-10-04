@echo off
cd /d "%~dp0.."
if not exist logs mkdir logs

echo ===== %date% %time% ===== >> logs\ingestion.log
venv\Scripts\python.exe -m scripts.ingestion_scheduler >> logs\ingestion.log 2>&1
exit /b %ERRORLEVEL%