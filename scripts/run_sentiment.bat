@echo off
cd /d "%~dp0.."
if not exist logs mkdir logs

echo ===== %date% %time% ===== >> logs\sentiment.log
venv\Scripts\python.exe -m scripts.sentiment_scheduler >> logs\sentiment.log 2>&1
exit /b %ERRORLEVEL%