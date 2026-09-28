@echo off

cd /d "F:\Pushing Python\API\FastApi\Purple_signals"

echo ===== START %date% %time% ===== >> logs\task_output.txt

"F:\Pushing Python\API\FastApi\Purple_signals\Venv\Scripts\python.exe" -m scripts.scheduler >> logs\task_output.txt 2>&1

echo Exit code: %ERRORLEVEL% >> logs\task_output.txt
echo ===== END %date% %time% ===== >> logs\task_output.txt