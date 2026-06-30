@echo off
setlocal

set "REPO_ROOT=%~dp0..\.."
cd /d "%REPO_ROOT%" || exit /b 1

if not exist "logs" mkdir "logs"
set "DATABASE_PATH=data/insider_alerts.db"

echo ==== [%date% %time%] START autopilot ====>> logs\autopilot.out.log

".venv\Scripts\python.exe" -m insider_alerts.cli ops autopilot --loop --interval 300 --decision-engine quant --quant-agent-id quant-insider --quant-batch-size 8 --quant-thinking low --decision-limit 100 --notify --notify-approve-only 1>> logs\autopilot.out.log 2>> logs\autopilot.err.log

echo ==== [%date% %time%] EXIT code %errorlevel% ====>> logs\autopilot.out.log
exit /b %errorlevel%
