@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0desinstalar_betfree_autostart_completo.ps1"
pause
