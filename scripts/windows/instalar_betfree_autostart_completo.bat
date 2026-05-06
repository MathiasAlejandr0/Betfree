@echo off
cd /d "%~dp0"
echo Instalando inicio automatico Betfree (menu-bot + tarea diaria)...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0instalar_betfree_autostart_completo.ps1" %*
if errorlevel 1 pause
exit /b %ERRORLEVEL%
