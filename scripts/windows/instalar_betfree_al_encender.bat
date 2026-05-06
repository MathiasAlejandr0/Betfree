@echo off
cd /d "%~dp0"
echo Instalando tarea "Betfree al encender PC" (digest + GitHub Pages)...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0instalar_betfree_al_encender.ps1" %*
if errorlevel 1 pause
exit /b %ERRORLEVEL%
