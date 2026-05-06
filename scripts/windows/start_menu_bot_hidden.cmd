@echo off
setlocal
pushd "%~dp0..\..\"
set "ROOT=%CD%"
if not exist "data\logs" mkdir "data\logs"
set "LOG=data\logs\menu_bot_launcher.log"

REM Prioridad: entorno virtual del repo (al encender Windows el PATH suele ser mínimo).
if exist "%ROOT%\venv\Scripts\pythonw.exe" (
    "%ROOT%\venv\Scripts\pythonw.exe" -m src.free_digest_app --menu-bot >> "%LOG%" 2>&1
    goto fin
)
if exist "%ROOT%\.venv\Scripts\pythonw.exe" (
    "%ROOT%\.venv\Scripts\pythonw.exe" -m src.free_digest_app --menu-bot >> "%LOG%" 2>&1
    goto fin
)

where pythonw >nul 2>&1
if %ERRORLEVEL%==0 (
    pythonw -m src.free_digest_app --menu-bot >> "%LOG%" 2>&1
    goto fin
)
where py >nul 2>&1
if %ERRORLEVEL%==0 (
    py -3w -m src.free_digest_app --menu-bot >> "%LOG%" 2>&1
    goto fin
)

echo [%date% %time%] sin pythonw / venv\.venv ni py en PATH >> "data\logs\menu_bot_launcher.err"
:fin
popd
endlocal
