#Requires -Version 5.1
<#
  Instala en un solo paso lo habitual en Windows:
  1) Acceso en "Inicio" del usuario → menú Telegram (--menu-bot) sin ventanas.
  2) Tarea programada diaria → digest + envío (--run-once) a la hora indicada.

  No requiere administrador salvo que el Programador de tareas lo pida en tu PC.
  Ejecutar una vez tras configurar .env (TELEGRAM_*, TELEGRAM_DIGEST_MENU=true para menú).

  Ejemplos:
    powershell -ExecutionPolicy Bypass -File .\instalar_betfree_autostart_completo.ps1
    powershell -ExecutionPolicy Bypass -File .\instalar_betfree_autostart_completo.ps1 -HoraDigest "09:00"
#>

param(
    [string] $HoraDigest = '08:30'
)

$ErrorActionPreference = 'Stop'

$scriptsDir = $PSScriptRoot
$repoRoot   = (Resolve-Path (Join-Path $scriptsDir '..\..')).Path
$vbsPath    = Join-Path $scriptsDir 'start_menu_bot_silent.vbs'
$startup    = [Environment]::GetFolderPath('Startup')

function Get-BetfreePythonLaunch {
    $venv1 = Join-Path $repoRoot 'venv\Scripts\python.exe'
    $venv2 = Join-Path $repoRoot '.venv\Scripts\python.exe'
    if (Test-Path -LiteralPath $venv1) {
        return @{ Executable = $venv1; Arguments = '-m src.free_digest_app --run-once' }
    }
    if (Test-Path -LiteralPath $venv2) {
        return @{ Executable = $venv2; Arguments = '-m src.free_digest_app --run-once' }
    }
    if ($cmd = Get-Command 'python' -ErrorAction SilentlyContinue) {
        return @{ Executable = $cmd.Source; Arguments = '-m src.free_digest_app --run-once' }
    }
    if ($cmd = Get-Command 'py' -ErrorAction SilentlyContinue) {
        return @{ Executable = $cmd.Source; Arguments = '-3 -m src.free_digest_app --run-once' }
    }
    return $null
}

# --- 1) Menú-bot en carpeta Inicio ---
if (-not (Test-Path -LiteralPath $vbsPath)) {
    Write-Error "No encuentro start_menu_bot_silent.vbs en $scriptsDir"
    exit 1
}
if (-not (Get-Command wscript.exe -ErrorAction SilentlyContinue)) {
    Write-Error 'wscript.exe no disponible.'
    exit 1
}

$wsh = New-Object -ComObject WScript.Shell
$shortcutPath = Join-Path $startup 'Betfree Telegram menu-bot.lnk'
$sc = $wsh.CreateShortcut($shortcutPath)
$sc.TargetPath = 'wscript.exe'
$sc.Arguments = "//B //Nologo `"$vbsPath`""
$sc.WorkingDirectory = $repoRoot
$sc.Description = 'Betfree: Telegram menu-bot (--menu-bot) en segundo plano'
try { $sc.IconLocation = '%SystemRoot%\System32\SHELL32.dll,194' } catch { }
$sc.Save()
Write-Host "[OK] Inicio de Windows → menú-bot (sin ventana)"
Write-Host "     $shortcutPath"

# --- 2) Tarea diaria digest ---
$nombre = 'Betfree Digest diario Telegram'
$launch = Get-BetfreePythonLaunch
if ($null -eq $launch) {
    Write-Error 'No encontré Python (venv, .venv, python ni py). Instalá dependencias y reintentá.'
    exit 1
}

$taskAction   = New-ScheduledTaskAction -Execute $launch.Executable -Argument $launch.Arguments.Trim() -WorkingDirectory $repoRoot
$taskTrigger  = New-ScheduledTaskTrigger -Daily -At $HoraDigest
$taskSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 3) -StartWhenAvailable
$principal    = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive

try {
    Register-ScheduledTask -TaskName $nombre -Action $taskAction -Trigger $taskTrigger -Settings $taskSettings -Principal $principal -Force | Out-Null
    Write-Host "[OK] Tarea programada: '$nombre' cada día a $HoraDigest (zona horaria de Windows)."
} catch {
    Write-Warning 'No se pudo registrar la tarea. Probá PowerShell como administrador o creala a mano en el Programador de tareas.'
    throw
}

Write-Host ""
Write-Host "Repo: $repoRoot"
Write-Host "Logs menú-bot: $repoRoot\data\logs\menu_bot.log"
Write-Host "Log arranque:  $repoRoot\data\logs\menu_bot_launcher.log"
Write-Host ""
Write-Host "Recordatorio .env:"
Write-Host "  - TELEGRAM_DIGEST_MENU=true si usás botones del menú."
Write-Host "  - Tras cambiar código/dependencias, conviene reiniciar sesión o matar pythonw del menú-bot y volver a iniciar."
Write-Host ""
Write-Host "Desinstalar todo: scripts\windows\desinstalar_betfree_autostart_completo.ps1"
