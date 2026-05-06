#Requires -Version 5.1
<#
 Coloca un acceso en "Inicio" de tu usuario para el listener Telegram (menú con botones).
 Ejecutá con doble clic. No necesita administrador.
#>

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$vbsPath  = Join-Path $PSScriptRoot 'start_menu_bot_silent.vbs'
$startup  = [Environment]::GetFolderPath('Startup')

if (-not (Test-Path $vbsPath)) {
    Write-Error "No encuentro start_menu_bot_silent.vbs en $PSScriptRoot"
    exit 1
}

if (-not (Get-Command wscript.exe -ErrorAction SilentlyContinue)) {
    Write-Error "wscript.exe no disponible."
    exit 1
}

$wsh      = New-Object -ComObject WScript.Shell
$shortcutPath = Join-Path $startup 'Betfree Telegram menu-bot.lnk'
$sc               = $wsh.CreateShortcut($shortcutPath)
$sc.TargetPath    = 'wscript.exe'
$sc.Arguments     = "//B //Nologo ""$vbsPath"""
$sc.WorkingDirectory = $repoRoot
$sc.Description   = 'Betfree: Telegram menu-bot (--menu-bot) en segundo plano'
try { $sc.IconLocation = '%SystemRoot%\System32\SHELL32.dll,194' } catch { }
$sc.Save()

Write-Host "Listo: acceso en Inicio para el menú-bot."
Write-Host "  $shortcutPath"
Write-Host ""
Write-Host "Log app:       $repoRoot\data\logs\menu_bot.log"
Write-Host "Log launcher: $repoRoot\data\logs\menu_bot_launcher.log"
Write-Host ""
Write-Host "Opcional (.env): MENU_BOT_NOTIFY_ON_START=true solo si querés Telegram al encender."
