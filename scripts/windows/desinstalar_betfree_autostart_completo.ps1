#Requires -Version 5.1
<#
  Quita el acceso de Inicio del menú-bot, la tarea 'Betfree Digest diario Telegram'
  y la tarea 'Betfree al encender PC' (digest + Pages al iniciar sesión).
#>

$ErrorActionPreference = 'Continue'

$startup = [Environment]::GetFolderPath('Startup')
$lnk     = Join-Path $startup 'Betfree Telegram menu-bot.lnk'
if (Test-Path -LiteralPath $lnk) {
    Remove-Item -LiteralPath $lnk -Force
    Write-Host "Eliminado: $lnk"
} else {
    Write-Host "No había acceso en Inicio: $lnk"
}

$nombre = 'Betfree Digest diario Telegram'
try {
    Unregister-ScheduledTask -TaskName $nombre -Confirm:$false -ErrorAction Stop
    Write-Host "Tarea eliminada: $nombre"
} catch {
    Write-Host "Tarea no registrada o sin permiso: $nombre"
}

$nombre2 = 'Betfree al encender PC'
try {
    Unregister-ScheduledTask -TaskName $nombre2 -Confirm:$false -ErrorAction Stop
    Write-Host "Tarea eliminada: $nombre2"
} catch {
    Write-Host "Tarea no registrada o sin permiso: $nombre2"
}
