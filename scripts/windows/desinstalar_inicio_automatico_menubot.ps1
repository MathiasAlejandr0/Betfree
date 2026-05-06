#Requires -Version 5.1

$startup = [Environment]::GetFolderPath('Startup')
$lnk     = Join-Path $startup 'Betfree Telegram menu-bot.lnk'

if (Test-Path $lnk) {
    Remove-Item -LiteralPath $lnk -Force
    Write-Host "Eliminado: $lnk"
} else {
    Write-Host "No estaba instalado ese acceso: $lnk"
}
