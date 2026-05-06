#Requires -Version 5.1
<#
  Registra una tarea al iniciar sesión en Windows:
    digest Telegram + export JSON Pages + git push (ver betfree_al_inicio.ps1).

  Requisitos previos:
    - git config user.name / user.email
    - git push ya probado manualmente (Credential Manager o token)
    - .env con TELEGRAM_*

  Si también tenés "Betfree Digest diario Telegram", podés recibir el digest dos veces al día
  (al encender + a la hora fija). Ajustá o desinstalá una de las tareas si molesta.

  Desinstalar: desinstalar_betfree_autostart_completo.ps1 (también quita esta tarea) o
  Unregister-ScheduledTask -TaskName 'Betfree al encender PC' -Confirm:$false
#>

$ErrorActionPreference = 'Stop'

$scriptsDir = $PSScriptRoot
$repoRoot   = (Resolve-Path (Join-Path $scriptsDir '..\..')).Path
$ps1        = Join-Path $scriptsDir 'betfree_al_inicio.ps1'

if (-not (Test-Path -LiteralPath $ps1)) {
    Write-Error "No encuentro betfree_al_inicio.ps1 en $scriptsDir"
    exit 1
}

$nombre = 'Betfree al encender PC'
$argLine = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$ps1`""
$taskAction   = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $argLine -WorkingDirectory $repoRoot
$taskTrigger  = New-ScheduledTaskTrigger -AtLogOn
$taskSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive

try {
    Register-ScheduledTask -TaskName $nombre -Action $taskAction -Trigger $taskTrigger -Settings $taskSettings -Principal $principal -Force | Out-Null
    Write-Host "[OK] Tarea registrada: '$nombre' (al iniciar sesión)."
    Write-Host "     Log resumen: $repoRoot\data\logs\betfree_startup.log"
    Write-Host "     Digest:      $repoRoot\data\logs\betfree_digest_last_run.log"
    Write-Host "     Export:      $repoRoot\data\logs\betfree_export_pages_last_run.log"
} catch {
    Write-Warning 'No se pudo registrar. Probá PowerShell como administrador o creá la tarea a mano.'
    throw
}
