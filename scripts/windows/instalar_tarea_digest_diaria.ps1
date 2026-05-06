#Requires -Version 5.1
<#
 Tarea diaria: genera agenda + menú Telegram (--run-once).
 Suele poder registrarse sin admin (tarea del usuario).

 Preferible para “todo automático”: instalar_betfree_autostart_completo.ps1 / .bat
 (menú al encender Windows + esta tarea).

 Ajustá $hora (formato 24 h). Si falla, abrí PowerShell como administrador.
#>

$hora   = '08:30'
$nombre = 'Betfree Digest diario Telegram'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

$executable = ''
$argument   = '-m src.free_digest_app --run-once'
$venv1 = Join-Path $repoRoot 'venv\Scripts\python.exe'
$venv2 = Join-Path $repoRoot '.venv\Scripts\python.exe'
if (Test-Path -LiteralPath $venv1) {
    $executable = $venv1
} elseif (Test-Path -LiteralPath $venv2) {
    $executable = $venv2
} elseif ($cmd = Get-Command 'python' -ErrorAction SilentlyContinue) {
    $executable = $cmd.Source
} elseif ($cmd = Get-Command 'py' -ErrorAction SilentlyContinue) {
    $executable = $cmd.Source
    $argument = '-3 ' + $argument
} else {
    Write-Error 'No encontré venv\.venv ni python/py en PATH.'
    exit 1
}

$taskAction = New-ScheduledTaskAction -Execute $executable -Argument $argument.Trim() -WorkingDirectory $repoRoot
$taskTrigger  = New-ScheduledTaskTrigger -Daily -At $hora
$taskSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 3) -StartWhenAvailable
$principal    = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive

try {
    Register-ScheduledTask -TaskName $nombre -Action $taskAction -Trigger $taskTrigger -Settings $taskSettings -Principal $principal -Force | Out-Null
    Write-Host "Tarea creada: $nombre a las $hora (zona horaria de Windows)."
    Write-Host "Repo: $repoRoot"
} catch {
    Write-Warning 'No se pudo registrar sin permisos extra. Probá:'
    Write-Host '  1) PowerShell como administrador y volvé a ejecutar este script.'
    Write-Host '  2) O creá la tarea a mano en el Programador de tareas con el mismo comando.'
    throw
}
