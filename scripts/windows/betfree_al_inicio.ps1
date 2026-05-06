#Requires -Version 5.1
<#
  Tarea "al encender PC": digest Telegram → export JSON Pages → git push (si hay cambios).

  Probar: powershell -ExecutionPolicy Bypass -File .\betfree_al_inicio.ps1
  Sin git:   ... -SkipGitPush
#>

param(
    [switch] $SkipGitPush
)

$ErrorActionPreference = 'Stop'

$scriptsDir = $PSScriptRoot
$repoRoot   = (Resolve-Path (Join-Path $scriptsDir '..\..')).Path
$logDir     = Join-Path $repoRoot 'data\logs'
$null = New-Item -ItemType Directory -Force -Path $logDir
$logFile    = Join-Path $logDir 'betfree_startup.log'
$digestLog  = Join-Path $logDir 'betfree_digest_last_run.log'
$exportLog  = Join-Path $logDir 'betfree_export_pages_last_run.log'

function Write-BetfreeLog {
    param([string] $Message)
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $line = "[$ts] $Message"
    Add-Content -LiteralPath $logFile -Value $line -Encoding UTF8
    Write-Host $line
}

function Get-BetfreePythonExe {
    $venv1 = Join-Path $repoRoot 'venv\Scripts\python.exe'
    $venv2 = Join-Path $repoRoot '.venv\Scripts\python.exe'
    if (Test-Path -LiteralPath $venv1) { return $venv1 }
    if (Test-Path -LiteralPath $venv2) { return $venv2 }
    $cmd = Get-Command 'python' -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $py = Get-Command 'py' -ErrorAction SilentlyContinue
    if ($py) { return $py.Source }
    return $null
}

Write-BetfreeLog '=== Inicio betfree_al_inicio ==='
Start-Sleep -Seconds 60
Write-BetfreeLog 'Delay 60s terminado.'

$pyExe = Get-BetfreePythonExe
if (-not $pyExe) {
    Write-BetfreeLog 'ERROR: No hay Python (venv, .venv, python, py).'
    exit 1
}

$isPyLauncher = [System.IO.Path]::GetFileNameWithoutExtension($pyExe) -eq 'py'

Push-Location $repoRoot
try {
    Write-BetfreeLog "Python: $pyExe"
    $env:PYTHONUTF8 = '1'

    # --- Digest Telegram ---
    try {
        if ($isPyLauncher) {
            $digestOut = & $pyExe -3 -m src.free_digest_app --run-once 2>&1
        } else {
            $digestOut = & $pyExe -m src.free_digest_app --run-once 2>&1
        }
        $de = $LASTEXITCODE
        $digestOut | Set-Content -LiteralPath $digestLog -Encoding UTF8
        Write-BetfreeLog "Digest terminado (exit=$de). Log: $digestLog"
    } catch {
        Write-BetfreeLog "ERROR digest: $($_.Exception.Message)"
    }

    # --- Export JSON ---
    try {
        if ($isPyLauncher) {
            $exportOut = & $pyExe -3 scripts/export_pages_predictions.py 2>&1
        } else {
            $exportOut = & $pyExe scripts/export_pages_predictions.py 2>&1
        }
        $ee = $LASTEXITCODE
        $exportOut | Set-Content -LiteralPath $exportLog -Encoding UTF8
        Write-BetfreeLog "Export Pages exit=$ee. Log: $exportLog"
    } catch {
        Write-BetfreeLog "ERROR export: $($_.Exception.Message)"
    }

    if ($SkipGitPush) {
        Write-BetfreeLog 'SkipGitPush activado.'
        exit 0
    }

    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        Write-BetfreeLog 'AVISO: git no está en PATH.'
        exit 0
    }

    git add docs/betfree_predictions.json 2>&1 | ForEach-Object { Write-BetfreeLog "git: $_" }
    $staged = @(git diff --cached --name-only 2>&1) -join "`n"
    if (-not $staged.Trim()) {
        Write-BetfreeLog 'Git: nada que commitear en docs/betfree_predictions.json.'
        exit 0
    }

    git commit -m 'chore: auto Pages (PC encendido)' 2>&1 | ForEach-Object { Write-BetfreeLog "git: $_" }
    if ($LASTEXITCODE -ne 0) {
        Write-BetfreeLog "git commit falló (exit=$LASTEXITCODE). ¿git config user.name / user.email?"
        exit 0
    }

    git push 2>&1 | ForEach-Object { Write-BetfreeLog "git: $_" }
    if ($LASTEXITCODE -ne 0) {
        Write-BetfreeLog "git push falló (exit=$LASTEXITCODE). Revisá credenciales o red."
    } else {
        Write-BetfreeLog 'git push OK.'
    }
} finally {
    Pop-Location
}

Write-BetfreeLog '=== Fin betfree_al_inicio ==='
exit 0
