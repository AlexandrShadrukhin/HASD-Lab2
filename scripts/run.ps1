param([switch]$Smoke, [switch]$SkipStart)
. "$PSScriptRoot/common.ps1"
if (!$SkipStart) { & "$PSScriptRoot/start.ps1" }
Initialize-Environment
Set-RunMode $Smoke.IsPresent
$log = Join-Path $script:ProjectRoot "logs/run-$env:RUN_MODE-$(Get-Date -Format yyyyMMdd-HHmmss).log"
Start-Transcript -Path $log | Out-Null
try {
    Invoke-Lab 'download'
    Invoke-Lab 'load'
    Invoke-Lab 'pipeline' $true
    Write-Host "Пайплайн завершён. Результаты: results/$env:RUN_MODE. Лог: $log"
} finally { Stop-Transcript | Out-Null }
