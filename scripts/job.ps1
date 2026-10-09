param(
    [ValidateSet('load', 'pipeline', 'validate', 'analytics', 'transform-test')]
    [string]$Command = 'pipeline',
    [switch]$Smoke
)
. "$PSScriptRoot/common.ps1"
Initialize-Environment
Set-RunMode $Smoke.IsPresent
$log = Join-Path $script:ProjectRoot "logs/job-$Command-$env:RUN_MODE-$(Get-Date -Format yyyyMMdd-HHmmss).log"
Start-Transcript -Path $log | Out-Null
try { Invoke-Lab $Command ($Command -ne 'load') }
finally { Stop-Transcript | Out-Null }
