param([switch]$Smoke)
. "$PSScriptRoot/common.ps1"
Initialize-Environment
Set-RunMode $Smoke.IsPresent
Invoke-Lab 'transform-test' $true
Invoke-Lab 'validate' $true
