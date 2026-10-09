param([switch]$Smoke, [switch]$Docker)
. "$PSScriptRoot/common.ps1"
Set-Java17
Initialize-Environment
Set-RunMode $Smoke.IsPresent
if ($Docker) { Invoke-Lab 'download' } else { Invoke-Checked "$script:ProjectRoot/gradlew.bat" @('--no-daemon','download') }
