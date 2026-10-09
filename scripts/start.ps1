. "$PSScriptRoot/common.ps1"
Set-Java17
Initialize-Environment
Invoke-Checked docker @('info','--format','{{.ServerVersion}}')
Invoke-Checked "$script:ProjectRoot/gradlew.bat" @('--no-daemon','build')
Invoke-Checked docker @('compose','config','--quiet')
Invoke-Checked docker @('compose','up','-d','--build','--wait','--wait-timeout','180')
$ready = $false
for ($attempt = 0; $attempt -lt 60; $attempt++) {
    try {
        $master = Invoke-RestMethod 'http://localhost:8080/json/'
        if (@($master.workers | Where-Object { $_.state -eq 'ALIVE' }).Count -gt 0) { $ready = $true; break }
    } catch { }
    Start-Sleep -Seconds 2
}
if (!$ready) { throw 'Spark Worker не зарегистрировался в Master' }
Write-Host 'Готово: Spark http://localhost:8080; MinIO http://localhost:9001'
