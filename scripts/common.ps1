$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [Console]::OutputEncoding
$script:ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $script:ProjectRoot

function Invoke-Checked {
    param([string]$File, [string[]]$Arguments)
    Get-Command $File -ErrorAction Stop | Out-Null
    $previousPreference = $ErrorActionPreference
    try {
        # PowerShell 5.1 считает stderr нативных программ ErrorRecord даже при коде 0.
        # Выводим оба потока текстом, чтобы Transcript сохранил весь журнал.
        $ErrorActionPreference = 'Continue'
        & $File @Arguments 2>&1 | ForEach-Object { Write-Host $_.ToString() }
        $exitCode = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previousPreference }
    if ($exitCode -ne 0) { throw "Команда $File завершилась с кодом $exitCode" }
}
function Set-Java17 {
    $candidates = @($env:JAVA_HOME)
    $candidates += @(Get-ChildItem "$env:USERPROFILE\.jdks\temurin-17*", 'C:\Program Files\Eclipse Adoptium\jdk-17*' -Directory -ErrorAction SilentlyContinue | ForEach-Object FullName)
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path "$candidate\release")) {
            $release = Get-Content "$candidate\release"
            if ($release -match '^JAVA_VERSION="17\.' -and $release -match 'Eclipse Adoptium') {
                $env:JAVA_HOME = $candidate
                $env:PATH = "$candidate\bin;$env:PATH"
                return
            }
        }
    }
    throw 'Установите Eclipse Temurin JDK 17 и задайте JAVA_HOME. Java 21/25/26 не подходит.'
}
function Initialize-Environment {
    if (!(Test-Path '.env')) {
        $password = [guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N')
        $content = (Get-Content '.env.example' -Raw -Encoding UTF8).Replace('replace-with-local-password', $password)
        [IO.File]::WriteAllText((Join-Path $script:ProjectRoot '.env'), $content, [Text.UTF8Encoding]::new($false))
    }
    foreach ($line in (Get-Content '.env' -Encoding UTF8)) {
        if ($line -match '^([A-Z_]+)=(.*)$') { [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process') }
    }
    New-Item -ItemType Directory -Force data/raw, results/full, results/smoke, logs | Out-Null
}
function Set-RunMode([bool]$Smoke) {
    $env:RUN_MODE = 'full'
    if ($Smoke) { $env:RUN_MODE = 'smoke'; $env:TARGET_MIB = '1' }
    $env:RESULTS_DIR = "results/$env:RUN_MODE"
}
function Invoke-Lab([string]$Command, [bool]$Spark = $false) {
    $arguments = @('compose','exec','-T','-e',"RUN_MODE=$env:RUN_MODE",'-e',"TARGET_MIB=$env:TARGET_MIB",'-e',"RESULTS_DIR=/workspace/results/$env:RUN_MODE",'spark-master')
    if ($Spark) {
        $arguments += @('/opt/spark/bin/spark-submit','--master','spark://spark-master:7077','--deploy-mode','client','--driver-memory','2g','--executor-memory','2g','--total-executor-cores','2','--conf','spark.driver.host=spark-master','--conf','spark.driver.bindAddress=0.0.0.0','--class','ru.hasd.Main','/opt/lab/hasd-lab2.jar',$Command)
    } else {
        $arguments += @('java','-cp','/opt/lab/hasd-lab2.jar:/opt/spark/jars/*','ru.hasd.Main',$Command)
    }
    Invoke-Checked docker $arguments
}
