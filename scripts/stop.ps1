. "$PSScriptRoot/common.ps1"
Invoke-Checked docker @('compose','down')
Write-Host 'Контейнеры остановлены. Данные MinIO сохранены в именованном томе.'
