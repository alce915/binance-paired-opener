$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$stopScript = Join-Path $PSScriptRoot 'stop_service.ps1'
$startScript = Join-Path $PSScriptRoot 'start_service.ps1'

Set-Location $projectRoot

try {
    & $stopScript
} catch {
    throw "stop_service.ps1 failed: $($_.Exception.Message)"
}

try {
    & $startScript
} catch {
    throw "start_service.ps1 failed: $($_.Exception.Message)"
}
