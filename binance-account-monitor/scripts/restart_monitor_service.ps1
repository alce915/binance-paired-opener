$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$localVenvPython = Join-Path $projectRoot '.venv\Scripts\python.exe'
$parentVenvPython = Join-Path (Split-Path $projectRoot -Parent) '.venv\Scripts\python.exe'
$pythonExecutable = if (Test-Path $localVenvPython) { $localVenvPython } else { $parentVenvPython }
$sitePackages = if (Test-Path (Join-Path $projectRoot '.venv\Lib\site-packages')) {
    Join-Path $projectRoot '.venv\Lib\site-packages'
} else {
    Join-Path (Split-Path $projectRoot -Parent) '.venv\Lib\site-packages'
}
$hostAddress = '127.0.0.1'
$port = 8010
$runtimeLog = Join-Path $projectRoot 'monitor.runtime.log'
$healthUrl = "http://{0}:{1}/healthz" -f $hostAddress, $port
$pythonPathValue = "$sitePackages;$projectRoot"

if (-not (Test-Path $pythonExecutable)) {
    throw "Python executable not found. Create a .venv in the monitor project or ensure the parent workspace .venv exists."
}

Set-Location $projectRoot

try {
    $connections = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop
} catch {
    $connections = @()
}

foreach ($connection in $connections) {
    try {
        Stop-Process -Id $connection.OwningProcess -Force -ErrorAction Stop
        Start-Sleep -Milliseconds 300
    } catch {
    }
}

if (Test-Path $runtimeLog) {
    Remove-Item $runtimeLog -Force -ErrorAction SilentlyContinue
}

$childCommand = "Set-Location '$projectRoot'; `$env:PYTHONPATH = '$pythonPathValue'; & '$pythonExecutable' -m uvicorn monitor_app.api:app --host $hostAddress --port $port *>> '$runtimeLog'"
Start-Process -FilePath 'powershell.exe' -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-Command',$childCommand -WindowStyle Hidden | Out-Null

$deadline = (Get-Date).AddSeconds(20)
$started = $false
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 500
    try {
        $response = Invoke-WebRequest -UseBasicParsing $healthUrl -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            $started = $true
            break
        }
    } catch {
    }
}

if (-not $started) {
    $runtime = if (Test-Path $runtimeLog) { Get-Content $runtimeLog -Raw -ErrorAction SilentlyContinue } else { '' }
    if (-not $runtime) {
        $runtime = 'monitor service did not become healthy within timeout'
    }
    throw "Monitor service restart failed: $runtime"
}

$listener = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop | Select-Object -First 1
"Monitor service restarted successfully on http://$hostAddress`:$port/ (PID=$($listener.OwningProcess))"