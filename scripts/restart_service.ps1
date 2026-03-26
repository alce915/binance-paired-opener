$ErrorActionPreference = 'Stop'

$projectRoot = 'D:\codex\币安自动开单系统'
$venvPython = 'D:\codex\币安自动开单系统\.venv\Scripts\python.exe'
$hostAddress = '127.0.0.1'
$port = 8000
$runtimeLog = Join-Path $projectRoot 'api.runtime.log'

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

$childCommand = "Set-Location '$projectRoot'; & '$venvPython' -m uvicorn paired_opener.api:app --host $hostAddress --port $port *>> '$runtimeLog'"
Start-Process -FilePath 'powershell.exe' -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-Command',$childCommand -WindowStyle Hidden | Out-Null

$deadline = (Get-Date).AddSeconds(20)
$started = $false
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 500
    try {
        $response = Invoke-WebRequest -UseBasicParsing ("http://{0}:{1}/" -f $hostAddress, $port) -TimeoutSec 2
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
        $runtime = 'service did not become healthy within timeout'
    }
    throw "Service restart failed: $runtime"
}

$listener = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop | Select-Object -First 1
"Service restarted successfully on http://$hostAddress`:$port/ (PID=$($listener.OwningProcess))"
