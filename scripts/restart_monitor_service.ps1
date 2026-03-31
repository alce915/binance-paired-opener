$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path $PSScriptRoot -Parent
$venvPython = Join-Path $projectRoot '.venv\Scripts\python.exe'
$sitePackages = Join-Path $projectRoot '.venv\Lib\site-packages'
$pyVenvConfig = Join-Path $projectRoot '.venv\pyvenv.cfg'
$basePython = $null
if (Test-Path $pyVenvConfig) {
    $homeLine = Get-Content $pyVenvConfig | Where-Object { $_ -like 'home = *' } | Select-Object -First 1
    if ($homeLine) {
        $homePath = $homeLine.Substring(7).Trim()
        if ($homePath) {
            $candidate = Join-Path $homePath 'python.exe'
            if (Test-Path $candidate) {
                $basePython = $candidate
            }
        }
    }
}
$pythonExecutable = if ($basePython) { $basePython } else { $venvPython }
$hostAddress = '127.0.0.1'
$port = 8010
$runtimeLog = Join-Path $projectRoot 'monitor.runtime.log'
$healthUrl = "http://{0}:{1}/healthz" -f $hostAddress, $port
$pythonPathValue = "$sitePackages;$projectRoot"

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

$env:PYTHONPATH = $pythonPathValue
& $pythonExecutable -m paired_opener.log_retention --file $runtimeLog

$childCommand = "Set-Location '$projectRoot'; `$env:PYTHONPATH = '$pythonPathValue'; & '$pythonExecutable' -m uvicorn paired_opener.monitor_api:app --host $hostAddress --port $port *>> '$runtimeLog'"
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