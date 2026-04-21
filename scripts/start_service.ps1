$ErrorActionPreference = 'Stop'

function Read-EnvConfig {
    param([string]$Path)

    $config = @{}
    if (-not (Test-Path $Path)) {
        return $config
    }

    foreach ($line in Get-Content -Path $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith('#')) {
            continue
        }

        $parts = $trimmed -split '=', 2
        if ($parts.Count -eq 2) {
            $config[$parts[0].Trim()] = $parts[1].Trim()
        }
    }

    return $config
}

function Test-PythonCandidate {
    param(
        [string]$Command,
        [string[]]$Arguments = @()
    )

    if (-not $Command) {
        return $null
    }

    if (($Command -like '*\*') -or ($Command -like '*:*')) {
        if (-not (Test-Path $Command)) {
            return $null
        }
    }

    try {
        & $Command @Arguments -c "print('codex_python_ok')" 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            return @{
                exe = $Command
                args = @($Arguments)
            }
        }
    } catch {
    }

    return $null
}

function Resolve-ServicePython {
    param(
        [string]$ProjectRoot,
        [hashtable]$EnvConfig
    )

    $localPyVenvConfig = Join-Path $ProjectRoot '.venv\pyvenv.cfg'
    $localVenvPython = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
    $parentVenvPython = Join-Path (Split-Path $ProjectRoot -Parent) '.venv\Scripts\python.exe'
    $bundledPython = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
    $candidates = New-Object System.Collections.Generic.List[object]

    foreach ($envKey in @('OPENER_PYTHON', 'SERVICE_PYTHON')) {
        if ($EnvConfig.ContainsKey($envKey) -and $EnvConfig[$envKey]) {
            $candidates.Add([pscustomobject]@{
                exe = $EnvConfig[$envKey]
                args = @()
            })
        }
    }

    if (Test-Path $localPyVenvConfig) {
        $homeLine = Get-Content -Path $localPyVenvConfig | Where-Object { $_ -like 'home = *' } | Select-Object -First 1
        if ($homeLine) {
            $homePath = $homeLine.Substring(7).Trim()
            if ($homePath) {
                $candidate = Join-Path $homePath 'python.exe'
                if (Test-Path $candidate) {
                    $candidates.Add([pscustomobject]@{
                        exe = $candidate
                        args = @()
                    })
                }
            }
        }
    }

    if (Test-Path $localVenvPython) {
        $candidates.Add([pscustomobject]@{
            exe = $localVenvPython
            args = @()
        })
    }

    if (Test-Path $parentVenvPython) {
        $candidates.Add([pscustomobject]@{
            exe = $parentVenvPython
            args = @()
        })
    }

    if (Test-Path $bundledPython) {
        $candidates.Add([pscustomobject]@{
            exe = $bundledPython
            args = @()
        })
    }

    if (Get-Command py -ErrorAction SilentlyContinue) {
        $candidates.Add([pscustomobject]@{
            exe = 'py'
            args = @('-3.12')
        })
        $candidates.Add([pscustomobject]@{
            exe = 'py'
            args = @('-3')
        })
    }

    if (Get-Command python -ErrorAction SilentlyContinue) {
        $candidates.Add([pscustomobject]@{
            exe = 'python'
            args = @()
        })
    }

    foreach ($candidateInfo in $candidates) {
        $resolved = Test-PythonCandidate -Command $candidateInfo.exe -Arguments $candidateInfo.args
        if ($resolved) {
            return $resolved
        }
    }

    throw 'No runnable Python launcher was found. Configure OPENER_PYTHON or SERVICE_PYTHON in .env first.'
}

function Resolve-ServicePythonExecutable {
    param([hashtable]$PythonInfo)

    $candidateExe = $PythonInfo.exe
    if (($candidateExe -like '*\*') -or ($candidateExe -like '*:*')) {
        $pythonDir = Split-Path $candidateExe -Parent
        $windowedPython = Join-Path $pythonDir 'pythonw.exe'
        if (Test-Path $windowedPython) {
            return $windowedPython
        }
    }

    return $candidateExe
}

function Get-ListeningConnections {
    param([int]$Port)

    $connections = @()

    try {
        $connections = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop)
    } catch {
        $connections = @()
    }

    if ($connections.Count -gt 0) {
        return $connections
    }

    $netstat = netstat -ano | Select-String -Pattern ":$port\s+\S+\s+LISTENING"
    foreach ($line in $netstat) {
        $parts = $line.Line -split '\s+'
        if ($parts.Length -gt 0) {
            $owningProcessId = $parts[-1]
            if ($owningProcessId -match '^\d+$') {
                $connections += [pscustomobject]@{ OwningProcess = [int]$owningProcessId }
            }
        }
    }

    return $connections
}

function Test-HealthResponds {
    param(
        [string]$HostAddress,
        [int]$Port
    )

    try {
        & curl.exe -fsS --max-time 3 ("http://{0}:{1}/" -f $HostAddress, $Port) 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) {
            return $false
        }
        return $true
    } catch {
        return $false
    }
}

function Write-ServiceProcessMetadata {
    param(
        [string]$MetadataPath,
        [System.Diagnostics.Process]$LauncherProcess,
        [string]$HostAddress,
        [int]$Port
    )

    $stateDir = Split-Path $MetadataPath -Parent
    if (-not (Test-Path $stateDir)) {
        New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
    }

    $payload = @{
        launcher_pid = $LauncherProcess.Id
        launcher_start_time = $LauncherProcess.StartTime.ToUniversalTime().ToString('o')
        host = $HostAddress
        port = $Port
        recorded_at = (Get-Date).ToUniversalTime().ToString('o')
    }

    $payload | ConvertTo-Json | Set-Content -Path $MetadataPath -Encoding UTF8
}

$projectRoot = Split-Path $PSScriptRoot -Parent
$envFile = Join-Path $projectRoot '.env'
$runtimeLog = Join-Path $projectRoot 'api.runtime.log'
$serviceRunner = Join-Path $PSScriptRoot 'run_service.py'
$serviceMetadataPath = Join-Path $projectRoot '.codex-runtime\service-process.json'
$envConfig = Read-EnvConfig -Path $envFile
$pythonInfo = Resolve-ServicePython -ProjectRoot $projectRoot -EnvConfig $envConfig
$servicePythonExe = Resolve-ServicePythonExecutable -PythonInfo $pythonInfo
$hostAddress = if ($envConfig.ContainsKey('API_HOST') -and $envConfig['API_HOST']) { $envConfig['API_HOST'] } else { '127.0.0.1' }
$port = if ($envConfig.ContainsKey('API_PORT') -and $envConfig['API_PORT']) { [int]$envConfig['API_PORT'] } else { 8000 }
$visibleConsole = $false
if ($envConfig.ContainsKey('SERVICE_VISIBLE_CONSOLE') -and $envConfig['SERVICE_VISIBLE_CONSOLE']) {
    $visibleConsole = $envConfig['SERVICE_VISIBLE_CONSOLE'].ToLowerInvariant() -in @('1', 'true', 'yes', 'on')
}
$sitePackages = if (Test-Path (Join-Path $projectRoot '.venv\Lib\site-packages')) {
    Join-Path $projectRoot '.venv\Lib\site-packages'
} else {
    Join-Path (Split-Path $projectRoot -Parent) '.venv\Lib\site-packages'
}
$pythonPathValue = "$sitePackages;$projectRoot"

Set-Location $projectRoot

$connections = Get-ListeningConnections -Port $port

foreach ($connection in $connections) {
    try {
        Stop-Process -Id $connection.OwningProcess -Force -ErrorAction Stop
        Start-Sleep -Milliseconds 300
    } catch {
    }
}

$remainingListeners = Get-ListeningConnections -Port $port
if ($remainingListeners.Count -gt 0) {
    throw "Service start failed: port $port is already in use"
}

$env:PYTHONPATH = $pythonPathValue
& $pythonInfo.exe @($pythonInfo.args) -m paired_opener.log_retention --file $runtimeLog

if (Test-Path $serviceMetadataPath) {
    Remove-Item -Path $serviceMetadataPath -Force -ErrorAction SilentlyContinue
}
$serviceArgs = @($pythonInfo.args) + @(
    $serviceRunner,
    '--project-root', $projectRoot,
    '--site-packages', $sitePackages,
    '--runtime-log', $runtimeLog,
    '--host', $hostAddress,
    '--port', $port.ToString()
)

if ($visibleConsole) {
    $null = Start-Process -FilePath $servicePythonExe -ArgumentList $serviceArgs -WorkingDirectory $projectRoot -WindowStyle Normal -PassThru
} else {
    $null = Start-Process -FilePath $servicePythonExe -ArgumentList $serviceArgs -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru
}

$deadline = (Get-Date).AddSeconds(20)
$started = $false
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 500
    if (Test-HealthResponds -HostAddress $hostAddress -Port $port) {
        $started = $true
        break
    }
}

if (-not $started) {
    if (Test-Path $serviceMetadataPath) {
        Remove-Item -Path $serviceMetadataPath -Force -ErrorAction SilentlyContinue
    }
    $failureDetails = @()
    if (Test-Path $runtimeLog) {
        $failureDetails += ((Get-Content $runtimeLog -Tail 80 -ErrorAction SilentlyContinue) -join [Environment]::NewLine)
    }
    $runtime = ($failureDetails | Where-Object { $_ }) -join [Environment]::NewLine
    if (-not $runtime) {
        $runtime = 'service did not become healthy within timeout'
    }
    throw "Service restart failed: $runtime"
}

$listener = Get-ListeningConnections -Port $port | Select-Object -First 1
if (-not $listener) {
    throw "Service started but no listening process was detected on port $port"
}
try {
    $serviceProcess = Get-Process -Id $listener.OwningProcess -ErrorAction Stop
    Write-ServiceProcessMetadata -MetadataPath $serviceMetadataPath -LauncherProcess $serviceProcess -HostAddress $hostAddress -Port $port
} catch {
}
"Service started successfully on http://$hostAddress`:$port/ (PID=$($listener.OwningProcess))"
