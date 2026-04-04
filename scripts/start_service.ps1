$ErrorActionPreference = 'Stop'

function Test-IsAdmin {
    $current = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($current)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

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

function Resolve-ServicePython {
    param(
        [string]$ProjectRoot,
        [hashtable]$EnvConfig
    )

    $localPyVenvConfig = Join-Path $ProjectRoot '.venv\pyvenv.cfg'
    $localVenvPython = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
    $parentVenvPython = Join-Path (Split-Path $ProjectRoot -Parent) '.venv\Scripts\python.exe'
    $pythonExecutable = $null

    foreach ($envKey in @('OPENER_PYTHON', 'SERVICE_PYTHON')) {
        if ($EnvConfig.ContainsKey($envKey) -and $EnvConfig[$envKey]) {
            return @{
                exe = $EnvConfig[$envKey]
                args = @()
            }
        }
    }

    if (Test-Path $localPyVenvConfig) {
        $homeLine = Get-Content -Path $localPyVenvConfig | Where-Object { $_ -like 'home = *' } | Select-Object -First 1
        if ($homeLine) {
            $homePath = $homeLine.Substring(7).Trim()
            if ($homePath) {
                $candidate = Join-Path $homePath 'python.exe'
                if (Test-Path $candidate) {
                    $pythonExecutable = $candidate
                }
            }
        }
    }

    if (-not $pythonExecutable) {
        if (Test-Path $localVenvPython) {
            $pythonExecutable = $localVenvPython
        } elseif (Test-Path $parentVenvPython) {
            $pythonExecutable = $parentVenvPython
        }
    }

    if ($pythonExecutable -and (Test-Path $pythonExecutable)) {
        return @{
            exe = $pythonExecutable
            args = @()
        }
    }

    $pyCommand = Get-Command py -ErrorAction SilentlyContinue
    if ($pyCommand) {
        return @{
            exe = 'py'
            args = @('-3')
        }
    }

    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        return @{
            exe = 'python'
            args = @()
        }
    }

    throw 'No runnable Python launcher was found. Configure OPENER_PYTHON or SERVICE_PYTHON in .env first.'
}

if (-not (Test-IsAdmin)) {
    try {
        $argsList = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $PSCommandPath)
        $process = Start-Process -FilePath 'powershell.exe' -ArgumentList $argsList -Verb RunAs -Wait -PassThru
        exit $process.ExitCode
    } catch {
        throw "Unable to elevate start_service.ps1: $($_.Exception.Message)"
    }
}

$projectRoot = Split-Path $PSScriptRoot -Parent
$envFile = Join-Path $projectRoot '.env'
$runtimeLog = Join-Path $projectRoot 'api.runtime.log'
$consoleScript = Join-Path $PSScriptRoot 'run_service_console.ps1'
$envConfig = Read-EnvConfig -Path $envFile
$pythonInfo = Resolve-ServicePython -ProjectRoot $projectRoot -EnvConfig $envConfig
$hostAddress = if ($envConfig.ContainsKey('API_HOST') -and $envConfig['API_HOST']) { $envConfig['API_HOST'] } else { '127.0.0.1' }
$port = if ($envConfig.ContainsKey('API_PORT') -and $envConfig['API_PORT']) { [int]$envConfig['API_PORT'] } else { 8000 }
$visibleConsole = $true
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

try {
    $connections = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop
} catch {
    $connections = @()
}

if (-not $connections -or $connections.Count -eq 0) {
    $netstat = netstat -ano | Select-String -Pattern ":$port\s+\S+\s+LISTENING"
    foreach ($line in $netstat) {
        $parts = $line.Line -split '\s+'
        if ($parts.Length -gt 0) {
            $pid = $parts[-1]
            if ($pid -match '^\d+$') {
                $connections += [pscustomobject]@{ OwningProcess = [int]$pid }
            }
        }
    }
}

foreach ($connection in $connections) {
    try {
        Stop-Process -Id $connection.OwningProcess -Force -ErrorAction Stop
        Start-Sleep -Milliseconds 300
    } catch {
    }
}

$env:PYTHONPATH = $pythonPathValue
& $pythonInfo.exe @($pythonInfo.args) -m paired_opener.log_retention --file $runtimeLog

$pythonArgsJson = ConvertTo-Json -Compress -InputObject @($pythonInfo.args)
$startArgs = @(
    '-NoExit',
    '-NoProfile',
    '-ExecutionPolicy', 'Bypass',
    '-File', $consoleScript,
    '-ProjectRoot', $projectRoot,
    '-PythonExe', $pythonInfo.exe,
    '-PythonArgsJson', $pythonArgsJson,
    '-HostAddress', $hostAddress,
    '-Port', "$port",
    '-PythonPathValue', $pythonPathValue,
    '-RuntimeLog', $runtimeLog
)
if ($visibleConsole) {
    Start-Process -FilePath 'powershell.exe' -ArgumentList $startArgs -WindowStyle Normal | Out-Null
} else {
    Start-Process -FilePath 'powershell.exe' -ArgumentList $startArgs -WindowStyle Hidden | Out-Null
}

$deadline = (Get-Date).AddSeconds(20)
$started = $false
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 500
    try {
        & curl.exe -fsS --max-time 3 ("http://{0}:{1}/" -f $hostAddress, $port) | Out-Null
        $started = $true
        break
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
"Service started successfully on http://$hostAddress`:$port/ (PID=$($listener.OwningProcess))"
