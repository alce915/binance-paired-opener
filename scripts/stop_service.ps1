param(
    [switch]$ElevatedRetry
)

$ErrorActionPreference = 'Stop'

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

function Read-ServiceMetadata {
    param([string]$MetadataPath)

    if (-not (Test-Path $MetadataPath)) {
        return $null
    }

    try {
        return Get-Content -Path $MetadataPath -Raw | ConvertFrom-Json
    } catch {
        Remove-Item -Path $MetadataPath -Force -ErrorAction SilentlyContinue
        return $null
    }
}

function Remove-ServiceMetadata {
    param([string]$MetadataPath)

    if (Test-Path $MetadataPath) {
        Remove-Item -Path $MetadataPath -Force -ErrorAction SilentlyContinue
    }
}

function Stop-TrackedLauncher {
    param([string]$MetadataPath)

    $metadata = Read-ServiceMetadata -MetadataPath $MetadataPath
    if ($null -eq $metadata) {
        return $false
    }

    $launcherPid = [int]($metadata.launcher_pid | ForEach-Object { $_ })
    if ($launcherPid -le 0) {
        Remove-ServiceMetadata -MetadataPath $MetadataPath
        return $false
    }

    $launcherProcess = Get-Process -Id $launcherPid -ErrorAction SilentlyContinue
    if ($null -eq $launcherProcess) {
        Remove-ServiceMetadata -MetadataPath $MetadataPath
        return $false
    }

    $recordedStartTime = $null
    if ($metadata.launcher_start_time) {
        $recordedStartTime = [datetime]::Parse($metadata.launcher_start_time.ToString())
    }
    if ($null -ne $recordedStartTime) {
        $deltaSeconds = [math]::Abs(($launcherProcess.StartTime.ToUniversalTime() - $recordedStartTime.ToUniversalTime()).TotalSeconds)
        if ($deltaSeconds -gt 2) {
            return $false
        }
    }

    try {
        Stop-Process -Id $launcherPid -Force -ErrorAction Stop
    } catch {
        & taskkill.exe /PID $launcherPid /T /F 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "failed to stop tracked launcher pid $launcherPid"
        }
    }

    return $true
}

function Stop-ListeningProcess {
    param([int]$OwningProcess)

    try {
        Stop-Process -Id $OwningProcess -Force -ErrorAction Stop
    } catch {
        & taskkill.exe /PID $OwningProcess /T /F 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "failed to stop listening process pid $OwningProcess"
        }
    }
}

function Invoke-ElevatedRetry {
    $argsList = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $PSCommandPath, '-ElevatedRetry')
    $process = Start-Process -FilePath 'powershell.exe' -ArgumentList $argsList -Verb RunAs -Wait -PassThru
    exit $process.ExitCode
}

$projectRoot = Split-Path $PSScriptRoot -Parent
$serviceMetadataPath = Join-Path $projectRoot '.codex-runtime\service-process.json'
$hostAddress = '127.0.0.1'
$port = 8000
$stopped = $false
$requiresElevation = $false
$healthResponds = $false

Set-Location $projectRoot

$stopSucceeded = $false
for ($attempt = 1; $attempt -le 8; $attempt++) {
    try {
        if (Stop-TrackedLauncher -MetadataPath $serviceMetadataPath) {
            $stopped = $true
            Start-Sleep -Milliseconds 500
        }
    } catch {
        if (-not $ElevatedRetry) {
            $requiresElevation = $true
        }
    }

    $connections = Get-ListeningConnections -Port $port
    foreach ($connection in $connections) {
        try {
            Stop-ListeningProcess -OwningProcess $connection.OwningProcess
            $stopped = $true
            Start-Sleep -Milliseconds 300
        } catch {
            if (-not $ElevatedRetry) {
                $requiresElevation = $true
            }
        }
    }

    Start-Sleep -Milliseconds 500
    $remaining = Get-ListeningConnections -Port $port
    $healthResponds = Test-HealthResponds -HostAddress $hostAddress -Port $port
    if ($remaining.Count -eq 0 -and -not $healthResponds) {
        $stopSucceeded = $true
        break
    }
}

if ($requiresElevation -and -not $ElevatedRetry) {
    Invoke-ElevatedRetry
}

if (-not $stopSucceeded) {
    $remaining = Get-ListeningConnections -Port $port
    $healthResponds = Test-HealthResponds -HostAddress $hostAddress -Port $port
}

if (-not $stopSucceeded -and $remaining.Count -gt 0) {
    throw "Service stop failed: process still listening on port $port"
}

if (-not $stopSucceeded -and $healthResponds) {
    throw "Service stop failed: health endpoint still responds on port $port"
}

Remove-ServiceMetadata -MetadataPath $serviceMetadataPath

if ($stopped) {
    "Service stopped successfully on port $port."
} else {
    "No running service found on port $port."
}
