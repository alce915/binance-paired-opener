$ErrorActionPreference = 'Stop'

function Test-IsAdmin {
    $current = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($current)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-IsAdmin)) {
    try {
        $argsList = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $PSCommandPath)
        $process = Start-Process -FilePath 'powershell.exe' -ArgumentList $argsList -Verb RunAs -Wait -PassThru
        exit $process.ExitCode
    } catch {
        throw "Unable to elevate stop_service.ps1: $($_.Exception.Message)"
    }
}

$projectRoot = Split-Path $PSScriptRoot -Parent
$port = 8000
$stopped = $false

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
        $stopped = $true
        Start-Sleep -Milliseconds 300
    } catch {
    }
}

try {
    $remaining = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop
} catch {
    $remaining = @()
}

if ($remaining.Count -gt 0) {
    throw "Service stop failed: process still listening on port $port"
}

$healthResponds = $false
try {
    & curl.exe -fsS --max-time 3 ("http://127.0.0.1:{0}/" -f $port) | Out-Null
    $healthResponds = $true
} catch {
}

if ($healthResponds) {
    throw "Service stop failed: health endpoint still responds on port $port"
}

if ($stopped) {
    "Service stopped successfully on port $port."
} else {
    "No running service found on port $port."
}
