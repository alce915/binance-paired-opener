$ErrorActionPreference = 'Stop'

$port = 8000
$stopped = $false

try {
    $connections = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop
} catch {
    $connections = @()
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

if ($stopped) {
    "Service stopped successfully on port $port."
} else {
    "No running service found on port $port."
}
