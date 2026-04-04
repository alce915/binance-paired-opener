param(
    [Parameter(Mandatory = $true)][string]$ProjectRoot,
    [Parameter(Mandatory = $true)][string]$PythonExe,
    [Parameter(Mandatory = $true)][string]$PythonArgsJson,
    [Parameter(Mandatory = $true)][string]$HostAddress,
    [Parameter(Mandatory = $true)][int]$Port,
    [Parameter(Mandatory = $true)][string]$PythonPathValue,
    [Parameter(Mandatory = $true)][string]$RuntimeLog
)

$ErrorActionPreference = 'Stop'
$pythonArgs = @()

if ($PythonArgsJson) {
    $decodedArgs = ConvertFrom-Json -InputObject $PythonArgsJson -ErrorAction Stop
    if ($decodedArgs -is [System.Array]) {
        $pythonArgs = @($decodedArgs)
    } elseif ($null -ne $decodedArgs) {
        $pythonArgs = @([string]$decodedArgs)
    }
}

Set-Location $ProjectRoot
$env:PYTHONPATH = $PythonPathValue

try {
    Start-Transcript -Path $RuntimeLog -Append -ErrorAction Stop | Out-Null
} catch {
}

Write-Host "Paired opener service running at http://$HostAddress`:$Port/" -ForegroundColor Green
Write-Host 'Keep this window open while using the service.' -ForegroundColor Yellow
Write-Host ''

$exitCode = 0
try {
    & $PythonExe @pythonArgs -m uvicorn paired_opener.api:app --host $HostAddress --port $Port
    if ($LASTEXITCODE -ne $null) {
        $exitCode = $LASTEXITCODE
    }
} finally {
    try {
        Stop-Transcript | Out-Null
    } catch {
    }
}

exit $exitCode
