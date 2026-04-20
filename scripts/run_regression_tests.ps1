param(
    [string[]]$PytestTargets = @(
        ".\tests\test_market_stream.py",
        ".\tests\test_service_config.py",
        ".\tests\test_binance_gateway.py",
        ".\tests\test_engine.py"
    ),
    [string]$NodeTarget = ".\tests\test_app_residual_display.mjs"
)

$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
$OutputEncoding = [Console]::OutputEncoding

$repoRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $repoRoot ".codex-runtime\regression"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logFile = Join-Path $logDir "regression-$timestamp.log"
$latestLog = Join-Path $logDir "latest.log"
$latestStatus = Join-Path $logDir "latest.status.txt"
$latestSummary = Join-Path $logDir "latest.summary.txt"

$script:Failures = New-Object System.Collections.Generic.List[string]

function Write-Log {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    $line | Tee-Object -FilePath $logFile -Append | Out-Host
}

function Add-Failure {
    param([string]$Message)
    $script:Failures.Add($Message)
    Write-Log "FAIL: $Message"
}

function Run-ExternalCommand {
    param(
        [string]$Name,
        [string]$FilePath,
        [string[]]$Arguments
    )

    Write-Log ">>> $Name"
    Write-Log ("CMD: {0} {1}" -f $FilePath, ($Arguments -join " "))
    & $FilePath @Arguments 2>&1 | Tee-Object -FilePath $logFile -Append | Out-Host
    $exitCode = $LASTEXITCODE
    Write-Log "<<< $Name exit=$exitCode"
    return $exitCode
}

function Test-PythonCandidate {
    param(
        [string]$Kind,
        [string]$Value,
        [int]$Attempts = 3
    )

    if ($Kind -eq "path" -and -not (Test-Path -LiteralPath $Value)) {
        return $null
    }

    $command = $Value
    $args = if ($Kind -eq "py") { @("-3.12", "-c", "print('codex_python_ok')") } else { @("-c", "print('codex_python_ok')") }

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        Write-Log ("Probing Python candidate: {0} {1} (attempt {2}/{3})" -f $Kind, $command, $attempt, $Attempts)
        try {
            & $command @args 2>&1 | Tee-Object -FilePath $logFile -Append | Out-Host
            $exitCode = $LASTEXITCODE
        } catch {
            Write-Log ("Probe exception: {0}" -f $_.Exception.Message)
            $exitCode = 1
        }

        if ($exitCode -eq 0) {
            Write-Log ("Python candidate works: {0}" -f $command)
            return [pscustomobject]@{
                Kind = $Kind
                Command = $command
            }
        }

        Write-Log ("Python candidate failed: {0} exit={1}" -f $command, $exitCode)
        if ($attempt -lt $Attempts) {
            Start-Sleep -Milliseconds 400
        }
    }

    return $null
}

function Resolve-PythonRunner {
    $candidates = @(
        @{ Kind = "path"; Value = (Join-Path $repoRoot ".venv\Scripts\python.exe") },
        @{ Kind = "py"; Value = "py" },
        @{ Kind = "path"; Value = "C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe" },
        @{ Kind = "path"; Value = "python" }
    )

    foreach ($candidate in $candidates) {
        $resolved = Test-PythonCandidate -Kind $candidate.Kind -Value $candidate.Value
        if ($null -ne $resolved) {
            return $resolved
        }
    }

    return $null
}

function Write-SummaryFiles {
    param(
        [string]$Status,
        [string]$Summary
    )

    Copy-Item -LiteralPath $logFile -Destination $latestLog -Force
    Set-Content -LiteralPath $latestStatus -Value $Status -Encoding UTF8
    Set-Content -LiteralPath $latestSummary -Value $Summary -Encoding UTF8
}

Set-Location -LiteralPath $repoRoot

Write-Log "Starting Codex regression run"
Write-Log "Repository root: $repoRoot"
Write-Log ("PowerShell: {0}" -f $PSVersionTable.PSVersion)
Write-Log "Log file: $logFile"

Write-Log "Environment probe: where python"
& where.exe python 2>&1 | Tee-Object -FilePath $logFile -Append | Out-Host
Write-Log "Environment probe: where py"
& where.exe py 2>&1 | Tee-Object -FilePath $logFile -Append | Out-Host
Write-Log "Environment probe: where node"
& where.exe node 2>&1 | Tee-Object -FilePath $logFile -Append | Out-Host

$pythonRunner = Resolve-PythonRunner
if ($null -eq $pythonRunner) {
    Add-Failure "No executable Python 3.12 runtime found; pytest was not started."
} else {
    $pytestArgs = @()
    if ($pythonRunner.Kind -eq "py") {
        $pytestArgs += "-3.12"
    }
    $pytestArgs += @("-m", "pytest", "-q")
    $pytestArgs += $PytestTargets

    $pytestExit = Run-ExternalCommand -Name "pytest regression" -FilePath $pythonRunner.Command -Arguments $pytestArgs
    if ($pytestExit -ne 0) {
        Add-Failure "pytest regression failed, exit=$pytestExit"
    }
}

$nodeAvailable = $false
try {
    & node --version 2>&1 | Tee-Object -FilePath $logFile -Append | Out-Host
    if ($LASTEXITCODE -eq 0) {
        $nodeAvailable = $true
    }
} catch {
    Write-Log ("Node probe exception: {0}" -f $_.Exception.Message)
}

$nodeTargetPath = Join-Path $repoRoot ($NodeTarget -replace '^[.\\/]+', '')
if ($nodeAvailable -and (Test-Path -LiteralPath $nodeTargetPath)) {
    $nodeExit = Run-ExternalCommand -Name "frontend residual display script" -FilePath "node" -Arguments @($NodeTarget)
    if ($nodeExit -ne 0) {
        Add-Failure "Node script failed, exit=$nodeExit"
    }
} else {
    Write-Log "Skipping Node script because node is unavailable or the test file is missing."
}

if ($script:Failures.Count -gt 0) {
    $summary = "Regression failed: " + ($script:Failures -join " | ")
    Write-Log $summary
    Write-SummaryFiles -Status "FAILED" -Summary $summary
    exit 1
}

$successSummary = "Regression completed successfully. See $latestLog"
Write-Log $successSummary
Write-SummaryFiles -Status "PASSED" -Summary $successSummary
exit 0
