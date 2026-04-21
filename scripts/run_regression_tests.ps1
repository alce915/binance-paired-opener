param(
    [string[]]$PytestTargets = @(
        ".\tests\test_market_stream.py",
        ".\tests\test_i18n_contracts.py",
        ".\tests\test_service_config.py",
        ".\tests\test_binance_gateway.py",
        ".\tests\test_engine.py",
        ".\binance-account-monitor\tests\test_account_monitor.py"
    ),
    [string[]]$NodeTargets = @(
        ".\tests\test_app_residual_display.mjs",
        ".\tests\test_monitor_message_contracts.mjs",
        ".\tests\test_app_error_contracts.mjs"
    )
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
$tempRoot = Join-Path $env:LOCALAPPDATA "Temp\codex-regression"
New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null

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

function Get-DisplayTargetName {
    param([string]$Target)

    return ($Target -replace '^[.\\/]+', '')
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
    $bundledPython = Join-Path $env:USERPROFILE ".cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
    $candidates = @(
        @{ Kind = "path"; Value = $bundledPython },
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

function Resolve-NodeRunner {
    $bundledNode = Join-Path $env:USERPROFILE ".cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe"
    $candidates = @($bundledNode, "node")

    foreach ($candidate in $candidates) {
        if (($candidate -ne "node") -and -not (Test-Path -LiteralPath $candidate)) {
            continue
        }

        Write-Log ("Probing Node candidate: {0}" -f $candidate)
        try {
            & $candidate --version 2>&1 | Tee-Object -FilePath $logFile -Append | Out-Host
            if ($LASTEXITCODE -eq 0) {
                Write-Log ("Node candidate works: {0}" -f $candidate)
                return $candidate
            }
        } catch {
            Write-Log ("Node probe exception: {0}" -f $_.Exception.Message)
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

function Update-RunState {
    param(
        [string]$Status,
        [string]$Summary
    )

    if (Test-Path -LiteralPath $logFile) {
        Copy-Item -LiteralPath $logFile -Destination $latestLog -Force
    }
    Set-Content -LiteralPath $latestStatus -Value $Status -Encoding UTF8
    Set-Content -LiteralPath $latestSummary -Value $Summary -Encoding UTF8
}

function Write-WhereProbe {
    param([string]$CommandName)

    Write-Log ("Environment probe: where {0}" -f $CommandName)
    $probeOutput = cmd /c "where $CommandName 2>nul"
    if ($LASTEXITCODE -eq 0) {
        $probeOutput | Tee-Object -FilePath $logFile -Append | Out-Host
    } else {
        Write-Log ("{0}: not found on PATH" -f $CommandName)
    }
}

Set-Location -LiteralPath $repoRoot

Write-Log "Starting Codex regression run"
Write-Log "Repository root: $repoRoot"
Write-Log ("PowerShell: {0}" -f $PSVersionTable.PSVersion)
Write-Log "Log file: $logFile"
Update-RunState -Status "RUNNING" -Summary "Regression started at $timestamp"

Write-WhereProbe -CommandName "python"
Write-WhereProbe -CommandName "py"
Write-WhereProbe -CommandName "node"

$sitePackages = Join-Path $repoRoot ".venv\Lib\site-packages"
$subProjectRoot = Join-Path $repoRoot "binance-account-monitor"
$pythonPathParts = @($repoRoot, $subProjectRoot)
if (Test-Path -LiteralPath $sitePackages) {
    $pythonPathParts += $sitePackages
}
if ($env:PYTHONPATH) {
    $pythonPathParts += $env:PYTHONPATH
}
$env:PYTHONPATH = ($pythonPathParts | Where-Object { $_ } | Select-Object -Unique) -join ";"
Write-Log ("Using PYTHONPATH: {0}" -f $env:PYTHONPATH)

$pythonRunner = Resolve-PythonRunner
if ($null -eq $pythonRunner) {
    Add-Failure "No executable Python 3.12 runtime found; pytest was not started."
} else {
    $pytestWrapper = Join-Path $repoRoot "scripts\run_pytest_target.py"
    if (-not (Test-Path -LiteralPath $pytestWrapper)) {
        Add-Failure "pytest wrapper missing: scripts\\run_pytest_target.py"
    }
    $pytestIndex = 0
    if ($script:Failures.Count -eq 0) {
        foreach ($pytestTarget in $PytestTargets) {
            $pytestTargetPath = Join-Path $repoRoot ($pytestTarget -replace '^[.\\/]+', '')
            if (-not (Test-Path -LiteralPath $pytestTargetPath)) {
                Add-Failure "pytest target missing: $pytestTarget"
                continue
            }

            $pytestIndex += 1
            $baseTemp = Join-Path $tempRoot ("pytest-temp-{0:D2}-{1}" -f $pytestIndex, $timestamp)
            New-Item -ItemType Directory -Force -Path $baseTemp | Out-Null
            $pytestArgs = @()
            if ($pythonRunner.Kind -eq "py") {
                $pytestArgs += "-3.12"
            }
            $pytestArgs += @(
                $pytestWrapper,
                $pytestTarget,
                "--basetemp",
                $baseTemp
            )

            $displayName = Get-DisplayTargetName $pytestTarget
            Update-RunState -Status "RUNNING" -Summary ("Running pytest: " + $displayName)
            $pytestExit = Run-ExternalCommand -Name ("pytest regression: " + $displayName) -FilePath $pythonRunner.Command -Arguments $pytestArgs
            if ($pytestExit -ne 0) {
                Add-Failure "pytest regression failed ($displayName), exit=$pytestExit"
            }
        }
    }
}

$nodeRunner = Resolve-NodeRunner
if ($null -ne $nodeRunner) {
    foreach ($nodeTarget in $NodeTargets) {
        $nodeTargetPath = Join-Path $repoRoot ($nodeTarget -replace '^[.\\/]+', '')
        if (Test-Path -LiteralPath $nodeTargetPath) {
            Update-RunState -Status "RUNNING" -Summary ("Running node regression: " + (Get-DisplayTargetName $nodeTarget))
            $nodeExit = Run-ExternalCommand -Name ("node regression: " + $nodeTarget) -FilePath $nodeRunner -Arguments @($nodeTarget)
            if ($nodeExit -ne 0) {
                Add-Failure "Node script failed ($nodeTarget), exit=$nodeExit"
            }
        } else {
            Write-Log ("Skipping Node target because the file is missing: {0}" -f $nodeTarget)
        }
    }
} else {
    Write-Log "Skipping Node regression scripts because no executable Node runtime was found."
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
