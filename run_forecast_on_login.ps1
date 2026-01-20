# Run forecast on login and open the Excel report
# This script is designed to be triggered by Windows Task Scheduler at logon
#
# Setup: Run setup_login_task.ps1 as Administrator to create the scheduled task
# Manual run: .\run_forecast_on_login.ps1

param(
    [int]$DelaySeconds = 30  # Wait for network/system to settle after login
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# Log file for troubleshooting
$LogFile = Join-Path $ScriptDir "logs\login_forecast.log"
$LogDir = Split-Path -Parent $LogFile
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] $Message"
    Add-Content -Path $LogFile -Value $logMessage
    Write-Host $logMessage
}

Write-Log "=== Forecast Login Script Started ==="
Write-Log "Working directory: $ScriptDir"

# Wait for system to settle (network, etc.)
if ($DelaySeconds -gt 0) {
    Write-Log "Waiting $DelaySeconds seconds for system to settle..."
    Start-Sleep -Seconds $DelaySeconds
}

# Check for network connectivity (GitHub push will need it)
$maxRetries = 5
$retryDelay = 10
for ($i = 1; $i -le $maxRetries; $i++) {
    $connected = Test-Connection -ComputerName "github.com" -Count 1 -Quiet
    if ($connected) {
        Write-Log "Network connectivity confirmed"
        break
    }
    Write-Log "Network not ready, retry $i of $maxRetries..."
    Start-Sleep -Seconds $retryDelay
}

if (-not $connected) {
    Write-Log "ERROR: No network connectivity after $maxRetries retries. Aborting."
    exit 1
}

# Run the forecast
Write-Log "Running forecast..."
try {
    & "$ScriptDir\run_and_push.ps1"
    Write-Log "Forecast completed successfully"
} catch {
    Write-Log "ERROR: Forecast failed - $_"
    exit 1
}

# Find the latest xlsx file in reports folder
Write-Log "Looking for latest Excel report..."
$reportsDir = Join-Path $ScriptDir "reports"
$latestXlsx = Get-ChildItem -Path $reportsDir -Filter "*.xlsx" -Recurse |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

if ($latestXlsx) {
    Write-Log "Opening: $($latestXlsx.FullName)"

    # Open Excel file and bring to focus
    Start-Process -FilePath $latestXlsx.FullName

    # Wait a moment for Excel to open, then bring to foreground
    Start-Sleep -Seconds 3

    # Use COM to bring Excel to focus
    try {
        $excel = [Runtime.InteropServices.Marshal]::GetActiveObject("Excel.Application")
        $excel.Visible = $true
        $excel.WindowState = -4137  # xlMaximized

        # Bring window to front using Windows API
        Add-Type @"
            using System;
            using System.Runtime.InteropServices;
            public class WindowHelper {
                [DllImport("user32.dll")]
                public static extern bool SetForegroundWindow(IntPtr hWnd);
                [DllImport("user32.dll")]
                public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
            }
"@
        $hwnd = $excel.Hwnd
        [WindowHelper]::ShowWindow($hwnd, 3)  # SW_MAXIMIZE
        [WindowHelper]::SetForegroundWindow($hwnd)

        Write-Log "Excel opened and brought to focus"
    } catch {
        Write-Log "Excel opened (focus helper not available: $_)"
    }
} else {
    Write-Log "WARNING: No xlsx file found in $reportsDir"
}

Write-Log "=== Forecast Login Script Completed ==="
