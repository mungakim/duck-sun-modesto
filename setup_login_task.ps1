# Setup Windows Task Scheduler task for automatic forecast on login
# No admin rights required - creates a user-level task
#
# Usage: .\setup_login_task.ps1

param(
    [string]$TaskName = "DuckSunForecast",
    [int]$DelayMinutes = 1,  # Delay after login before running
    [switch]$Remove        # Use -Remove to delete the task
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if ($Remove) {
    # Remove existing task
    Write-Host "Removing scheduled task: $TaskName" -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Task removed successfully" -ForegroundColor Green
    exit 0
}

Write-Host "=== Setting up Duck Sun Forecast Login Task ===" -ForegroundColor Cyan
Write-Host "Task Name: $TaskName"
Write-Host "Script Directory: $ScriptDir"
Write-Host "Delay after login: $DelayMinutes minute(s)"
Write-Host ""

# Remove existing task if it exists
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing task..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Build the script path
$scriptPath = Join-Path $ScriptDir "run_forecast_on_login.ps1"

if (-not (Test-Path $scriptPath)) {
    Write-Host "ERROR: Script not found at $scriptPath" -ForegroundColor Red
    exit 1
}

# Create the scheduled task
$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -WindowStyle Normal -File `"$scriptPath`" -DelaySeconds 30" `
    -WorkingDirectory $ScriptDir

# Trigger: At logon with delay
$trigger = New-ScheduledTaskTrigger -AtLogOn
$trigger.Delay = "PT${DelayMinutes}M"  # ISO 8601 duration format

# Settings
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

# Principal: Run as current user, only when logged in
$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Limited

# Register the task
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "Runs Duck Sun Modesto forecast on login and opens Excel report"

Write-Host ""
Write-Host "=== Task Created Successfully ===" -ForegroundColor Green
Write-Host ""
Write-Host "The forecast will run automatically when you log in." -ForegroundColor Cyan
Write-Host ""
Write-Host "To test now, run:" -ForegroundColor Yellow
Write-Host "  Start-ScheduledTask -TaskName '$TaskName'"
Write-Host ""
Write-Host "To view task in Task Scheduler:" -ForegroundColor Yellow
Write-Host "  taskschd.msc"
Write-Host ""
Write-Host "To remove the task later:" -ForegroundColor Yellow
Write-Host "  .\setup_login_task.ps1 -Remove"
Write-Host ""

# Export task for coworkers
$exportPath = Join-Path $ScriptDir "DuckSunForecast_Task.xml"
Write-Host "Exporting task XML for coworkers: $exportPath" -ForegroundColor Cyan
Export-ScheduledTask -TaskName $TaskName | Out-File -FilePath $exportPath -Encoding UTF8

Write-Host ""
Write-Host "=== For Coworkers ===" -ForegroundColor Magenta
Write-Host "Share these files with coworkers:"
Write-Host "  1. The entire project folder (or git clone)"
Write-Host "  2. They run: .\setup_login_task.ps1"
Write-Host ""
Write-Host "Or import the XML manually:"
Write-Host "  schtasks /create /xml `"$exportPath`" /tn `"$TaskName`""
