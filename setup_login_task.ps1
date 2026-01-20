# Setup Windows Task Scheduler task for automatic forecast on login
# No admin rights required - uses schtasks.exe for user-level task
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
    Write-Host "Removing scheduled task: $TaskName" -ForegroundColor Yellow
    schtasks /delete /tn $TaskName /f 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Task removed successfully" -ForegroundColor Green
    } else {
        Write-Host "Task not found or already removed" -ForegroundColor Yellow
    }
    exit 0
}

Write-Host "=== Setting up Duck Sun Forecast Login Task ===" -ForegroundColor Cyan
Write-Host "Task Name: $TaskName"
Write-Host "Script Directory: $ScriptDir"
Write-Host "Delay after login: $DelayMinutes minute(s)"
Write-Host ""

# Remove existing task if it exists
Write-Host "Checking for existing task..." -ForegroundColor Gray
schtasks /delete /tn $TaskName /f 2>$null

# Build the script path
$scriptPath = Join-Path $ScriptDir "run_forecast_on_login.ps1"

if (-not (Test-Path $scriptPath)) {
    Write-Host "ERROR: Script not found at $scriptPath" -ForegroundColor Red
    exit 1
}

# Create XML task definition (schtasks /create with ONLOGON needs this for delay)
$delayISO = "PT${DelayMinutes}M"
$taskXml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <Delay>$delayISO</Delay>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <ExecutionTimeLimit>PT30M</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>powershell.exe</Command>
      <Arguments>-ExecutionPolicy Bypass -WindowStyle Normal -File "$scriptPath" -DelaySeconds 30</Arguments>
      <WorkingDirectory>$ScriptDir</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@

# Save XML to temp file
$xmlPath = Join-Path $env:TEMP "DuckSunForecast_Task.xml"
$taskXml | Out-File -FilePath $xmlPath -Encoding Unicode

# Create the task using schtasks
Write-Host "Creating scheduled task..." -ForegroundColor Cyan
$result = schtasks /create /tn $TaskName /xml $xmlPath /f 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to create task" -ForegroundColor Red
    Write-Host $result -ForegroundColor Red
    Remove-Item $xmlPath -ErrorAction SilentlyContinue
    exit 1
}

# Clean up temp file
Remove-Item $xmlPath -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Task Created Successfully ===" -ForegroundColor Green
Write-Host ""
Write-Host "The forecast will run automatically when you log in." -ForegroundColor Cyan
Write-Host ""
Write-Host "To test now, run:" -ForegroundColor Yellow
Write-Host "  schtasks /run /tn '$TaskName'"
Write-Host ""
Write-Host "To view in Task Scheduler GUI:" -ForegroundColor Yellow
Write-Host "  taskschd.msc"
Write-Host ""
Write-Host "To remove the task later:" -ForegroundColor Yellow
Write-Host "  .\setup_login_task.ps1 -Remove"
Write-Host ""
Write-Host "=== For Coworkers ===" -ForegroundColor Magenta
Write-Host "They just need to run: .\setup_login_task.ps1"
Write-Host "(No admin rights required)"
