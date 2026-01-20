# Automatic Forecast on Login Setup

This guide explains how to set up the Duck Sun forecast to run automatically when you log into Windows.

## What It Does

1. Waits 1 minute after login for system/network to settle
2. Runs the daily forecast (`run_and_push.ps1`)
3. Commits and pushes results to GitHub
4. Opens the latest Excel report and brings it to focus

## Quick Setup (Your Machine)

1. **Open PowerShell** (no admin required)

2. **Navigate to the project folder**
   ```powershell
   cd C:\path\to\duck-sun-modesto
   ```

3. **Run the setup script**
   ```powershell
   .\setup_login_task.ps1
   ```

4. **Done!** The forecast will run automatically on your next login.

## Test Without Logging Out

```powershell
# Test the scheduled task immediately
Start-ScheduledTask -TaskName "DuckSunForecast"

# Or run the script directly
.\run_forecast_on_login.ps1 -DelaySeconds 0
```

## Setup for Coworkers

### Option A: Run Setup Script (Recommended)

1. Clone or copy the project folder to coworker's machine
2. Ensure Python venv and dependencies are set up
3. Have coworker run:
   ```powershell
   .\setup_login_task.ps1
   ```

No admin rights required - creates a user-level scheduled task.

### Option B: Import Task XML

If the setup script doesn't work, import the pre-exported task:

```powershell
schtasks /create /xml "DuckSunForecast_Task.xml" /tn "DuckSunForecast"
```

**Note:** The XML contains the original user's paths. Coworker may need to edit the task in Task Scheduler to update the path to their project folder.

## Customization

### Change the delay after login

```powershell
.\setup_login_task.ps1 -DelayMinutes 2  # Wait 2 minutes instead of 1
```

### Remove the scheduled task

```powershell
.\setup_login_task.ps1 -Remove
```

### View/Edit in Task Scheduler GUI

1. Press `Win + R`
2. Type `taskschd.msc` and press Enter
3. Find "DuckSunForecast" in the task list

## Troubleshooting

### Check the log file

Logs are saved to `logs\login_forecast.log` in the project folder.

### Task doesn't run

1. Open Task Scheduler (`taskschd.msc`)
2. Find "DuckSunForecast"
3. Check "Last Run Result" column
4. Right-click -> "Run" to test manually

### Excel doesn't open

- Ensure Excel/Office is installed
- Check that xlsx files are associated with Excel

### Network errors

The script waits for network connectivity before running. If your network takes longer to connect:

```powershell
.\setup_login_task.ps1 -DelayMinutes 3  # Increase delay
```

## Files

| File | Purpose |
|------|---------|
| `run_forecast_on_login.ps1` | Main script that runs on login |
| `setup_login_task.ps1` | Creates the Windows scheduled task |
| `DuckSunForecast_Task.xml` | Exported task (for manual import) |
| `logs\login_forecast.log` | Log file for troubleshooting |
