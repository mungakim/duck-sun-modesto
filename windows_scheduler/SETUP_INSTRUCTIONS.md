# Windows Task Scheduler Setup Guide

This guide walks you through setting up the Duck Sun weather report to run automatically on your Windows PC every morning.

## Why Local Scheduling?

GitHub Actions runs from datacenter IPs that weather sites (Weather.com, Weather Underground) actively block. Running from your home PC uses a residential IP that bypasses this detection.

## Prerequisites

1. **Git installed** and in your PATH
2. **Git credentials cached** via Windows Credential Manager
3. **Project cloned** to your Windows PC
4. **Python venv** already set up (the `venv` folder should exist)

## Step 1: Configure the Script

1. Open `run_weather_report.ps1` in a text editor
2. Find this line near the top:
   ```powershell
   $ProjectDir = "C:\Users\YourUsername\duck-sun-modesto"
   ```
3. Update it to match your actual project path, e.g.:
   ```powershell
   $ProjectDir = "C:\Users\John\Documents\duck-sun-modesto"
   ```

## Step 2: Test the Script Manually

Open PowerShell and run:

```powershell
# Navigate to the script location
cd "C:\Users\YourUsername\duck-sun-modesto\windows_scheduler"

# Run the script (may need to allow execution)
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\run_weather_report.ps1
```

The script should:
- Fetch weather data from all 11 sources
- Generate a PDF report
- Commit and push to GitHub

If it works, proceed to Step 3.

## Step 3: Create the Scheduled Task

### Option A: Using Task Scheduler GUI

1. Press `Win + R`, type `taskschd.msc`, press Enter
2. In the right panel, click **Create Task** (not "Create Basic Task")

**General Tab:**
- Name: `Duck Sun Weather Report`
- Description: `Daily solar forecast for Modesto, CA`
- Select: **Run whether user is logged on or not**
- Check: **Run with highest privileges**

**Triggers Tab:**
- Click **New...**
- Begin the task: **On a schedule**
- Settings: **Daily**
- Start: Pick a time (recommend **5:00 AM**)
- Recur every: **1** days
- Check: **Enabled**
- Click **OK**

**Actions Tab:**
- Click **New...**
- Action: **Start a program**
- Program/script: `powershell.exe`
- Add arguments:
  ```
  -ExecutionPolicy Bypass -File "C:\Users\YourUsername\duck-sun-modesto\windows_scheduler\run_weather_report.ps1"
  ```
  (Update the path to match your setup)
- Click **OK**

**Conditions Tab:**
- Uncheck: **Start the task only if the computer is on AC power**
- Uncheck: **Stop if the computer switches to battery power**

**Settings Tab:**
- Check: **Allow task to be run on demand**
- Check: **Run task as soon as possible after a scheduled start is missed**
- Check: **If the task fails, restart every:** `5 minutes`
- Attempt to restart up to: `3` times
- Check: **Stop the task if it runs longer than:** `1 hour`

3. Click **OK** and enter your Windows password when prompted

### Option B: Using Command Line (PowerShell Admin)

Run PowerShell as Administrator and execute:

```powershell
$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument '-ExecutionPolicy Bypass -File "C:\Users\YourUsername\duck-sun-modesto\windows_scheduler\run_weather_report.ps1"'

$trigger = New-ScheduledTaskTrigger -Daily -At 5:00AM

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5)

Register-ScheduledTask `
    -TaskName "Duck Sun Weather Report" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -RunLevel Highest `
    -Description "Daily solar forecast for Modesto, CA"
```

## Step 4: Verify It Works

1. In Task Scheduler, right-click your task and select **Run**
2. Watch the task execute (may take 2-3 minutes)
3. Check `History` tab in Task Scheduler for results
4. Check `logs/scheduler_YYYY-MM-DD.log` in your project folder

## Troubleshooting

### "Script is not digitally signed"
Run this in PowerShell:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Git push fails with 403
Your git credentials need to be cached. Run once in the project folder:
```cmd
git push origin main
```
When prompted, log in via browser. Windows will remember.

### Task runs but nothing happens
Check the log file at: `C:\Users\YourUsername\duck-sun-modesto\logs\scheduler_YYYY-MM-DD.log`

### Python not found
Make sure the venv exists:
```cmd
dir C:\Users\YourUsername\duck-sun-modesto\venv\Scripts\python.exe
```
If not, create it:
```cmd
python -m venv venv
venv\Scripts\pip.exe install -r requirements.txt
```

### Task doesn't run when computer is asleep
Either:
- Keep computer awake (not recommended)
- Use Windows wake timers: In Power Options → Change plan settings → Change advanced power settings → Sleep → Allow wake timers → Enable

## Alternative: Batch File

If PowerShell gives you trouble, use the batch file instead:

1. Edit `run_weather_report.bat` and update the PROJECT_DIR path
2. In Task Scheduler, use:
   - Program: `cmd.exe`
   - Arguments: `/c "C:\Users\YourUsername\duck-sun-modesto\windows_scheduler\run_weather_report.bat"`

## Disabling GitHub Actions

Once your local scheduler is working, you can disable the GitHub Actions workflow:

1. Go to your repo on GitHub
2. Click **Actions** tab
3. Click **Daily Duck Sun Forecast** workflow
4. Click the **...** menu and select **Disable workflow**

Or rename the workflow file:
```bash
mv .github/workflows/daily_forecast.yml .github/workflows/daily_forecast.yml.disabled
```

## Log Locations

- **Script log:** `logs/scheduler_YYYY-MM-DD.log`
- **Python scheduler log:** `logs/duck_sun.log`
- **Python stdout:** `logs/scheduler_stdout.log`
- **Python stderr:** `logs/scheduler_stderr.log`
