# 🤖 Automation Setup Guide - Duck Sun Modesto

This guide will help you set up **Windows Task Scheduler** to run your Duck Sun forecast automatically every morning at 6:00 AM.

---

## Quick Setup (5 Minutes)

### Step 1: Open Task Scheduler
1. Press `Win + R` on your keyboard
2. Type: `taskschd.msc`
3. Press `Enter`

### Step 2: Create Basic Task
1. In Task Scheduler, click **"Create Basic Task"** on the right sidebar
2. Name: `Duck Sun Modesto - Daily Brief`
3. Description: `Automated solar forecast with Tule Fog detection`
4. Click **Next**

### Step 3: Set Trigger (When It Runs)
1. Select: **"Daily"**
2. Click **Next**
3. Start date: Today's date
4. Start time: **06:00 AM** (or your preferred time)
5. Recur every: **1 days**
6. Click **Next**

### Step 4: Set Action (What It Runs)
1. Select: **"Start a program"**
2. Click **Next**
3. **Program/script:** `C:\Windows\System32\cmd.exe`
4. **Add arguments:** `/c run_forecast.bat`
5. **Start in:** `C:\Professional Projects\duck-sun-modesto\`
   - ⚠️ **CRITICAL:** This MUST be the folder where `main.py` lives
   - ⚠️ Do NOT include the batch file name, just the folder path
6. Click **Next**

### Step 5: Finish
1. Check **"Open the Properties dialog..."** box
2. Click **Finish**

### Step 6: Advanced Settings (Optional but Recommended)
In the Properties dialog that opens:

**General Tab:**
- ☑ Run whether user is logged on or not
- ☑ Run with highest privileges
- Configure for: **Windows 10** (or your Windows version)

**Conditions Tab:**
- ☐ Uncheck "Start the task only if the computer is on AC power"
  - (So it runs even on battery power)

**Settings Tab:**
- ☑ Allow task to be run on demand
- ☑ Run task as soon as possible after a scheduled start is missed
- If the task fails, restart every: **10 minutes**
- Attempt to restart up to: **3 times**

Click **OK** to save.

---

## Verification Test

### Test It Manually (Before Scheduling)
1. In Task Scheduler, find your task in the list
2. Right-click it → **Run**
3. Watch for:
   - Console window appears
   - Weather data fetches
   - Report generates
   - **Markdown file auto-opens** ✅
4. Check the output file: `reports\daily_brief_YYYY-MM-DD_HH-MM-SS.md`

### Troubleshooting
If the task fails:

**Issue: "The system cannot find the path specified"**
- Fix: Check **"Start in"** path in Step 4 - must be exact folder path
- Example: `C:\Professional Projects\duck-sun-modesto\`

**Issue: "Python not found"**
- Fix: Use full Python path in the batch file
- Edit `run_forecast.bat` line 28:
  ```batch
  python main.py
  ```
  Change to:
  ```batch
  C:\Users\YOUR_USERNAME\AppData\Local\Programs\Python\Python314\python.exe main.py
  ```

**Issue: Task runs but no report opens**
- Expected behavior: Task Scheduler runs in background (no UI)
- Reports still generate in `reports\` folder
- Auto-open only works when running batch file manually

**Issue: Virtual environment not activating**
- The batch file checks for `venv\Scripts\activate.bat`
- If missing, it uses system Python (this is fine)

---

## What Happens When It Runs

### At 6:00 AM Every Day:
1. **Task Scheduler wakes up** (even if PC is sleeping, if Wake Timer enabled)
2. **Batch file executes:**
   - Checks for virtual environment
   - Installs/updates requirements
   - Runs `main.py`
3. **Python script:**
   - Fetches Open-Meteo, NWS, Met.no, METAR
   - Builds consensus temperature model
   - Runs Fog Guard analysis with Pre-Dawn Lock-in
   - Detects any critical fog events
   - Generates AI briefing via Claude
4. **Outputs saved:**
   - Raw JSON: `outputs\solar_data_YYYY-MM-DD_HH-MM-SS.json`
   - Markdown brief: `reports\daily_brief_YYYY-MM-DD_HH-MM-SS.md`
   - Memory file: `C:\Professional Projects\duck_sun_agent_memory.json`

### You Wake Up To:
- Fresh forecast in `reports\` folder
- Critical fog alerts if Tule Fog detected
- 8-day temperature outlook
- Tomorrow's Duck Curve (HE09-16) with risk levels

---

## Advanced Options

### Email Integration (Future)
To get the briefing in your inbox, add this to the batch file (requires `sendmail` or similar):

```batch
:: After line 28 (python main.py)
FOR /F "delims=" %%I IN ('DIR "reports\*.md" /B /O:D') DO SET LAST_REPORT=%%I
powershell -Command "Send-MailMessage -To 'you@example.com' -From 'ducksun@modesto' -Subject 'Solar Forecast' -Body (Get-Content 'reports\%LAST_REPORT%' -Raw) -SmtpServer 'smtp.example.com'"
```

### Multiple Daily Runs
If you want forecasts at **6 AM, 12 PM, and 6 PM**:
1. Create 3 separate tasks in Task Scheduler
2. Name them: `Duck Sun - Morning`, `Duck Sun - Noon`, `Duck Sun - Evening`
3. Use the same batch file for all three
4. Set different trigger times

### Wake Computer From Sleep
**General Tab → Conditions:**
- ☑ Wake the computer to run this task

**Settings Tab:**
- ☑ Run task as soon as possible after a scheduled start is missed

This ensures if your PC is asleep at 6 AM, it wakes up to run the forecast.

---

## File Locations Reference

```
c:\Professional Projects\duck-sun-modesto\
├── run_forecast.bat          ← The batch file Task Scheduler runs
├── main.py                    ← Main Python script
├── duck_sun\                  ← Code modules
│   ├── agent.py
│   ├── memory.py
│   ├── uncanniness.py
│   └── providers\
├── reports\                   ← Generated briefs (check here daily)
│   └── daily_brief_*.md
├── outputs\                   ← Raw JSON data
│   └── solar_data_*.json
└── duck_sun_agent_memory.json ← Persistent memory
```

---

## Monitoring & Maintenance

### Weekly Checklist
- [ ] Check `reports\` folder for 7 daily briefs
- [ ] Verify Task Scheduler history (right-click task → History)
- [ ] Compare fog predictions vs. reality (look outside at 11 AM)
- [ ] Review `duck_sun_agent_memory.json` for accuracy trends

### Monthly Cleanup
```batch
:: Delete reports older than 30 days
forfiles /p "c:\Professional Projects\duck-sun-modesto\reports" /s /m *.md /d -30 /c "cmd /c del @path"

:: Delete output JSON older than 30 days  
forfiles /p "c:\Professional Projects\duck-sun-modesto\outputs" /s /m *.json /d -30 /c "cmd /c del @path"
```

### Log Files
The system logs everything to console. To save logs:

Edit `run_forecast.bat` line 28:
```batch
python main.py > "logs\run_%date:~-4,4%-%date:~-7,2%-%date:~-10,2%.log" 2>&1
```

Create a `logs\` folder first:
```batch
mkdir "c:\Professional Projects\duck-sun-modesto\logs"
```

---

## Security Notes

### Credentials
- All API keys loaded from environment variables (never hardcoded)
- Memory file contains no sensitive data

### Network Access
The system calls these endpoints:
- `api.open-meteo.com` (weather data)
- `api.weather.gov` (NWS forecast)
- `api.met.no` (European model)
- `tgftp.nws.noaa.gov` (METAR observations)
- `dataservice.accuweather.com` (AccuWeather forecast)
- `weather.googleapis.com` (Google Weather / MetNet-3)
- `weather.com` (The Weather Company)
- `wunderground.com` (Weather Underground)

### Security Controls
- HTTPS connections configured to trust MID corporate SSL proxy inspection certificates
- Web scraping rate-limited to 3 calls/day per source to minimize external footprint
- API error responses truncated in logs to limit data exposure

All traffic flows through MID's corporate proxy. No data is sent outbound except API requests.

---

## Troubleshooting Task Scheduler

### View Task History
1. Right-click your task → **Properties**
2. Go to **History** tab
3. Look for:
   - **Event ID 100:** Task started
   - **Event ID 102:** Task completed successfully
   - **Event ID 103:** Task failed

### Common Exit Codes
- `0x0` = Success
- `0x1` = Python script error (check logs)
- `0x41301` = Task still running from previous trigger
- `0x800710E0` = Task Scheduler service not running

### Enable All Tasks History
If History tab is empty:
1. Click **Action** menu → **Enable All Tasks History**
2. Wait for next run

---

## Success Checklist

You'll know it's working when:
- ✅ Task Scheduler History shows "Completed" (exit code 0x0)
- ✅ New files appear in `reports\` folder each morning
- ✅ File timestamps match scheduled run time (6:00 AM)
- ✅ Agent memory file updates with latest run
- ✅ Console output (if visible) shows all 4 weather sources fetch successfully

---

## Need Help?

If you encounter issues:

1. **Run manually first:** Double-click `run_forecast.bat`
   - If it works manually, problem is Task Scheduler setup
   - If it fails manually, problem is code/environment

2. **Check logs:** Look at console output for errors
   - "Module not found" → run `pip install -r requirements.txt`
   - "API error" → check internet connection
   - "File not found" → check working directory path

3. **Test scheduler:** Right-click task → **Run**
   - Watch Task Scheduler History tab
   - Note any error codes

---

**You're all set!** The Duck Sun Modesto system will now greet you with a fresh forecast every morning. 🌅

---

*Last Updated: December 12, 2025*
