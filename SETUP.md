# Duck Sun Modesto - First-Time Setup Guide (Windows)

Step-by-step instructions for installing and running Duck Sun Modesto on Windows.

**Target install location:** `C:\Professional Projects\duck-sun-modesto`

**Prerequisites:** Git installed (https://git-scm.com/download/win)

---

## Step 1: Install Python

### 1.1 Download Python

1. Open your web browser
2. Go to: https://www.python.org/downloads/
3. Click the yellow "Download Python 3.x.x" button (get version 3.11 or newer)
4. Save the file (e.g., `python-3.12.x-amd64.exe`) to your Downloads folder

### 1.2 Run the Python Installer

1. Open File Explorer
2. Navigate to your Downloads folder
3. Double-click `python-3.12.x-amd64.exe`
4. **IMPORTANT:** Check the box that says **"Add python.exe to PATH"** at the bottom
5. Click "Install Now"
6. Wait for installation to complete
7. Click "Close"

### 1.3 Verify Python Installation

1. Press `Windows + R` to open Run dialog
2. Type `cmd` and press Enter
3. In the black command prompt window, type:
   ```
   python --version
   ```
4. Press Enter
5. You should see something like: `Python 3.12.4`

If you see `'python' is not recognized`, close the command prompt and reopen it (the PATH needs to refresh).

---

## Step 2: Clone the Repository from GitHub

### 2.1 Open Command Prompt

1. Press `Windows + R` to open Run dialog
2. Type `cmd` and press Enter

### 2.2 Navigate to Parent Folder

1. In the command prompt, type:
   ```
   cd "C:\Professional Projects"
   ```
2. Press Enter

### 2.3 Verify Parent Folder Exists

1. Type:
   ```
   dir
   ```
2. Press Enter
3. If you get "The system cannot find the path specified", create it first:
   ```
   mkdir "C:\Professional Projects"
   cd "C:\Professional Projects"
   ```

### 2.4 Clone the Repository

1. Type:
   ```
   git clone https://github.com/mungakim/duck-sun-modesto.git
   ```
2. Press Enter
3. Wait for download to complete (you'll see progress messages)
4. When done, you'll see: `Cloning into 'duck-sun-modesto'...` followed by `done.`

### 2.5 Verify Clone Succeeded

1. Type:
   ```
   dir duck-sun-modesto
   ```
2. Press Enter
3. You should see files like `requirements.txt`, `main.py`, `CLAUDE.md`

---

## Step 3: Navigate into Project Folder

### 3.1 Change Directory

1. In the same command prompt, type:
   ```
   cd duck-sun-modesto
   ```
2. Press Enter
3. Your prompt should now show:
   ```
   C:\Professional Projects\duck-sun-modesto>
   ```

### 3.2 Verify You're in the Right Folder

1. Type:
   ```
   dir
   ```
2. Press Enter
3. You should see files like `requirements.txt`, `main.py`, `CLAUDE.md`, `duck_sun` folder

---

## Step 4: Create Virtual Environment

### 4.1 Create the Virtual Environment

1. In the same command prompt, type:
   ```
   python -m venv venv
   ```
2. Press Enter
3. Wait 10-30 seconds (no output means success)

### 4.2 Verify Virtual Environment Was Created

1. Type:
   ```
   dir venv
   ```
2. Press Enter
3. You should see folders including `Scripts` and `Lib`

---

## Step 5: Activate Virtual Environment

### 5.1 Activate

1. In the same command prompt, type:
   ```
   venv\Scripts\activate.bat
   ```
2. Press Enter
3. Your prompt should now show `(venv)` at the beginning, like:
   ```
   (venv) C:\Professional Projects\duck-sun-modesto>
   ```

**Note:** You must activate the virtual environment every time you open a new command prompt to run the forecast.

---

## Step 6: Install Dependencies

### 6.1 Install Required Packages

1. With `(venv)` showing in your prompt, type:
   ```
   pip install -r requirements.txt
   ```
2. Press Enter
3. Wait for packages to download and install (1-3 minutes)
4. You'll see many lines of output ending with "Successfully installed..."

### 6.2 Verify Installation

1. Type:
   ```
   pip list
   ```
2. Press Enter
3. You should see packages like `httpx`, `pandas`, `fpdf2`, `python-dotenv`

---

## Step 7: Create the .env File

### 7.1 Create the File

1. Open Notepad (press `Windows`, type `notepad`, press Enter)
2. In Notepad, type the following exactly:
   ```
   GOOGLE_MAPS_API_KEY=your_google_key_here
   ACCUWEATHER_API_KEY=your_accuweather_key_here
   LOG_LEVEL=INFO
   ```
3. Click File > Save As
4. In the "Save as type" dropdown, select "All Files (*.*)"
5. In the "File name" field, type exactly: `.env`
6. Navigate to: `C:\Professional Projects\duck-sun-modesto`
7. Click Save

### 7.2 Verify the .env File Exists

1. Back in your command prompt, type:
   ```
   dir .env
   ```
2. Press Enter
3. You should see the `.env` file listed

### 7.3 Get Your API Keys

**Google Maps Weather API Key:**
1. Go to: https://console.cloud.google.com/
2. Sign in with your Google account
3. Click "Select a project" > "New Project"
4. Name it (e.g., "Duck Sun Modesto") and click "Create"
5. In the search bar at top, type "Weather API" and select it
6. Click "Enable"
7. Click "Credentials" in the left sidebar
8. Click "Create Credentials" > "API Key"
9. Copy the key and paste it into your `.env` file replacing `your_google_key_here`

**AccuWeather API Key:**
1. Go to: https://developer.accuweather.com/
2. Click "Register" and create an account
3. After logging in, click "My Apps"
4. Click "Add a new App"
5. Fill in the form (App name: "Duck Sun", Product: "Limited Trial", etc.)
6. Click "Create App"
7. Copy the API Key and paste it into your `.env` file replacing `your_accuweather_key_here`

### 7.4 Edit the .env File with Real Keys

1. In File Explorer, navigate to `C:\Professional Projects\duck-sun-modesto`
2. Right-click on `.env` and select "Open with" > "Notepad"
3. Replace `your_google_key_here` with your actual Google API key
4. Replace `your_accuweather_key_here` with your actual AccuWeather API key
5. Save the file (Ctrl+S)

---

## Step 8: Run the Forecast

### 8.1 Run the Scheduler

1. Make sure your command prompt shows `(venv)` and you're in the project folder
2. Type:
   ```
   python -m duck_sun.scheduler
   ```
3. Press Enter
4. Wait for the forecast to run (30 seconds to 2 minutes)

### 8.2 Find Your Output

1. Open File Explorer
2. Navigate to: `C:\Professional Projects\duck-sun-modesto\reports`
3. Open the folder with today's date (e.g., `2026-01\2026-01-16`)
4. Double-click the PDF file to view your forecast

---

## Running Again Later

Every time you want to run a new forecast:

1. Open Command Prompt
2. Navigate to project:
   ```
   cd "C:\Professional Projects\duck-sun-modesto"
   ```
3. Activate virtual environment:
   ```
   venv\Scripts\activate.bat
   ```
4. Run forecast:
   ```
   python -m duck_sun.scheduler
   ```

---

## Troubleshooting

### "'python' is not recognized as an internal or external command"
- Close command prompt and reopen it
- If still failing, reinstall Python and make sure to check "Add python.exe to PATH"

### "No module named 'duck_sun'"
- Make sure you're in the correct folder: `C:\Professional Projects\duck-sun-modesto`
- Make sure virtual environment is activated (you see `(venv)` in prompt)

### "pip install" fails with SSL errors
- Try running Command Prompt as Administrator (right-click > Run as administrator)

### .env file shows as ".env.txt" in File Explorer
- Windows may have added .txt extension
- In File Explorer, click View > Show > File name extensions
- Rename the file from `.env.txt` to `.env`

### "API key not found" warnings during run
- The forecast will still work with free sources (NOAA, Open-Meteo, Met.no)
- For best accuracy, add your API keys to the `.env` file
