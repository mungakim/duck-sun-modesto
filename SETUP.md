# Duck Sun Modesto - First-Time Setup Guide

Complete instructions for installing and running Duck Sun Modesto on a fresh machine.

## Prerequisites

- **Python 3.10+** (3.11 recommended)
- **pip** (Python package manager)
- **Git** (for cloning the repository)

## Step 1: Clone the Repository

```bash
git clone <repository-url>
cd duck-sun-modesto
```

## Step 2: Create a Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate

# On Windows (Command Prompt):
venv\Scripts\activate.bat

# On Windows (PowerShell):
venv\Scripts\Activate.ps1
```

## Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- `httpx` - Async HTTP client for API requests
- `python-dotenv` - Environment variable management
- `pandas` - Data processing for consensus model
- `fpdf2` - PDF report generation
- `curl-cffi` - Web scraping (Weather.com, Weather Underground)
- `beautifulsoup4` - HTML parsing
- And other supporting libraries

## Step 4: Create the `.env` File

Create a file named `.env` in the project root directory with your API keys:

```bash
# Create the .env file
touch .env
```

Add the following content to `.env`:

```env
# Required API Keys
GOOGLE_MAPS_API_KEY=your_google_maps_api_key_here
ACCUWEATHER_API_KEY=your_accuweather_api_key_here

# Optional Settings
LOG_LEVEL=INFO
```

### Getting API Keys

1. **Google Maps API Key (Primary Source - Weight 6x)**
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing
   - Enable the "Weather API" (Maps Platform Weather API)
   - Create credentials → API Key
   - Restrict the key to Weather API for security

2. **AccuWeather API Key (Weight 4x)**
   - Go to [AccuWeather Developer Portal](https://developer.accuweather.com/)
   - Create a free account
   - Register a new app to get your API key
   - Free tier: 50 calls/day (system uses max 42/day)

**Note:** Without API keys, the system will still work using the free sources (Open-Meteo, NOAA, Met.no), but accuracy may be reduced.

## Step 5: Run the Forecast

```bash
# With virtual environment activated:
python -m duck_sun.scheduler
```

Or run the main entry point:
```bash
python main.py
```

## Output Files

After running, you'll find:

- **PDF Report**: `reports/YYYY-MM/YYYY-MM-DD/daily_forecast_*.pdf`
- **JSON Data**: `outputs/solar_data_YYYY-MM-DD_HH-MM-SS.json`

## Quick Start (Copy-Paste)

For Linux/macOS, run these commands in order:

```bash
# 1. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create .env file (edit with your actual keys)
cat > .env << 'EOF'
GOOGLE_MAPS_API_KEY=your_google_maps_api_key_here
ACCUWEATHER_API_KEY=your_accuweather_api_key_here
LOG_LEVEL=INFO
EOF

# 4. Run the forecast
python -m duck_sun.scheduler
```

## Troubleshooting

### "ModuleNotFoundError: No module named 'duck_sun'"
Make sure you're running from the project root directory and your virtual environment is activated.

### "API key not found" warnings
The system will continue with available free sources. Add API keys to `.env` for full functionality.

### curl_cffi installation issues
On some systems, you may need to install additional dependencies:
```bash
# Ubuntu/Debian
sudo apt-get install libcurl4-openssl-dev

# macOS
brew install curl
```

### Weather.com scraping fails
Weather.com has aggressive anti-bot protection. Cloud/container IPs are often blocked. The system will fall back to other sources automatically.

## Verification

To verify the installation is working:

```bash
# Test individual providers
python -m duck_sun.providers.noaa
python -m duck_sun.providers.open_meteo
```

## Daily Automation

For automated daily runs, see `AUTOMATION_SETUP.md` for:
- GitHub Actions scheduling
- Windows Task Scheduler setup
- Linux cron job configuration
