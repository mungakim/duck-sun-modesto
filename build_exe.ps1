# Build, sign, and deploy DuckSunForecast.exe in one step
# Usage: .\build_exe.ps1

$ErrorActionPreference = "Stop"

$exePath = "dist\DuckSunForecast.exe"
$deployPath = "X:\Operatns\Pwrsched\Weather\DuckSunForecast.exe"

# --- Step 1: Clean old build ---
Write-Host "=== Cleaning old build ===" -ForegroundColor Cyan
if (Test-Path $exePath) {
    Remove-Item $exePath -Force
    Write-Host "Removed old $exePath"
}

# --- Step 2: Build exe ---
Write-Host ""
Write-Host "=== Building exe ===" -ForegroundColor Cyan
& .\venv\Scripts\pyinstaller.exe --onefile --name DuckSunForecast --collect-data certifi --hidden-import truststore --hidden-import jinja2 run_forecast_cli.py

if ($LASTEXITCODE -ne 0 -or -not (Test-Path $exePath)) {
    Write-Host "ERROR: PyInstaller build failed." -ForegroundColor Red
    exit 1
}

$size = [math]::Round((Get-Item $exePath).Length / 1MB, 1)
Write-Host "Built $exePath ($size MB)" -ForegroundColor Green

# --- Step 3: Sign exe ---
Write-Host ""
Write-Host "=== Signing exe ===" -ForegroundColor Cyan
$cert = Get-ChildItem Cert:\CurrentUser\My -CodeSigningCert | Where-Object { $_.Thumbprint -eq "A216DBEA9E3F569B6A81XXXXXXXXXXXXX0A5" }

if (-not $cert) {
    Write-Host "ERROR: Code signing certificate not found in CurrentUser\My store." -ForegroundColor Red
    Write-Host "Run: Get-ChildItem Cert:\CurrentUser\My -CodeSigningCert | FL Subject, Thumbprint" -ForegroundColor Yellow
    exit 1
}

Write-Host "Using certificate: $($cert.Subject)" -ForegroundColor Gray
$result = Set-AuthenticodeSignature -FilePath $exePath -Certificate $cert -TimestampServer "http://timestamp.digicert.com" -HashAlgorithm SHA256

if ($result.Status -ne "Valid") {
    Write-Host "ERROR: Signing failed with status: $($result.Status)" -ForegroundColor Red
    exit 1
}

Write-Host "Signature valid." -ForegroundColor Green

# --- Step 4: Deploy to network ---
Write-Host ""
Write-Host "=== Deploying to X: drive ===" -ForegroundColor Cyan

if (-not (Test-Path (Split-Path $deployPath))) {
    Write-Host "ERROR: X: drive not mapped. Connect to network drive first." -ForegroundColor Red
    exit 1
}

Copy-Item $exePath $deployPath -Force
Write-Host "Copied to $deployPath" -ForegroundColor Green

# --- Verify ---
Write-Host ""
Write-Host "=== Verifying ===" -ForegroundColor Cyan
$sig = Get-AuthenticodeSignature $deployPath
Write-Host "Deployed signature status: $($sig.Status)" -ForegroundColor $(if ($sig.Status -eq "Valid") { "Green" } else { "Red" })
Get-Item $deployPath | Select-Object LastWriteTime, Length

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
