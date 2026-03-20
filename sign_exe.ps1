# Sign DuckSunForecast.exe with MID code signing certificate and deploy to X: drive
# Usage: .\sign_exe.ps1

$ErrorActionPreference = "Stop"

$exePath = "dist\DuckSunForecast.exe"
$deployPath = "X:\Operatns\Pwrsched\Weather\DuckSunForecast.exe"

if (-not (Test-Path $exePath)) {
    Write-Host "ERROR: $exePath not found. Run PyInstaller first." -ForegroundColor Red
    exit 1
}

Write-Host "=== Signing exe ===" -ForegroundColor Cyan
$cert = Get-ChildItem Cert:\CurrentUser\My -CodeSigningCert | Where-Object { $_.Thumbprint -eq "A216DBEA9E3F569B6A81XXXXXXXXXXXXX0A5" }

if (-not $cert) {
    Write-Host "ERROR: MID code signing certificate not found in CurrentUser\My store." -ForegroundColor Red
    Write-Host "Open mmc.exe -> Certificates to verify it is installed." -ForegroundColor Yellow
    exit 1
}

Write-Host "Using certificate: $($cert.Subject)" -ForegroundColor Gray
$result = Set-AuthenticodeSignature -FilePath $exePath -Certificate $cert -TimestampServer "http://timestamp.digicert.com" -HashAlgorithm SHA256

if ($result.Status -ne "Valid") {
    Write-Host "ERROR: Signing failed with status: $($result.Status)" -ForegroundColor Red
    exit 1
}

Write-Host "Signature valid." -ForegroundColor Green

Write-Host ""
Write-Host "=== Deploying to X: drive ===" -ForegroundColor Cyan
Copy-Item $exePath $deployPath -Force
Write-Host "Copied to $deployPath" -ForegroundColor Green

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
