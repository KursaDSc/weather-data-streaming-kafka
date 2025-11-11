# PowerShell script for Ngrok and Kafka setup
Write-Host "🚀 Starting Ngrok and Kafka setup..." -ForegroundColor Green

# Check if Python is installed
try {
    $pythonVersion = python --version
    Write-Host "✅ Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Python not found. Please install Python 3.9 or higher." -ForegroundColor Red
    exit 1
}

# Check if Ngrok is installed
try {
    $ngrokVersion = ngrok --version
    Write-Host "✅ Ngrok found: $ngrokVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Ngrok not found. Please install Ngrok from https://ngrok.com/download" -ForegroundColor Red
    exit 1
}

# Check if Docker is running
try {
    $dockerInfo = docker info 2>$null
    Write-Host "✅ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker is not running. Please start Docker Desktop." -ForegroundColor Red
    exit 1
}

# Run the Python script
Write-Host "🎯 Executing Python setup script..." -ForegroundColor Yellow
python .\scripts\setup_ngrok_kafka.py

# Check if Python script executed successfully
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Setup completed successfully!" -ForegroundColor Green
} else {
    Write-Host "❌ Setup failed with error code: $LASTEXITCODE" -ForegroundColor Red
}

Write-Host "`nPress any key to continue..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")