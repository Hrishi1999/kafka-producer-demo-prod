# Install SQL-Kafka Producer as Windows Service
# Run as Administrator

param(
    [string]$ConfigDir = "C:\Kafka"
)

# Check if running as administrator
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Error "This script requires Administrator privileges"
    exit 1
}

Write-Host "Installing SQL-Kafka Producer Windows Service..."

# Create service directories
New-Item -ItemType Directory -Force -Path "$ConfigDir" | Out-Null
New-Item -ItemType Directory -Force -Path "$ConfigDir\logs" | Out-Null
New-Item -ItemType Directory -Force -Path "$ConfigDir\config" | Out-Null
New-Item -ItemType Directory -Force -Path "$ConfigDir\state" | Out-Null

# Copy configuration files
if (Test-Path "config.yaml") {
    Copy-Item "config.yaml" "$ConfigDir\config\config.yaml" -Force
    Write-Host "Copied config.yaml"
}

if (Test-Path ".env.template") {
    Copy-Item ".env.template" "$ConfigDir\config\.env" -Force
    Write-Host "Copied .env template - EDIT THIS FILE WITH YOUR CREDENTIALS"
}

# Install Python dependencies
pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to install Python dependencies"
    exit 1
}

# Install the service
python windows_service.py install
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to install service"
    exit 1
}

# Configure service recovery options
sc.exe failure SQLKafkaProducer reset= 86400 actions= restart/5000/restart/10000/restart/30000

Write-Host ""
Write-Host "Installation completed"
Write-Host "Config location: $ConfigDir\config\"
Write-Host "Logs location: $ConfigDir\logs\"
Write-Host ""
Write-Host "IMPORTANT: Edit $ConfigDir\config\.env with your actual credentials"
Write-Host ""
Write-Host "To start the service: net start SQLKafkaProducer"
Write-Host "To stop the service: net stop SQLKafkaProducer"
Write-Host "To check status: sc query SQLKafkaProducer"
Write-Host "To remove: python windows_service.py remove"