# PowerShell Deployment Script for Price Crawler
# Run this script to deploy the crawler to Google Cloud Run

Write-Host "=== Price Crawler Deployment Script ===" -ForegroundColor Green
Write-Host "Timestamp: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Yellow

# Check if gcloud is available
$gcloudPath = Get-Command gcloud -ErrorAction SilentlyContinue
if ($gcloudPath) {
    Write-Host "✓ gcloud CLI found at: $($gcloudPath.Source)" -ForegroundColor Green
} else {
    Write-Host "✗ gcloud CLI not found in PATH" -ForegroundColor Red
    Write-Host "Please install gcloud CLI from: https://cloud.google.com/sdk/docs/install" -ForegroundColor Yellow
    Write-Host "Or use Google Cloud Console for deployment" -ForegroundColor Yellow
    exit 1
}

# Check current directory
$currentDir = Get-Location
Write-Host "Current directory: $currentDir" -ForegroundColor Cyan

# Check if required files exist
$requiredFiles = @("app.py", "Dockerfile", "requirements.txt", "crawler_cloud.py")
$missingFiles = @()

foreach ($file in $requiredFiles) {
    if (Test-Path $file) {
        Write-Host "✓ Found $file" -ForegroundColor Green
    } else {
        Write-Host "✗ Missing $file" -ForegroundColor Red
        $missingFiles += $file
    }
}

if ($missingFiles.Count -gt 0) {
    Write-Host "Missing required files: $($missingFiles -join ', ')" -ForegroundColor Red
    exit 1
}

# Check if user is authenticated
Write-Host "`nChecking authentication..." -ForegroundColor Yellow
try {
    $authResult = gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>$null
    if ($authResult) {
        Write-Host "✓ Authenticated as: $authResult" -ForegroundColor Green
    } else {
        Write-Host "✗ Not authenticated" -ForegroundColor Red
        Write-Host "Running: gcloud auth login" -ForegroundColor Yellow
        gcloud auth login
    }
} catch {
    Write-Host "✗ Authentication check failed" -ForegroundColor Red
    Write-Host "Please run: gcloud auth login" -ForegroundColor Yellow
    exit 1
}

# Check project configuration
Write-Host "`nChecking project configuration..." -ForegroundColor Yellow
try {
    $project = gcloud config get-value project 2>$null
    if ($project -eq "civic-ripsaw-466109-e2") {
        Write-Host "✓ Project set to: $project" -ForegroundColor Green
    } else {
        Write-Host "✗ Project is set to: $project" -ForegroundColor Red
        Write-Host "Setting project to: civic-ripsaw-466109-e2" -ForegroundColor Yellow
        gcloud config set project civic-ripsaw-466109-e2
    }
} catch {
    Write-Host "✗ Project configuration failed" -ForegroundColor Red
    exit 1
}

# Deployment options
Write-Host "`n=== Deployment Options ===" -ForegroundColor Green
Write-Host "1. Deploy with default settings" -ForegroundColor Cyan
Write-Host "2. Deploy with custom settings" -ForegroundColor Cyan
Write-Host "3. Deploy with no traffic (for testing)" -ForegroundColor Cyan
Write-Host "4. Exit" -ForegroundColor Cyan

$choice = Read-Host "`nSelect option (1-4)"

switch ($choice) {
    "1" {
        Write-Host "`nDeploying with default settings..." -ForegroundColor Green
        gcloud run deploy price-crawler --source . --region=me-west1 --allow-unauthenticated
    }
    "2" {
        Write-Host "`nDeploying with custom settings..." -ForegroundColor Green
        gcloud run deploy price-crawler --source . --region=me-west1 --allow-unauthenticated --memory=2Gi --cpu=2 --timeout=3600 --max-instances=1
    }
    "3" {
        Write-Host "`nDeploying with no traffic (for testing)..." -ForegroundColor Green
        gcloud run deploy price-crawler --source . --region=me-west1 --allow-unauthenticated --no-traffic
    }
    "4" {
        Write-Host "Exiting..." -ForegroundColor Yellow
        exit 0
    }
    default {
        Write-Host "Invalid option. Exiting..." -ForegroundColor Red
        exit 1
    }
}

Write-Host "`n=== Deployment Complete ===" -ForegroundColor Green
Write-Host "Service URL: https://price-crawler-947639158495.me-west1.run.app" -ForegroundColor Cyan
Write-Host "Health check: https://price-crawler-947639158495.me-west1.run.app/" -ForegroundColor Cyan
Write-Host "Test endpoint: https://price-crawler-947639158495.me-west1.run.app/test" -ForegroundColor Cyan
Write-Host "Trigger crawler: POST https://price-crawler-947639158495.me-west1.run.app/crawl" -ForegroundColor Cyan 