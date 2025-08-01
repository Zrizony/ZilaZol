#!/usr/bin/env pwsh
# Setup Cloud Scheduler for daily crawler runs

param(
    [string]$ProjectId = "civic-ripsaw-466109-e2",
    [string]$Region = "me-west1",
    [string]$ServiceName = "price-crawler"
)

Write-Host "🕐 Setting up Cloud Scheduler for daily crawler runs..." -ForegroundColor Green

# Set project
gcloud config set project $ProjectId

# Get the Cloud Run service URL
Write-Host "Getting Cloud Run service URL..." -ForegroundColor Yellow
$service_url = gcloud run services describe $ServiceName --region=$Region --format="value(status.url)" 2>$null

if (-not $service_url) {
    Write-Host "❌ Cloud Run service not found. Please deploy first using: .\deploy.ps1" -ForegroundColor Red
    exit 1
}

Write-Host "Service URL: $service_url" -ForegroundColor Cyan

# Create Cloud Scheduler job
Write-Host "Creating daily scheduler job..." -ForegroundColor Yellow
gcloud scheduler jobs create http daily-crawler `
    --schedule="0 7 * * *" `
    --uri="$service_url/crawl" `
    --http-method=POST `
    --location=europe-west1 `
    --description="Daily price crawler run at 7 AM Jerusalem time" `
    --time-zone="Asia/Jerusalem" `
    --headers="Content-Type=application/json" `
    --message-body="{}" `
    --attempt-deadline="1800s"

Write-Host "✅ Daily scheduler created!" -ForegroundColor Green
Write-Host "The crawler will now run automatically every day at 7 AM (Jerusalem time)" -ForegroundColor Cyan

# List all scheduler jobs
Write-Host "`nCurrent scheduler jobs:" -ForegroundColor Yellow
gcloud scheduler jobs list --location=europe-west1

Write-Host "`n=== Scheduler Setup Complete ===" -ForegroundColor Green
Write-Host "Service URL: $service_url" -ForegroundColor Cyan
Write-Host "Scheduler will trigger: POST $service_url/crawl" -ForegroundColor Cyan
Write-Host "Schedule: Daily at 7 AM Jerusalem time" -ForegroundColor Cyan 