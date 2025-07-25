#!/usr/bin/env pwsh
# Fix Cloud Scheduler timeout for daily crawler runs

param(
    [string]$ProjectId = "civic-ripsaw-466109-e2",
    [string]$Region = "europe-west1",
    [string]$JobName = "daily-crawler"
)

Write-Host "🔧 Fixing Cloud Scheduler timeout..." -ForegroundColor Green

# Set project
gcloud config set project $ProjectId

# Update the existing scheduler job with proper timeout
Write-Host "Updating scheduler job with 30-minute timeout..." -ForegroundColor Yellow
gcloud scheduler jobs update http $JobName `
    --location=$Region `
    --attempt-deadline="1800s"

Write-Host "✅ Scheduler timeout updated!" -ForegroundColor Green
Write-Host "The crawler now has 30 minutes (1800 seconds) to complete" -ForegroundColor Cyan

# Show current job configuration
Write-Host "`nCurrent job configuration:" -ForegroundColor Yellow
gcloud scheduler jobs describe $JobName --location=$Region --format="value(httpTarget.uri,httpTarget.httpMethod,schedule,timeZone,attemptDeadline)"

Write-Host "`n=== Scheduler Fix Complete ===" -ForegroundColor Green
Write-Host "Timeout: 1800 seconds (30 minutes)" -ForegroundColor Cyan
Write-Host "Next run: Check Cloud Scheduler console" -ForegroundColor Cyan 