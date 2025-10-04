#!/usr/bin/env pwsh
# Complete Cloud Scheduler setup for price crawler
# This script sets up both warmup and crawler jobs

param(
    [string]$ProjectId = "civic-ripsaw-466109-e2",
    [string]$Region = "europe-west1",
    [string]$ServiceName = "price-crawler",
    [string]$ServiceUrl = "https://price-crawler-947639158495.me-west1.run.app"
)

Write-Host "🚀 Setting up complete Cloud Scheduler configuration..." -ForegroundColor Green
Write-Host "Project: $ProjectId" -ForegroundColor Cyan
Write-Host "Region: $Region" -ForegroundColor Cyan
Write-Host "Service: $ServiceName" -ForegroundColor Cyan
Write-Host "URL: $ServiceUrl" -ForegroundColor Cyan

# Set project
gcloud config set project $ProjectId

# Check if service exists
Write-Host "`n🔍 Checking Cloud Run service..." -ForegroundColor Yellow
$service_info = gcloud run services describe $ServiceName --region=$Region --format="value(status.url)" 2>$null

if (-not $service_info) {
    Write-Host "❌ Cloud Run service '$ServiceName' not found in region '$Region'" -ForegroundColor Red
    Write-Host "Please deploy the service first using: .\deploy.ps1" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ Service found: $service_info" -ForegroundColor Green

# Function to create or update scheduler job
function Set-SchedulerJob {
    param(
        [string]$JobName,
        [string]$Schedule,
        [string]$Uri,
        [string]$Method = "GET",
        [string]$Description,
        [string]$Timeout = "60s"
    )
    
    Write-Host "`n🔧 Setting up job: $JobName" -ForegroundColor Yellow
    
    # Check if job exists
    $existing_job = gcloud scheduler jobs describe $JobName --location=$Region 2>$null
    
    if ($existing_job) {
        Write-Host "   Updating existing job..." -ForegroundColor Cyan
        $command = "update"
    } else {
        Write-Host "   Creating new job..." -ForegroundColor Cyan
        $command = "create"
    }
    
    # Build the command
    $gcloud_cmd = @(
        "gcloud", "scheduler", "jobs", $command, "http", $JobName,
        "--location=$Region",
        "--schedule=`"$Schedule`"",
        "--uri=`"$Uri`"",
        "--http-method=$Method",
        "--description=`"$Description`"",
        "--project=$ProjectId"
    )
    
    if ($Timeout -ne "60s") {
        $gcloud_cmd += "--attempt-deadline=`"$Timeout`""
    }
    
    # Add timezone for crawler job
    if ($JobName -eq "daily-crawler") {
        $gcloud_cmd += "--time-zone=`"Asia/Jerusalem`""
    }
    
    # Execute command
    try {
        & $gcloud_cmd[0] $gcloud_cmd[1..($gcloud_cmd.Length-1)]
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ Job $JobName configured successfully" -ForegroundColor Green
            return $true
        } else {
            Write-Host "   ❌ Failed to configure job $JobName" -ForegroundColor Red
            return $false
        }
    } catch {
        Write-Host "   ❌ Error configuring job $JobName: $_" -ForegroundColor Red
        return $false
    }
}

# Setup warmup job (every 5 minutes)
$warmup_success = Set-SchedulerJob `
    -JobName "price-crawler-warmup" `
    -Schedule "*/5 * * * *" `
    -Uri "$ServiceUrl/ping" `
    -Method "GET" `
    -Description "Keep price crawler service warm (prevent cold starts)" `
    -Timeout "60s"

# Setup daily crawler job (7 AM Jerusalem time)
$crawler_success = Set-SchedulerJob `
    -JobName "daily-crawler" `
    -Schedule "0 7 * * *" `
    -Uri "$ServiceUrl/run" `
    -Method "POST" `
    -Description "Daily price crawler run at 7 AM Jerusalem time" `
    -Timeout "1800s"

# Show current configuration
Write-Host "`n📋 Current Scheduler Configuration:" -ForegroundColor Yellow
Write-Host "-" * 50

try {
    gcloud scheduler jobs list --location=$Region --format="table(name,schedule,httpTarget.uri,state)"
} catch {
    Write-Host "Could not list jobs: $_" -ForegroundColor Red
}

# Summary
Write-Host "`n🎯 Setup Summary:" -ForegroundColor Green
Write-Host "-" * 30

if ($warmup_success) {
    Write-Host "✅ Warmup job: Every 5 minutes → /ping" -ForegroundColor Green
} else {
    Write-Host "❌ Warmup job: Failed to configure" -ForegroundColor Red
}

if ($crawler_success) {
    Write-Host "✅ Crawler job: Daily at 7 AM → /run (30 min timeout)" -ForegroundColor Green
} else {
    Write-Host "❌ Crawler job: Failed to configure" -ForegroundColor Red
}

if ($warmup_success -and $crawler_success) {
    Write-Host "`n🎉 Scheduler setup completed successfully!" -ForegroundColor Green
    Write-Host "`n📚 What happens now:" -ForegroundColor Cyan
    Write-Host "• Warmup job keeps service responsive (no cold starts)" -ForegroundColor White
    Write-Host "• Daily crawler runs full price scraping at 7 AM" -ForegroundColor White
    Write-Host "• Check logs in Google Cloud Console for monitoring" -ForegroundColor White
    
    Write-Host "`n🔍 Monitoring commands:" -ForegroundColor Cyan
    Write-Host "• View jobs: gcloud scheduler jobs list --location=$Region" -ForegroundColor White
    Write-Host "• Check logs: gcloud logging read 'resource.type=cloud_scheduler_job' --limit 50" -ForegroundColor White
    Write-Host "• Pause crawler: gcloud scheduler jobs pause daily-crawler --location=$Region" -ForegroundColor White
    Write-Host "• Resume crawler: gcloud scheduler jobs resume daily-crawler --location=$Region" -ForegroundColor White
} else {
    Write-Host "`n⚠️ Scheduler setup had issues. Please check the errors above." -ForegroundColor Yellow
    exit 1
}

Write-Host "`n=== Setup Complete ===" -ForegroundColor Green
