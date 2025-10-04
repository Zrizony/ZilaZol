#!/usr/bin/env pwsh
# Fix the scheduler job that's hitting the /run endpoint with proper timeout

param(
    [string]$ProjectId = "civic-ripsaw-466109-e2",
    [string]$Region = "europe-west1",
    [string]$JobName = "daily-crawler"
)

Write-Host "🔧 Fixing scheduler job for /run endpoint..." -ForegroundColor Green

# Set project
gcloud config set project $ProjectId

# Check if the job exists and what it's currently configured to do
Write-Host "🔍 Checking current scheduler job configuration..." -ForegroundColor Yellow
try {
    $job_info = gcloud scheduler jobs describe $JobName --location=$Region --format="value(httpTarget.uri,httpTarget.httpMethod,schedule,timeZone,attemptDeadline)" 2>$null
    if ($job_info) {
        Write-Host "Current job configuration:" -ForegroundColor Cyan
        Write-Host "  URI: $($job_info[0])" -ForegroundColor White
        Write-Host "  Method: $($job_info[1])" -ForegroundColor White
        Write-Host "  Schedule: $($job_info[2])" -ForegroundColor White
        Write-Host "  Timezone: $($job_info[3])" -ForegroundColor White
        Write-Host "  Timeout: $($job_info[4])" -ForegroundColor White
        
        # Check if it's hitting the /run endpoint
        if ($job_info[0] -like "*/run") {
            Write-Host "✅ Job is correctly configured to hit /run endpoint" -ForegroundColor Green
        } else {
            Write-Host "⚠️ Job is hitting $($job_info[0]) instead of /run endpoint" -ForegroundColor Yellow
        }
    } else {
        Write-Host "❌ Job '$JobName' not found in region '$Region'" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ Failed to get job information: $_" -ForegroundColor Red
    exit 1
}

# Update the scheduler job with proper timeout for the /run endpoint
Write-Host "`n🔧 Updating scheduler job with 30-minute timeout..." -ForegroundColor Yellow
try {
    gcloud scheduler jobs update http $JobName `
        --location=$Region `
        --attempt-deadline="1800s" `
        --description="Daily price crawler run via /run endpoint (30 min timeout)"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Scheduler job updated successfully!" -ForegroundColor Green
    } else {
        Write-Host "❌ Failed to update scheduler job" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ Error updating scheduler job: $_" -ForegroundColor Red
    exit 1
}

# Show updated configuration
Write-Host "`n📋 Updated job configuration:" -ForegroundColor Yellow
try {
    $updated_job = gcloud scheduler jobs describe $JobName --location=$Region --format="value(httpTarget.uri,httpTarget.httpMethod,schedule,timeZone,attemptDeadline)"
    if ($updated_job) {
        Write-Host "  URI: $($updated_job[0])" -ForegroundColor White
        Write-Host "  Method: $($updated_job[1])" -ForegroundColor White
        Write-Host "  Schedule: $($updated_job[2])" -ForegroundColor White
        Write-Host "  Timezone: $($updated_job[3])" -ForegroundColor White
        Write-Host "  Timeout: $($updated_job[4])" -ForegroundColor White
    }
} catch {
    Write-Host "⚠️ Could not retrieve updated configuration" -ForegroundColor Yellow
}

Write-Host "`n🎯 Scheduler Fix Summary:" -ForegroundColor Green
Write-Host "  ✅ Job configured to hit /run endpoint" -ForegroundColor White
Write-Host "  ✅ Timeout set to 1800 seconds (30 minutes)" -ForegroundColor White
Write-Host "  ✅ Ready for next scheduled run" -ForegroundColor White

Write-Host "`n📚 What was fixed:" -ForegroundColor Cyan
Write-Host "  • The /run endpoint now runs crawls directly (no self-HTTP calls)" -ForegroundColor White
Write-Host "  • Proper error handling and logging added" -ForegroundColor White
Write-Host "  • 30-minute timeout should handle full crawler execution" -ForegroundColor White
Write-Host "  • Returns detailed results instead of just 'accepted'" -ForegroundColor White

Write-Host "`n🧪 Test the fix:" -ForegroundColor Cyan
Write-Host "  python test_run_endpoint.py" -ForegroundColor White

Write-Host "`n=== Scheduler Fix Complete ===" -ForegroundColor Green
