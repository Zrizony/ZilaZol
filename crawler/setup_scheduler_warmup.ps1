# Setup Cloud Scheduler for service warmup (keepalive)
# This creates a scheduler job that pings the service every 5 minutes to prevent cold starts

$PROJECT_ID = "civic-ripsaw-466109-e2"
$REGION = "europe-west1"
$SERVICE_URL = "https://price-crawler-947639158495.me-west1.run.app"

Write-Host "Setting up Cloud Scheduler for service warmup..." -ForegroundColor Cyan

# Create warmup job (every 5 minutes to keep instance warm)
Write-Host "`nCreating warmup job (pings service every 5 minutes)..." -ForegroundColor Yellow
gcloud scheduler jobs create http price-crawler-warmup `
    --location=$REGION `
    --schedule="*/5 * * * *" `
    --uri="$SERVICE_URL/ping" `
    --http-method=GET `
    --description="Keep price crawler service warm (prevent cold starts)" `
    --project=$PROJECT_ID

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Warmup job created successfully!" -ForegroundColor Green
} else {
    Write-Host "⚠️ Warmup job may already exist. Updating..." -ForegroundColor Yellow
    
    gcloud scheduler jobs update http price-crawler-warmup `
        --location=$REGION `
        --schedule="*/5 * * * *" `
        --uri="$SERVICE_URL/ping" `
        --http-method=GET `
        --description="Keep price crawler service warm (prevent cold starts)" `
        --project=$PROJECT_ID
}

Write-Host "`n📋 Scheduler Jobs Summary:" -ForegroundColor Cyan
Write-Host "  • price-crawler-warmup: Every 5 minutes → /ping (keepalive)" -ForegroundColor White
Write-Host "  • daily-crawler: Daily at 7 AM → /crawl (full crawl)" -ForegroundColor White

Write-Host "`n✅ Setup complete!" -ForegroundColor Green
Write-Host "`n📚 Usage:" -ForegroundColor Cyan
Write-Host "  • Warmup job keeps service responsive (no cold starts)" -ForegroundColor White
Write-Host "  • Daily crawler runs full price scraping" -ForegroundColor White
Write-Host "  • Manual trigger: curl $SERVICE_URL/ping" -ForegroundColor White

