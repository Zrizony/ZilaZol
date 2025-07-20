#!/usr/bin/env pwsh
# Deployment script for Google Cloud Run

param(
    [string]$ServiceName = "price-crawler",
    [string]$Region = "me-west1",
    [string]$ProjectId = "civic-ripsaw-466109-e2"
)

Write-Host "🚀 Deploying Price Crawler to Google Cloud Run..." -ForegroundColor Green

# Set project and region
Write-Host "Setting project and region..." -ForegroundColor Yellow
gcloud config set project $ProjectId
gcloud config set run/region $Region

# Build and deploy to Cloud Run
Write-Host "Building and deploying container..." -ForegroundColor Yellow
gcloud run deploy $ServiceName `
    --source . `
    --platform managed `
    --region $Region `
    --allow-unauthenticated `
    --memory 2Gi `
    --cpu 2 `
    --timeout 3600 `
    --max-instances 1 `
    --set-env-vars "PYTHONUNBUFFERED=1,GCS_BUCKET_NAME=civic-ripsaw-466109-e2-crawler-data,GOOGLE_CLOUD_PROJECT=$ProjectId"

Write-Host "✅ Deployment completed!" -ForegroundColor Green
Write-Host "Service URL will be displayed above." -ForegroundColor Cyan 