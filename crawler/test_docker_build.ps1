#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Test Docker build locally before pushing to Cloud Build
    
.DESCRIPTION
    This script tests the Docker build process locally to catch issues
    before they cause Cloud Build failures.
    
.EXAMPLE
    .\test_docker_build.ps1
#>

Write-Host "🐳 Testing Docker Build Locally" -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green
Write-Host ""

# Check if Docker is running
try {
    docker version | Out-Null
    Write-Host "✅ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker is not running or not installed" -ForegroundColor Red
    Write-Host "Please start Docker Desktop and try again" -ForegroundColor Yellow
    exit 1
}

# Test build with the main Dockerfile
Write-Host "🔨 Testing main Dockerfile..." -ForegroundColor Cyan
try {
    docker build --no-cache --progress=plain -t price-crawler-test ./crawler
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Main Dockerfile build successful!" -ForegroundColor Green
    } else {
        Write-Host "❌ Main Dockerfile build failed" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ Main Dockerfile build failed with error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Test build with alternative Dockerfile (if it exists)
if (Test-Path "./crawler/Dockerfile.alternative") {
    Write-Host ""
    Write-Host "🔨 Testing alternative Dockerfile..." -ForegroundColor Cyan
    try {
        docker build --no-cache --progress=plain -f ./crawler/Dockerfile.alternative -t price-crawler-test-alt ./crawler
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Alternative Dockerfile build successful!" -ForegroundColor Green
        } else {
            Write-Host "❌ Alternative Dockerfile build failed" -ForegroundColor Red
        }
    } catch {
        Write-Host "❌ Alternative Dockerfile build failed with error: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# Test running the container
Write-Host ""
Write-Host "🚀 Testing container startup..." -ForegroundColor Cyan
try {
    # Start container in background
    docker run -d --name price-crawler-test-container -p 8080:8080 price-crawler-test
    
    # Wait a moment for startup
    Start-Sleep -Seconds 5
    
    # Check if container is running
    $containerStatus = docker ps --filter "name=price-crawler-test-container" --format "table {{.Status}}"
    if ($containerStatus -like "*Up*") {
        Write-Host "✅ Container started successfully!" -ForegroundColor Green
        
        # Test health endpoint
        try {
            $response = Invoke-RestMethod -Uri "http://localhost:8080/" -Method GET -TimeoutSec 10
            Write-Host "✅ Health endpoint responding: $($response.status)" -ForegroundColor Green
        } catch {
            Write-Host "⚠️ Health endpoint test failed: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    } else {
        Write-Host "❌ Container failed to start" -ForegroundColor Red
    }
    
    # Clean up
    docker stop price-crawler-test-container
    docker rm price-crawler-test-container
    
} catch {
    Write-Host "❌ Container test failed: $($_.Exception.Message)" -ForegroundColor Red
}

# Clean up test images
Write-Host ""
Write-Host "🧹 Cleaning up test images..." -ForegroundColor Cyan
docker rmi price-crawler-test -f 2>$null
docker rmi price-crawler-test-alt -f 2>$null

Write-Host ""
Write-Host "🎯 Docker build test completed!" -ForegroundColor Green
Write-Host "If all tests passed, you can safely push to Cloud Build." -ForegroundColor Yellow
