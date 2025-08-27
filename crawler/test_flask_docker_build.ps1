#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Test Docker build specifically for Flask installation issues
    
.DESCRIPTION
    This script tests the Docker build process to ensure Flask is properly installed
    and can be imported without errors.
    
.EXAMPLE
    .\test_flask_docker_build.ps1
#>

Write-Host "🐳 Testing Flask Installation in Docker Build" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
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

# Clean up any existing test containers/images
Write-Host "🧹 Cleaning up previous test artifacts..." -ForegroundColor Cyan
docker rmi flask-test-build -f 2>$null
docker rm flask-test-container -f 2>$null

# Test build with verbose output
Write-Host "🔨 Building Docker image with Flask verification..." -ForegroundColor Cyan
try {
    docker build --no-cache --progress=plain -t flask-test-build .
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Docker build completed successfully!" -ForegroundColor Green
        
        # Test Flask import in the container
        Write-Host "🧪 Testing Flask import in container..." -ForegroundColor Cyan
        $flaskTest = docker run --rm flask-test-build python -c "import flask; print('Flask version:', flask.__version__)"
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Flask import test passed: $flaskTest" -ForegroundColor Green
        } else {
            Write-Host "❌ Flask import test failed" -ForegroundColor Red
            exit 1
        }
        
        # Test the Flask app startup
        Write-Host "🚀 Testing Flask app startup..." -ForegroundColor Cyan
        docker run -d --name flask-test-container -p 8081:8080 flask-test-build
        
        Start-Sleep -Seconds 10
        
        try {
            $response = Invoke-RestMethod -Uri "http://localhost:8081/" -Method GET -TimeoutSec 10
            Write-Host "✅ Flask app responding: $($response.status)" -ForegroundColor Green
        } catch {
            Write-Host "❌ Flask app test failed: $($_.Exception.Message)" -ForegroundColor Red
        }
        
        # Clean up
        docker stop flask-test-container
        docker rm flask-test-container
        
    } else {
        Write-Host "❌ Docker build failed" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ Docker build failed with error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Clean up test images
Write-Host "🧹 Cleaning up test images..." -ForegroundColor Cyan
docker rmi flask-test-build -f 2>$null

Write-Host ""
Write-Host "🎯 Flask Docker build test completed!" -ForegroundColor Green
Write-Host "✅ Flask installation is working correctly in Docker." -ForegroundColor Yellow
