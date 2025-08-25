#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Test script for Google Cloud Storage optimization and cleanup features
    
.DESCRIPTION
    This script tests the enhanced storage operations including:
    - Storage analytics
    - Screenshot cleanup
    - Batch operations
    - Storage health checks
    
.PARAMETER BaseUrl
    Base URL of the deployed service (default: localhost:8080)
    
.PARAMETER TestCleanup
    Whether to test the cleanup functionality (default: true)
    
.EXAMPLE
    .\test_storage_optimization.ps1 -BaseUrl "https://price-crawler-947639158495.me-west1.run.app"
    
.EXAMPLE
    .\test_storage_optimization.ps1 -TestCleanup:$false
#>

param(
    [string]$BaseUrl = "http://localhost:8080",
    [bool]$TestCleanup = $true
)

Write-Host "🚀 Testing Google Cloud Storage Optimization Features" -ForegroundColor Green
Write-Host "==================================================" -ForegroundColor Green
Write-Host "Base URL: $BaseUrl" -ForegroundColor Yellow
Write-Host "Test Cleanup: $TestCleanup" -ForegroundColor Yellow
Write-Host ""

# Test function with error handling
function Test-Endpoint {
    param(
        [string]$Endpoint,
        [string]$Method = "GET",
        [string]$Description
    )
    
    try {
        Write-Host "🔍 Testing: $Description" -ForegroundColor Cyan
        Write-Host "   Endpoint: $Method $Endpoint" -ForegroundColor Gray
        
        $uri = "$BaseUrl$Endpoint"
        $headers = @{
            "Content-Type" = "application/json"
        }
        
        if ($Method -eq "POST") {
            $response = Invoke-RestMethod -Uri $uri -Method $Method -Headers $headers
        } else {
            $response = Invoke-RestMethod -Uri $uri -Method $Method -Headers $headers
        }
        
        Write-Host "   ✅ Status: Success" -ForegroundColor Green
        Write-Host "   📊 Response: $($response.status)" -ForegroundColor Gray
        
        if ($response.analytics) {
            Write-Host "   📈 Analytics Available" -ForegroundColor Green
        }
        
        if ($response.cleanup_results) {
            Write-Host "   🧹 Cleanup Results:" -ForegroundColor Green
            Write-Host "      Screenshots deleted: $($response.cleanup_results.screenshots_deleted)" -ForegroundColor Gray
            Write-Host "      Space saved: $($response.cleanup_results.space_saved_bytes) bytes" -ForegroundColor Gray
        }
        
        return $true
        
    } catch {
        Write-Host "   ❌ Error: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Test all endpoints
Write-Host "📋 Running Storage Optimization Tests..." -ForegroundColor Blue
Write-Host ""

$tests = @(
    @{
        Endpoint = "/"
        Method = "GET"
        Description = "Main Health Check"
    },
    @{
        Endpoint = "/storage/status"
        Method = "GET"
        Description = "Storage Status Check"
    },
    @{
        Endpoint = "/storage/analytics"
        Method = "GET"
        Description = "Storage Analytics"
    }
)

if ($TestCleanup) {
    $tests += @{
        Endpoint = "/storage/cleanup"
        Method = "POST"
        Description = "Storage Cleanup Trigger"
    }
}

$testResults = @()
foreach ($test in $tests) {
    $result = Test-Endpoint -Endpoint $test.Endpoint -Method $test.Method -Description $test.Description
    $testResults += [PSCustomObject]@{
        Test = $test.Description
        Status = if ($result) { "PASS" } else { "FAIL" }
        Endpoint = "$($test.Method) $($test.Endpoint)"
    }
    Write-Host ""
}

# Summary
Write-Host "📊 Test Results Summary" -ForegroundColor Green
Write-Host "======================" -ForegroundColor Green
$testResults | Format-Table -AutoSize

$passed = ($testResults | Where-Object { $_.Status -eq "PASS" }).Count
$total = $testResults.Count

Write-Host ""
Write-Host "🎯 Overall Results: $passed/$total tests passed" -ForegroundColor $(if ($passed -eq $total) { "Green" } else { "Yellow" })

if ($passed -eq $total) {
    Write-Host "✅ All storage optimization features are working correctly!" -ForegroundColor Green
} else {
    Write-Host "⚠️ Some tests failed. Check the logs above for details." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "🔧 Available Storage Management Endpoints:" -ForegroundColor Blue
Write-Host "   GET  /storage/status     - Check storage health and bucket info" -ForegroundColor Gray
Write-Host "   GET  /storage/analytics  - Get detailed storage usage statistics" -ForegroundColor Gray
Write-Host "   POST /storage/cleanup    - Manually trigger screenshot cleanup" -ForegroundColor Gray

Write-Host ""
Write-Host "📚 Usage Examples:" -ForegroundColor Blue
Write-Host "   # Check storage status" -ForegroundColor Gray
Write-Host "   curl $BaseUrl/storage/status" -ForegroundColor White
Write-Host ""
Write-Host "   # Get storage analytics" -ForegroundColor Gray
Write-Host "   curl $BaseUrl/storage/analytics" -ForegroundColor White
Write-Host ""
Write-Host "   # Trigger cleanup" -ForegroundColor Gray
Write-Host "   curl -X POST $BaseUrl/storage/cleanup" -ForegroundColor White
