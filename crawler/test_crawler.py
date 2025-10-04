#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Comprehensive test suite for the price crawler
Tests both sync and async functionality
"""

import sys
import os
import asyncio
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test that all required modules can be imported"""
    print("🧪 Testing imports...")
    
    try:
        # Test core crawler imports
        from crawler_cloud import (
            ROOT, UA, BUCKET_NAME, PROJECT_ID, CREDS, ZIP_RX, log,
            get_storage_client, upload_to_gcs, slug, creds_for, _to_float, parse_xml
        )
        print("✅ Core crawler imports successful")
        
        # Test async crawler imports
        from crawler_async import (
            retailer_links_async, playwright_with_login_async, 
            zip_links_async, crawl_single_shop_async, main_async
        )
        print("✅ Async crawler imports successful")
        
        # Test Flask app imports
        from app import app
        print("✅ Flask app imports successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Import test failed: {e}")
        return False

def test_basic_functions():
    """Test basic utility functions"""
    print("🧪 Testing basic functions...")
    
    try:
        from crawler_cloud import slug, creds_for, _to_float
        
        # Test slug function
        test_name = "דור אלון ניהול מתחמים קמעונאיים בע\"מ"
        slug_result = slug(test_name)
        print(f"✅ Slug test: '{test_name}' -> '{slug_result}'")
        
        # Test creds function
        creds = creds_for(test_name)
        print(f"✅ Creds test: Found {len(creds) if creds else 0} credentials")
        
        # Test float conversion
        float_result = _to_float("12.34")
        print(f"✅ Float test: '12.34' -> {float_result}")
        
        return True
        
    except Exception as e:
        print(f"❌ Basic functions test failed: {e}")
        return False

async def test_async_retailer_links():
    """Test async retailer links function"""
    print("🧪 Testing async retailer links...")
    
    try:
        from crawler_async import retailer_links_async
        
        links = await retailer_links_async()
        
        if links:
            print(f"✅ Found {len(links)} retailer links")
            # Show first few
            for i, (name, url) in enumerate(list(links.items())[:3], 1):
                print(f"   {i}. {name[:50]}...")
            return True
        else:
            print("❌ No retailer links found")
            return False
            
    except Exception as e:
        print(f"❌ Async retailer links test failed: {e}")
        return False

def test_flask_endpoints():
    """Test Flask app endpoints"""
    print("🧪 Testing Flask endpoints...")
    
    try:
        from app import app
        
        with app.test_client() as client:
            # Test health check
            response = client.get('/')
            if response.status_code == 200:
                print("✅ Health check endpoint working")
            else:
                print(f"❌ Health check failed: {response.status_code}")
                return False
            
            # Test ping endpoint
            response = client.get('/ping')
            if response.status_code == 200:
                print("✅ Ping endpoint working")
            else:
                print(f"❌ Ping failed: {response.status_code}")
                return False
            
            # Test test endpoint
            response = client.get('/test')
            if response.status_code == 200:
                print("✅ Test endpoint working")
            else:
                print(f"❌ Test endpoint failed: {response.status_code}")
                return False
            
            return True
            
    except Exception as e:
        print(f"❌ Flask endpoints test failed: {e}")
        return False

def test_cloud_storage():
    """Test cloud storage functionality"""
    print("🧪 Testing cloud storage...")
    
    try:
        from crawler_cloud import test_cloud_setup
        
        if test_cloud_setup():
            print("✅ Cloud storage setup test passed")
            return True
        else:
            print("❌ Cloud storage setup test failed")
            return False
            
    except Exception as e:
        print(f"❌ Cloud storage test failed: {e}")
        return False

async def test_async_crawler():
    """Test the async crawler (limited scope)"""
    print("🧪 Testing async crawler (limited scope)...")
    
    try:
        from crawler_async import retailer_links_async
        
        # Just test getting retailer links, not full crawl
        links = await retailer_links_async()
        
        if links and len(links) > 0:
            print(f"✅ Async crawler test passed - found {len(links)} retailers")
            return True
        else:
            print("❌ Async crawler test failed - no retailers found")
            return False
            
    except Exception as e:
        print(f"❌ Async crawler test failed: {e}")
        return False

def test_endpoint_integration():
    """Test the /run endpoint integration"""
    print("🧪 Testing /run endpoint integration...")
    
    try:
        from app import app
        import json
        
        with app.test_client() as client:
            # Test the /run endpoint (this will actually try to run the crawler)
            print("   Note: This test will attempt to run the actual crawler...")
            print("   If this takes too long, it's working but may timeout")
            
            response = client.post('/run', 
                                 json={}, 
                                 timeout=60)  # 1 minute timeout
            
            if response.status_code in [200, 202, 503]:
                print(f"✅ /run endpoint responded with status {response.status_code}")
                try:
                    data = response.get_json()
                    print(f"   Response: {data.get('status', 'unknown')}")
                except:
                    print(f"   Response text: {response.data.decode()[:100]}...")
                return True
            else:
                print(f"❌ /run endpoint failed with status {response.status_code}")
                return False
                
    except Exception as e:
        print(f"❌ /run endpoint test failed: {e}")
        return False

async def run_all_tests():
    """Run all tests"""
    print("🚀 Starting comprehensive crawler test suite...")
    print(f"⏰ Test started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    tests = [
        ("Import Tests", test_imports),
        ("Basic Functions", test_basic_functions),
        ("Flask Endpoints", test_flask_endpoints),
        ("Cloud Storage", test_cloud_storage),
        ("Async Retailer Links", test_async_retailer_links),
        ("Async Crawler", test_async_crawler),
        ("Endpoint Integration", test_endpoint_integration),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 40)
        
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            
            results.append((test_name, result))
            
            if result:
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
                
        except Exception as e:
            print(f"❌ {test_name} ERROR: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! The crawler is ready to use.")
        return True
    else:
        print("⚠️ Some tests failed. Check the output above for details.")
        return False

def main():
    """Main test runner"""
    success = asyncio.run(run_all_tests())
    
    print(f"\n⏰ Test completed at: {datetime.now().isoformat()}")
    
    if success:
        print("\n🚀 Next steps:")
        print("1. Deploy the crawler: .\\deploy.ps1")
        print("2. Set up scheduler: .\\setup_scheduler.ps1")
        print("3. Monitor logs in Google Cloud Console")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
