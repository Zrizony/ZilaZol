#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to verify the Flask app works locally
"""

import os
import sys
from datetime import datetime

def test_imports():
    """Test if all required imports work"""
    print("Testing imports...")
    
    try:
        import flask
        print("✓ Flask imported successfully")
    except ImportError as e:
        print(f"✗ Flask import failed: {e}")
        return False
    
    try:
        import requests
        print("✓ Requests imported successfully")
    except ImportError as e:
        print(f"✗ Requests import failed: {e}")
        return False
    
    try:
        import playwright
        print("✓ Playwright imported successfully")
    except ImportError as e:
        print(f"✗ Playwright import failed: {e}")
        return False
    
    try:
        import google.cloud.storage
        print("✓ Google Cloud Storage imported successfully")
    except ImportError as e:
        print(f"✗ Google Cloud Storage import failed: {e}")
        return False
    
    return True

def test_app_creation():
    """Test if the Flask app can be created"""
    print("\nTesting Flask app creation...")
    
    try:
        from app import app
        print("✓ Flask app created successfully")
        return True
    except Exception as e:
        print(f"✗ Flask app creation failed: {e}")
        return False

def test_crawler_import():
    """Test if crawler_cloud can be imported"""
    print("\nTesting crawler_cloud import...")
    
    try:
        from crawler_cloud import main
        print("✓ crawler_cloud imported successfully")
        return True
    except Exception as e:
        print(f"✗ crawler_cloud import failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=== Local Test Suite ===")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Python version: {sys.version}")
    print(f"Working directory: {os.getcwd()}")
    
    # Test imports
    imports_ok = test_imports()
    
    # Test app creation
    app_ok = test_app_creation()
    
    # Test crawler import
    crawler_ok = test_crawler_import()
    
    # Summary
    print("\n=== Test Summary ===")
    print(f"Imports: {'✓ PASS' if imports_ok else '✗ FAIL'}")
    print(f"App Creation: {'✓ PASS' if app_ok else '✗ FAIL'}")
    print(f"Crawler Import: {'✓ PASS' if crawler_ok else '✗ FAIL'}")
    
    if all([imports_ok, app_ok, crawler_ok]):
        print("\n🎉 All tests passed! Ready for deployment.")
        return True
    else:
        print("\n❌ Some tests failed. Please fix issues before deploying.")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1) 