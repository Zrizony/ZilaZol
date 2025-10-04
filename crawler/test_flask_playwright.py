#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test Flask + Playwright integration to isolate the async issue
"""

import sys
import os
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_flask_playwright():
    """Test Flask + Playwright integration"""
    
    print(f"🧪 Testing Flask + Playwright integration...")
    print(f"⏰ Test started at: {datetime.now().isoformat()}")
    
    try:
        from flask import Flask
        from crawler_cloud import retailer_links
        
        app = Flask(__name__)
        
        @app.route('/test-playwright')
        def test_playwright():
            try:
                print("🎭 Testing Playwright within Flask context...")
                links = retailer_links()
                if links:
                    return f"Success: Found {len(links)} retailers"
                else:
                    return "Warning: No retailers found"
            except Exception as e:
                return f"Error: {e}"
        
        # Test the endpoint
        with app.test_client() as client:
            response = client.get('/test-playwright')
            print(f"📊 Response: {response.data.decode()}")
            
            if "Success:" in response.data.decode():
                print("✅ SUCCESS: Flask + Playwright integration works!")
                return True
            else:
                print("❌ FAILED: Flask + Playwright integration has issues")
                return False
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        print(f"Stack trace: {traceback.format_exc()}")
        return False
    
    finally:
        print(f"⏰ Test completed at: {datetime.now().isoformat()}")

if __name__ == "__main__":
    success = test_flask_playwright()
    sys.exit(0 if success else 1)
