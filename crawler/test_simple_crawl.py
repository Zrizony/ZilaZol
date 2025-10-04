#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Simple test to verify the crawler works without Flask/async issues
"""

import sys
import os
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_simple_crawl():
    """Test the crawler main function directly"""
    
    print(f"🧪 Testing simple crawler execution...")
    print(f"⏰ Test started at: {datetime.now().isoformat()}")
    
    try:
        from crawler_cloud import main
        
        print("🚀 Running crawler main function...")
        main()  # This should run without async issues
        
        print("✅ SUCCESS: Crawler completed without errors!")
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        print(f"Stack trace: {traceback.format_exc()}")
        return False
    
    finally:
        print(f"⏰ Test completed at: {datetime.now().isoformat()}")

if __name__ == "__main__":
    success = test_simple_crawl()
    sys.exit(0 if success else 1)
