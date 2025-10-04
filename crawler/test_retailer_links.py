#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to verify the retailer_links function works correctly
"""

import sys
import os
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_retailer_links():
    """Test the retailer_links function directly"""
    
    print(f"🧪 Testing retailer_links function...")
    print(f"⏰ Test started at: {datetime.now().isoformat()}")
    
    try:
        from crawler_cloud import retailer_links
        
        print("📡 Attempting to fetch retailer links...")
        links = retailer_links()
        
        if links:
            print(f"✅ SUCCESS: Found {len(links)} retailer links!")
            print("📋 Retailer list:")
            for i, (name, url) in enumerate(links.items(), 1):
                print(f"  {i:2d}. {name}")
                print(f"      URL: {url}")
                if i >= 5:  # Show only first 5 for brevity
                    remaining = len(links) - 5
                    if remaining > 0:
                        print(f"      ... and {remaining} more retailers")
                    break
            return True
        else:
            print("❌ FAILED: No retailer links returned")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        print(f"Stack trace: {traceback.format_exc()}")
        return False
    
    finally:
        print(f"⏰ Test completed at: {datetime.now().isoformat()}")

if __name__ == "__main__":
    success = test_retailer_links()
    sys.exit(0 if success else 1)
