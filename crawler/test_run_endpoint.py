#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to verify the /run endpoint works correctly
"""

import requests
import json
from datetime import datetime

def test_run_endpoint():
    """Test the /run endpoint locally or remotely"""
    
    # You can test locally or against the deployed service
    # For local testing, make sure the Flask app is running
    base_url = "https://price-crawler-947639158495.me-west1.run.app"
    # base_url = "http://localhost:8080"  # For local testing
    
    endpoint = f"{base_url}/run"
    
    print(f"🧪 Testing /run endpoint: {endpoint}")
    print(f"⏰ Test started at: {datetime.now().isoformat()}")
    
    try:
        # Test the endpoint
        response = requests.post(endpoint, timeout=300)  # 5 minute timeout
        
        print(f"📊 Response Status: {response.status_code}")
        print(f"📊 Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("✅ SUCCESS: /run endpoint completed successfully!")
            try:
                data = response.json()
                print(f"📋 Response Data:")
                print(json.dumps(data, indent=2, ensure_ascii=False))
            except:
                print(f"📋 Response Text: {response.text}")
        else:
            print(f"❌ FAILED: /run endpoint returned status {response.status_code}")
            print(f"📋 Response Text: {response.text}")
            
    except requests.exceptions.Timeout:
        print("⏰ TIMEOUT: Request timed out (this might be expected for long-running crawls)")
    except requests.exceptions.ConnectionError:
        print("🔌 CONNECTION ERROR: Could not connect to the service")
        print("   Make sure the service is running and accessible")
    except Exception as e:
        print(f"❌ ERROR: {e}")
    
    print(f"⏰ Test completed at: {datetime.now().isoformat()}")

if __name__ == "__main__":
    test_run_endpoint()
