#!/usr/bin/env python3
"""
Victory Online Image Scraper

This script reverse-engineers the Victory Online API to download product images
in bulk by requesting 5000 items at once instead of paginating.

Images are uploaded to Supabase Storage instead of saving locally.

Usage:
    python scripts/victory_scraper.py

Requirements:
    - requests library (pip install requests)
    - supabase library (pip install supabase)
    
Environment Variables:
    - SUPABASE_URL: Your Supabase project URL
    - SUPABASE_KEY: Your Supabase service role key (or anon key with storage permissions)
    - SUPABASE_STORAGE_BUCKET: Storage bucket name (default: "product-images")
"""

import requests
import json
import os
import time
import sys
from pathlib import Path

# Fix Windows console encoding issues
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Add parent directory to path for imports if needed
sys.path.insert(0, str(Path(__file__).parent.parent))

# Try to import Supabase client
try:
    from supabase import create_client, Client
except ImportError:
    print("ERROR: supabase library is not installed.")
    print("Please install it with: pip install supabase")
    sys.exit(1)

# 1. SETUP: Supabase Storage Configuration
def get_env_var(var_name: str, default: str = None) -> str:
    """Get environment variable from command line, environment, or .env file"""
    # First check environment variables
    value = os.getenv(var_name)
    if value:
        return value
    
    # Try loading from .env file
    env_files = [
        Path(__file__).parent.parent.parent / "NextJS" / ".env",
        Path(__file__).parent.parent / ".env",
        Path(__file__).parent.parent.parent / ".env",
    ]
    
    for env_file in env_files:
        if env_file.exists():
            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith(f'{var_name}='):
                            value = line.split('=', 1)[1].strip()
                            # Remove quotes if present
                            if value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            elif value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                            return value
            except Exception:
                pass
    
    return default

SUPABASE_URL = get_env_var("SUPABASE_URL")
SUPABASE_KEY = get_env_var("SUPABASE_KEY")
SUPABASE_STORAGE_BUCKET = get_env_var("SUPABASE_STORAGE_BUCKET", "product-images")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set.")
    print("\nYou can set them in one of these ways:")
    print("1. Environment variables:")
    print("   export SUPABASE_URL='https://your-project.supabase.co'")
    print("   export SUPABASE_KEY='your-service-role-key'")
    print("\n2. Or add them to a .env file in one of these locations:")
    print("   - NextJS/.env")
    print("   - Supers/.env")
    print("   - .env (root directory)")
    print("\nExample .env file content:")
    print("   SUPABASE_URL=https://your-project.supabase.co")
    print("   SUPABASE_KEY=your-service-role-key")
    print("   SUPABASE_STORAGE_BUCKET=product-images")
    sys.exit(1)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 2. THE HACKED REQUEST
# We changed 'size' to 5000 to get thousands of items in one request.
# URL without category path to get ALL products from the store
url = "https://www.victoryonline.co.il/v2/retailers/1470/branches/2440/products"

params = {
    "appId": "4",
    # This complex filter basically says "Show me items that are IN STOCK"
    "filters": '{"bool":{"should":[{"bool":{"must_not":{"exists":{"field":"branch.outOfStockShowUntilDate"}}}},{"bool":{"must":[{"range":{"branch.outOfStockShowUntilDate":{"gt":"now"}}},{"term":{"branch.isOutOfStock":true}}]}},{"bool":{"must":[{"term":{"branch.isOutOfStock":false}}]}}]}}',
    "from": "0",
    "languageId": "1",
    "minScore": "0",
    "size": "1000"  # Maximum allowed seems to be around 1000, so we'll paginate
}

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=1, i",
    "referer": "https://www.victoryonline.co.il/",
    "sec-ch-ua": '"Brave";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-gpc": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "pathname": "/"
}

cookies = {
    "retailerId": "1470"
}

print("🚀 Fetching products from Victory Online...")
all_products = []
page_size = 100  # API seems to have a lower limit without category
from_offset = 0

# Paginate through all products
while True:
    params["from"] = str(from_offset)
    params["size"] = str(page_size)
    
    try:
        print(f"   📥 Fetching products {from_offset} to {from_offset + page_size}...")
        response = requests.get(url, params=params, headers=headers, cookies=cookies, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ API Request Failed: {e}")
        if 'response' in locals():
            print(response.text[:500])
        break
    
    data = response.json()
    
    # Response structure uses "products" instead of "data" for the all-products endpoint
    products = data.get("products", data.get("data", []))
    
    # Debug info on first request
    if from_offset == 0:
        total = data.get("total", len(products))
        print(f"   📊 Total products available: {total}")
    
    if not products:
        break
    
    all_products.extend(products)
    print(f"   ✅ Received {len(products)} products (Total: {len(all_products)})")
    
    # If we got fewer than page_size, we've reached the end
    if len(products) < page_size:
        break
    
    from_offset += page_size
    time.sleep(0.5)  # Be polite with rate limiting

print(f"\n✅ Success! Received {len(all_products)} total products.")
products = all_products

# 3. DOWNLOAD LOOP
count = 0
skipped = 0
failed = 0

for p in products:
    name = p.get("name", "Unknown")
    # Use Barcode as filename (Crucial for linking data later)
    barcode = p.get("mainBarcode", "") or str(p.get("id"))
    
    if not barcode:
        skipped += 1
        continue
    
    # Extract Image URL (Victory hides it in 'medias')
    image_url = None
    if p.get("medias") and len(p["medias"]) > 0:
        image_url = p["medias"][0].get("url")
        
        # Fix relative URLs
        if image_url and not image_url.startswith("http"):
            image_url = "https://www.victoryonline.co.il" + image_url
            
    if not image_url:
        skipped += 1
        continue

    # Upload Image to Supabase Storage
    storage_path = f"victory/{barcode}.jpg"
    
    # Check if file already exists in Supabase Storage
    try:
        existing_files = supabase.storage.from_(SUPABASE_STORAGE_BUCKET).list("victory")
        if existing_files and any(f.get("name") == f"{barcode}.jpg" for f in existing_files):
            skipped += 1
            continue
    except Exception:
        # If check fails, continue anyway (might be permission issue or file doesn't exist)
        pass

    try:
        # Download image from Victory
        img_response = requests.get(image_url, timeout=10)
        img_response.raise_for_status()
        img_data = img_response.content
        
        # Upload to Supabase Storage (upsert will overwrite if exists)
        supabase.storage.from_(SUPABASE_STORAGE_BUCKET).upload(
            path=storage_path,
            file=img_data,
            file_options={"content-type": "image/jpeg", "upsert": True}
        )
        
        print(f"📸 Uploaded: {name} -> {storage_path}")
        count += 1
        
        # Rate limit slightly to be polite
        if count % 100 == 0:
            print(f"   ⏸️  Pausing... ({count} uploaded so far)")
            time.sleep(1)
            
    except Exception as e:
        print(f"⚠️  Failed to upload {name} ({barcode}): {e}")
        failed += 1

print(f"\n🎉 Done!")
print(f"   ✅ Uploaded: {count} new images to Supabase Storage")
print(f"   ⏭️  Skipped: {skipped} (already exist or no image)")
print(f"   ❌ Failed: {failed}")
print(f"   📦 Storage bucket: {SUPABASE_STORAGE_BUCKET}")
print(f"   📁 Storage path: victory/")

