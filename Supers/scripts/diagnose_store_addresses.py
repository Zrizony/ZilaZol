#!/usr/bin/env python3
"""
Diagnostic script to check why store addresses are NULL in the database.
This script helps identify:
1. Whether store files are being downloaded
2. What fields exist in XML files
3. Whether parsers are extracting address data correctly
"""
import sys
import os
import asyncio
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from crawler.db import get_pool, fetch_stores_with_retailer, close_pool
from crawler.parsers import parse_stores_xml, parse_prices_xml
from crawler import logger

async def check_database_stores():
    """Check stores in database and their address status."""
    print("\n" + "="*60)
    print("DATABASE STORE ANALYSIS")
    print("="*60 + "\n")
    
    pool = await get_pool()
    if not pool:
        print("ERROR: Could not connect to database. Check DATABASE_URL environment variable.")
        return
    
    stores = await fetch_stores_with_retailer()
    
    if not stores:
        print("No stores found in database.")
        return
    
    total = len(stores)
    with_address = sum(1 for s in stores if s.get("address"))
    with_city = sum(1 for s in stores if s.get("city"))
    with_both = sum(1 for s in stores if s.get("address") and s.get("city"))
    with_neither = sum(1 for s in stores if not s.get("address") and not s.get("city"))
    
    print(f"Total stores: {total}")
    print(f"  - With address: {with_address} ({with_address*100/total:.1f}%)")
    print(f"  - With city: {with_city} ({with_city*100/total:.1f}%)")
    print(f"  - With both: {with_both} ({with_both*100/total:.1f}%)")
    print(f"  - With neither: {with_neither} ({with_neither*100/total:.1f}%)")
    
    # Group by retailer
    print("\nBy Retailer:")
    by_retailer = {}
    for store in stores:
        slug = store.get("retailerSlug", "unknown")
        if slug not in by_retailer:
            by_retailer[slug] = {"total": 0, "with_address": 0, "with_city": 0}
        by_retailer[slug]["total"] += 1
        if store.get("address"):
            by_retailer[slug]["with_address"] += 1
        if store.get("city"):
            by_retailer[slug]["with_city"] += 1
    
    for slug, stats in sorted(by_retailer.items()):
        pct_addr = stats["with_address"] * 100 / stats["total"] if stats["total"] > 0 else 0
        pct_city = stats["with_city"] * 100 / stats["total"] if stats["total"] > 0 else 0
        print(f"  {slug}: {stats['total']} stores, "
              f"{stats['with_address']} addresses ({pct_addr:.1f}%), "
              f"{stats['with_city']} cities ({pct_city:.1f}%)")
    
    # Show sample stores without addresses
    print("\nSample stores WITHOUT address/city:")
    count = 0
    for store in stores:
        if not store.get("address") and not store.get("city") and count < 5:
            print(f"  - {store.get('retailerSlug')} store {store.get('externalId')} "
                  f"(id={store.get('id')}, name={store.get('name')})")
            count += 1
    
    await close_pool()

def test_xml_parsing(xml_path: str):
    """Test parsing an XML file to see what data is extracted."""
    print("\n" + "="*60)
    print(f"XML PARSING TEST: {xml_path}")
    print("="*60 + "\n")
    
    if not os.path.exists(xml_path):
        print(f"ERROR: File not found: {xml_path}")
        return
    
    with open(xml_path, 'rb') as f:
        xml_bytes = f.read()
    
    # Check if it's a store file or price file
    filename = os.path.basename(xml_path).lower()
    is_store_file = "store" in filename and "price" not in filename
    
    if is_store_file:
        print("Detected as STORE file")
        rows = parse_stores_xml(xml_bytes)
        print(f"\nParsed {len(rows)} stores:")
        for i, row in enumerate(rows[:5], 1):
            print(f"\n  Store {i}:")
            print(f"    external_id: {row.get('external_id')}")
            print(f"    name: {row.get('name')}")
            print(f"    city: {row.get('city')}")
            print(f"    address: {row.get('address')}")
    else:
        print("Detected as PRICE file")
        rows, store_metadata = parse_prices_xml(xml_bytes, company="test")
        print(f"\nParsed {len(rows)} price items")
        print(f"\nStore metadata extracted:")
        print(f"  store_id: {store_metadata.get('store_id')}")
        print(f"  name: {store_metadata.get('name')}")
        print(f"  city: {store_metadata.get('city')}")
        print(f"  address: {store_metadata.get('address')}")
        
        if not store_metadata.get("address") and not store_metadata.get("city"):
            print("\n  ⚠️  WARNING: No address/city found in store metadata!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python diagnose_store_addresses.py check-db          # Check database")
        print("  python diagnose_store_addresses.py test-xml <file>  # Test XML parsing")
        print("\nExamples:")
        print("  python diagnose_store_addresses.py check-db")
        print("  python diagnose_store_addresses.py test-xml /path/to/StoresFull.xml")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "check-db":
        asyncio.run(check_database_stores())
    elif command == "test-xml":
        if len(sys.argv) < 3:
            print("ERROR: Please provide XML file path")
            sys.exit(1)
        test_xml_parsing(sys.argv[2])
    else:
        print(f"ERROR: Unknown command: {command}")
        sys.exit(1)
