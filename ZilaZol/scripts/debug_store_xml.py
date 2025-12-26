#!/usr/bin/env python3
"""
Debug script to inspect store XML files and see what address fields are available.
"""
import sys
import os
from pathlib import Path
from lxml import etree

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def inspect_xml_file(xml_path: str):
    """Inspect an XML file and show all store-related tags."""
    print(f"\n{'='*60}")
    print(f"Inspecting: {xml_path}")
    print(f"{'='*60}\n")
    
    try:
        with open(xml_path, 'rb') as f:
            xml_bytes = f.read()
        
        root = etree.fromstring(xml_bytes)
        
        # Find all Store elements
        stores = root.findall(".//Store")
        print(f"Found {len(stores)} Store elements\n")
        
        for i, store in enumerate(stores[:3], 1):  # Show first 3 stores
            print(f"--- Store {i} ---")
            print(f"Tag: {store.tag}")
            print(f"Attributes: {store.attrib}")
            print(f"All child elements:")
            for child in store:
                text = child.text.strip() if child.text else "(empty)"
                print(f"  <{child.tag}>: {text[:100]}")
            print()
        
        # Also check root level for store info
        print("--- Root Level Store Info ---")
        root_store_tags = ["StoreId", "StoreID", "storeid", "StoreName", "StoreNm", 
                          "Name", "City", "CityName", "Address", "Street", "StoreAddress",
                          "AddressLine1", "FullAddress", "Location", "StreetAddress"]
        for tag in root_store_tags:
            elem = root.find(f".//{tag}")
            if elem is not None and elem.text:
                print(f"  <{tag}>: {elem.text.strip()[:100]}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_store_xml.py <path_to_xml_file>")
        print("\nExample:")
        print("  python debug_store_xml.py /path/to/StoresFull.xml")
        sys.exit(1)
    
    xml_path = sys.argv[1]
    if not os.path.exists(xml_path):
        print(f"Error: File not found: {xml_path}")
        sys.exit(1)
    
    inspect_xml_file(xml_path)

