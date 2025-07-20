#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
view_json_sample.py - View sample data from unified JSON files
"""

import json
from pathlib import Path

def view_sample_data():
    """View sample data from unified JSON files"""
    
    # Load statistics
    with open("unified_json/statistics.json", 'r', encoding='utf-8') as f:
        stats = json.load(f)
    
    print("=== UNIFIED JSON STATISTICS ===")
    print(f"Generated at: {stats['generated_at']}")
    print(f"Files processed: {stats['statistics']['files_processed']}")
    print(f"Total products: {stats['statistics']['total_products']}")
    print(f"Unique products: {stats['summary']['unique_products']}")
    print(f"Total price entries: {stats['summary']['total_price_entries']}")
    print()
    
    # Load sample from barcode index
    with open("unified_json/barcode_index.json", 'r', encoding='utf-8') as f:
        barcode_data = json.load(f)
    
    print("=== SAMPLE BARCODE INDEX ===")
    sample_barcodes = list(barcode_data['barcodes'].keys())[:5]
    for barcode in sample_barcodes:
        data = barcode_data['barcodes'][barcode]
        print(f"Barcode: {barcode}")
        print(f"  Name: {data['name']}")
        print(f"  Category: {data['category']}")
        print(f"  Shops: {data['shops']}")
        print(f"  Has promotions: {data['has_promotions']}")
        print()
    
    # Load sample from products
    with open("unified_json/products.json", 'r', encoding='utf-8') as f:
        products_data = json.load(f)
    
    print("=== SAMPLE PRODUCT DATA ===")
    sample_products = list(products_data['products'].keys())[:3]
    for barcode in sample_products:
        product = products_data['products'][barcode]
        print(f"Barcode: {barcode}")
        print(f"  Name: {product['name']}")
        print(f"  Category: {product['category']}")
        print(f"  Unit: {product['unit']}")
        print(f"  Shops: {product['shops']}")
        print(f"  Price entries: {len(product['prices'])}")
        print(f"  Promotion entries: {len(product['promotions'])}")
        
        # Show latest price
        if product['prices']:
            latest_price = max(product['prices'], key=lambda x: x['date'])
            print(f"  Latest price: {latest_price['price']} at {latest_price['shop']} ({latest_price['date']})")
        
        print()
    
    # Load shop index
    with open("unified_json/shop_index.json", 'r', encoding='utf-8') as f:
        shop_data = json.load(f)
    
    print("=== SHOP INDEX ===")
    for shop, barcodes in shop_data['shops'].items():
        print(f"Shop: {shop}")
        print(f"  Products: {len(barcodes)}")
        print(f"  Sample barcodes: {barcodes[:3]}")
        print()

if __name__ == "__main__":
    view_sample_data() 