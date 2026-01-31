#!/usr/bin/env python3
"""Quick test to see Victory API product structure"""
import requests
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

url = "https://www.victoryonline.co.il/v2/retailers/1470/branches/2440/products"
params = {
    "appId": "4",
    "filters": '{"bool":{"should":[{"bool":{"must_not":{"exists":{"field":"branch.outOfStockShowUntilDate"}}}},{"bool":{"must":[{"range":{"branch.outOfStockShowUntilDate":{"gt":"now"}}},{"term":{"branch.isOutOfStock":true}}]}},{"bool":{"must":[{"term":{"branch.isOutOfStock":false}}]}}]}}',
    "from": "0",
    "languageId": "1",
    "minScore": "0",
    "size": "10"
}

headers = {
    "accept": "application/json, text/plain, */*",
    "user-agent": "Mozilla/5.0"
}
cookies = {"retailerId": "1470"}

response = requests.get(url, params=params, headers=headers, cookies=cookies, timeout=30)
data = response.json()
products = data.get("products", data.get("data", []))

print(f"Found {len(products)} products\n")

for i, p in enumerate(products[:3], 1):
    print(f"=== Product {i} ===")
    print(f"All keys: {list(p.keys())}\n")
    
    # Try to find name
    if "name" in p:
        print(f"name: {p['name']}")
    if "names" in p:
        print(f"names: {p['names']}")
    if "localName" in p:
        print(f"localName: {p['localName']}")
    
    # Check weighted fields
    if "isWeighable" in p:
        print(f"isWeighable: {p['isWeighable']}")
    if "unitOfMeasure" in p:
        print(f"unitOfMeasure: {p['unitOfMeasure']}")
    if "weight" in p:
        print(f"weight: {p['weight']}")
    
    # Check branch
    if "branch" in p:
        branch = p["branch"]
        print(f"branch keys: {list(branch.keys())[:20]}")
        if "regularPrice" in branch:
            print(f"branch.regularPrice: {branch['regularPrice']}")
        if "unitOfMeasure" in branch:
            print(f"branch.unitOfMeasure: {branch['unitOfMeasure']}")
        if "pricePerUnit" in branch:
            print(f"branch.pricePerUnit: {branch['pricePerUnit']}")
    
    print("\n" + "="*50 + "\n")
