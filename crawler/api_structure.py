#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
api_structure.py - API structure examples for backend consumption

This file shows how the backend can consume the unified JSON data
and provides example API endpoints and data structures.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional, Any

class PriceDataAPI:
    """Example API class for consuming unified JSON data"""
    
    def __init__(self, data_dir: str = "unified_json"):
        self.data_dir = data_dir
        self.products = {}
        self.barcode_index = {}
        self.shop_index = {}
        self.stats = {}
        self._load_data()
    
    def _load_data(self):
        """Load all JSON data files"""
        try:
            # Load products
            with open(f"{self.data_dir}/products.json", 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.products = data['products']
                self.stats = data['metadata']
            
            # Load barcode index
            with open(f"{self.data_dir}/barcode_index.json", 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.barcode_index = data['barcodes']
            
            # Load shop index
            with open(f"{self.data_dir}/shop_index.json", 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.shop_index = data['shops']
                
        except Exception as e:
            print(f"Error loading data: {e}")
    
    # API Endpoint Examples
    
    def get_product_by_barcode(self, barcode: str) -> Optional[Dict]:
        """Get product details by barcode"""
        if barcode in self.products:
            product = self.products[barcode].copy()
            
            # Add current price (latest)
            if product['prices']:
                latest_price = max(product['prices'], key=lambda x: x['date'])
                product['current_price'] = {
                    'price': latest_price['price'],
                    'shop': latest_price['shop'],
                    'date': latest_price['date']
                }
            
            # Add active promotions
            current_date = datetime.now().strftime('%Y-%m-%d')
            active_promotions = [
                p for p in product['promotions']
                if p['start_date'] <= current_date <= p['end_date']
            ]
            product['active_promotions'] = active_promotions
            
            return product
        return None
    
    def search_products(self, query: str, limit: int = 10) -> List[Dict]:
        """Search products by name"""
        results = []
        query_lower = query.lower()
        
        for barcode, product in self.products.items():
            if query_lower in product['name'].lower():
                results.append({
                    'barcode': barcode,
                    'name': product['name'],
                    'category': product['category'],
                    'shops': product['shops'],
                    'has_promotions': len(product['promotions']) > 0
                })
                
                if len(results) >= limit:
                    break
        
        return results
    
    def get_products_by_shop(self, shop: str, limit: int = 50) -> List[Dict]:
        """Get all products from a specific shop"""
        if shop not in self.shop_index:
            return []
        
        results = []
        for barcode in self.shop_index[shop][:limit]:
            product = self.products[barcode]
            results.append({
                'barcode': barcode,
                'name': product['name'],
                'category': product['category'],
                'price_entries': len(product['prices']),
                'promotion_entries': len(product['promotions'])
            })
        
        return results
    
    def get_price_history(self, barcode: str, days: int = 30) -> List[Dict]:
        """Get price history for a product"""
        if barcode not in self.products:
            return []
        
        product = self.products[barcode]
        cutoff_date = datetime.now().strftime('%Y-%m-%d')
        
        # Filter prices by date (simplified)
        recent_prices = product['prices'][-days:] if len(product['prices']) > days else product['prices']
        
        return recent_prices
    
    def get_promotions_by_shop(self, shop: str) -> List[Dict]:
        """Get all active promotions from a shop"""
        current_date = datetime.now().strftime('%Y-%m-%d')
        promotions = []
        
        for barcode, product in self.products.items():
            if shop in product['shops']:
                for promo in product['promotions']:
                    if promo['shop'] == shop and promo['start_date'] <= current_date <= promo['end_date']:
                        promotions.append({
                            'barcode': barcode,
                            'name': product['name'],
                            'promo_price': promo['promo_price'],
                            'regular_price': promo['regular_price'],
                            'description': promo['description'],
                            'start_date': promo['start_date'],
                            'end_date': promo['end_date']
                        })
        
        return promotions
    
    def get_statistics(self) -> Dict:
        """Get overall statistics"""
        return {
            'total_products': len(self.products),
            'total_shops': len(self.shop_index),
            'total_price_entries': sum(len(p['prices']) for p in self.products.values()),
            'total_promotion_entries': sum(len(p['promotions']) for p in self.products.values()),
            'last_updated': self.stats.get('generated_at', 'Unknown')
        }
    
    def get_shops(self) -> List[str]:
        """Get list of all shops"""
        return list(self.shop_index.keys())


# Example Flask API endpoints
def create_flask_app():
    """Example Flask app structure"""
    
    from flask import Flask, jsonify, request
    
    app = Flask(__name__)
    api = PriceDataAPI()
    
    @app.route('/api/products/<barcode>', methods=['GET'])
    def get_product(barcode):
        """Get product by barcode"""
        product = api.get_product_by_barcode(barcode)
        if product:
            return jsonify(product)
        return jsonify({'error': 'Product not found'}), 404
    
    @app.route('/api/products/search', methods=['GET'])
    def search_products():
        """Search products"""
        query = request.args.get('q', '')
        limit = int(request.args.get('limit', 10))
        
        if not query:
            return jsonify({'error': 'Query parameter required'}), 400
        
        results = api.search_products(query, limit)
        return jsonify({'results': results, 'count': len(results)})
    
    @app.route('/api/shops/<shop>/products', methods=['GET'])
    def get_shop_products(shop):
        """Get products from a shop"""
        limit = int(request.args.get('limit', 50))
        results = api.get_products_by_shop(shop, limit)
        return jsonify({'shop': shop, 'products': results, 'count': len(results)})
    
    @app.route('/api/shops/<shop>/promotions', methods=['GET'])
    def get_shop_promotions(shop):
        """Get promotions from a shop"""
        promotions = api.get_promotions_by_shop(shop)
        return jsonify({'shop': shop, 'promotions': promotions, 'count': len(promotions)})
    
    @app.route('/api/products/<barcode>/history', methods=['GET'])
    def get_price_history(barcode):
        """Get price history for a product"""
        days = int(request.args.get('days', 30))
        history = api.get_price_history(barcode, days)
        return jsonify({'barcode': barcode, 'history': history, 'count': len(history)})
    
    @app.route('/api/statistics', methods=['GET'])
    def get_statistics():
        """Get overall statistics"""
        return jsonify(api.get_statistics())
    
    @app.route('/api/shops', methods=['GET'])
    def get_shops():
        """Get list of shops"""
        shops = api.get_shops()
        return jsonify({'shops': shops, 'count': len(shops)})
    
    return app


# Example usage
def demo_api_usage():
    """Demonstrate API usage"""
    print("=== PRICE DATA API DEMO ===\n")
    
    api = PriceDataAPI()
    
    # Get statistics
    stats = api.get_statistics()
    print(f"Total products: {stats['total_products']}")
    print(f"Total shops: {stats['total_shops']}")
    print(f"Last updated: {stats['last_updated']}")
    print()
    
    # Get shops
    shops = api.get_shops()
    print(f"Available shops: {shops}")
    print()
    
    # Search for a product
    search_results = api.search_products("חלב", limit=3)
    print(f"Search results for 'חלב': {len(search_results)} products")
    for result in search_results:
        print(f"  - {result['barcode']}: {result['name']}")
    print()
    
    # Get product details
    if search_results:
        barcode = search_results[0]['barcode']
        product = api.get_product_by_barcode(barcode)
        if product:
            print(f"Product details for {barcode}:")
            print(f"  Name: {product['name']}")
            print(f"  Category: {product['category']}")
            print(f"  Shops: {product['shops']}")
            if 'current_price' in product:
                print(f"  Current price: {product['current_price']['price']} at {product['current_price']['shop']}")
            print(f"  Price history entries: {len(product['prices'])}")
            print(f"  Promotion entries: {len(product['promotions'])}")


if __name__ == "__main__":
    demo_api_usage() 