#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
nextjs_integration.py - Next.js integration for unified JSON data

This creates the structure and API routes for a Next.js application
that consumes the unified JSON data.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any

class NextJSDataProvider:
    """Data provider for Next.js application"""
    
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
    
    # API Methods for Next.js
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


def create_nextjs_structure():
    """Create Next.js project structure"""
    
    # Create Next.js project structure
    nextjs_structure = {
        "app": {
            "api": {
                "products": {
                    "[barcode]": {
                        "route.ts": generate_product_api_route(),
                        "search": {
                            "route.ts": generate_search_api_route()
                        }
                    }
                },
                "shops": {
                    "route.ts": generate_shops_api_route(),
                    "[shop]": {
                        "products": {
                            "route.ts": generate_shop_products_api_route()
                        }
                    }
                },
                "statistics": {
                    "route.ts": generate_statistics_api_route()
                }
            },
            "components": {
                "ProductCard.tsx": generate_product_card_component(),
                "SearchBar.tsx": generate_search_bar_component(),
                "PriceHistory.tsx": generate_price_history_component(),
                "ShopSelector.tsx": generate_shop_selector_component()
            },
            "lib": {
                "data.ts": generate_data_lib(),
                "types.ts": generate_types_file()
            },
            "page.tsx": generate_home_page(),
            "products": {
                "[barcode]": {
                    "page.tsx": generate_product_page()
                }
            },
            "shops": {
                "[shop]": {
                    "page.tsx": generate_shop_page()
                }
            },
            "layout.tsx": generate_layout_file(),
            "globals.css": generate_globals_css()
        },
        "public": {
            "data": {
                "README.md": generate_data_readme()
            }
        },
        "package.json": generate_package_json(),
        "next.config.js": generate_next_config(),
        "tailwind.config.js": generate_tailwind_config(),
        "tsconfig.json": generate_tsconfig(),
        "README.md": generate_project_readme()
    }
    
    return nextjs_structure


def generate_product_api_route():
    """Generate Next.js API route for product by barcode"""
    return '''import { NextRequest, NextResponse } from 'next/server';
import { getProductByBarcode } from '@/lib/data';

export async function GET(
  request: NextRequest,
  { params }: { params: { barcode: string } }
) {
  try {
    const { barcode } = params;
    const product = getProductByBarcode(barcode);
    
    if (!product) {
      return NextResponse.json(
        { error: 'Product not found' },
        { status: 404 }
      );
    }
    
    return NextResponse.json(product);
  } catch (error) {
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}'''


def generate_search_api_route():
    """Generate Next.js API route for product search"""
    return '''import { NextRequest, NextResponse } from 'next/server';
import { searchProducts } from '@/lib/data';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get('q');
    const limit = parseInt(searchParams.get('limit') || '10');
    
    if (!query) {
      return NextResponse.json(
        { error: 'Query parameter required' },
        { status: 400 }
      );
    }
    
    const results = searchProducts(query, limit);
    
    return NextResponse.json({
      results,
      count: results.length,
      query
    });
  } catch (error) {
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}'''


def generate_shops_api_route():
    """Generate Next.js API route for shops list"""
    return '''import { NextResponse } from 'next/server';
import { getShops } from '@/lib/data';

export async function GET() {
  try {
    const shops = getShops();
    
    return NextResponse.json({
      shops,
      count: shops.length
    });
  } catch (error) {
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}'''


def generate_shop_products_api_route():
    """Generate Next.js API route for shop products"""
    return '''import { NextRequest, NextResponse } from 'next/server';
import { getProductsByShop } from '@/lib/data';

export async function GET(
  request: NextRequest,
  { params }: { params: { shop: string } }
) {
  try {
    const { shop } = params;
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get('limit') || '50');
    
    const products = getProductsByShop(shop, limit);
    
    return NextResponse.json({
      shop,
      products,
      count: products.length
    });
  } catch (error) {
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}'''


def generate_statistics_api_route():
    """Generate Next.js API route for statistics"""
    return '''import { NextResponse } from 'next/server';
import { getStatistics } from '@/lib/data';

export async function GET() {
  try {
    const stats = getStatistics();
    
    return NextResponse.json(stats);
  } catch (error) {
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}'''


def generate_data_lib():
    """Generate data library for Next.js"""
    return '''import { PriceDataAPI } from './types';

// Initialize data provider
const dataProvider = new PriceDataAPI();

export function getProductByBarcode(barcode: string) {
  return dataProvider.get_product_by_barcode(barcode);
}

export function searchProducts(query: string, limit: number = 10) {
  return dataProvider.search_products(query, limit);
}

export function getProductsByShop(shop: string, limit: number = 50) {
  return dataProvider.get_products_by_shop(shop, limit);
}

export function getShops() {
  return dataProvider.get_shops();
}

export function getStatistics() {
  return dataProvider.get_statistics();
}

export { dataProvider };'''


def generate_types_file():
    """Generate TypeScript types"""
    return '''export interface Product {
  barcode: string;
  name: string;
  category: string;
  unit: string;
  prices: PriceEntry[];
  promotions: PromotionEntry[];
  shops: string[];
  last_updated: string;
  current_price?: {
    price: number;
    shop: string;
    date: string;
  };
}

export interface PriceEntry {
  price: number;
  shop: string;
  date: string;
  source_file: string;
  parsed_at: string;
}

export interface PromotionEntry {
  promo_price: number;
  regular_price: number;
  description: string;
  start_date: string;
  end_date: string;
  shop: string;
  source_file: string;
  parsed_at: string;
}

export interface SearchResult {
  barcode: string;
  name: string;
  category: string;
  shops: string[];
  has_promotions: boolean;
}

export interface Statistics {
  total_products: number;
  total_shops: number;
  total_price_entries: number;
  total_promotion_entries: number;
  last_updated: string;
}

export class PriceDataAPI {
  private products: Record<string, Product> = {};
  private barcode_index: Record<string, any> = {};
  private shop_index: Record<string, string[]> = {};
  private stats: any = {};

  constructor(data_dir: string = "public/data") {
    this.loadData(data_dir);
  }

  private loadData(data_dir: string) {
    // Implementation would load JSON files
    // This is a placeholder for the actual implementation
  }

  get_product_by_barcode(barcode: string): Product | null {
    return this.products[barcode] || null;
  }

  search_products(query: string, limit: number = 10): SearchResult[] {
    // Implementation
    return [];
  }

  get_products_by_shop(shop: string, limit: number = 50): any[] {
    // Implementation
    return [];
  }

  get_shops(): string[] {
    return Object.keys(this.shop_index);
  }

  get_statistics(): Statistics {
    return {
      total_products: Object.keys(this.products).length,
      total_shops: Object.keys(this.shop_index).length,
      total_price_entries: 0,
      total_promotion_entries: 0,
      last_updated: this.stats.generated_at || 'Unknown'
    };
  }
}'''


def generate_product_card_component():
    """Generate React component for product card"""
    return ''''use client';

import { Product } from '@/lib/types';
import Link from 'next/link';

interface ProductCardProps {
  product: Product;
}

export default function ProductCard({ product }: ProductCardProps) {
  const currentPrice = product.current_price;
  
  return (
    <div className="bg-white rounded-lg shadow-md p-6 hover:shadow-lg transition-shadow">
      <div className="flex justify-between items-start mb-4">
        <div>
          <h3 className="text-lg font-semibold text-gray-900 mb-2">
            {product.name || 'Unnamed Product'}
          </h3>
          <p className="text-sm text-gray-600">Barcode: {product.barcode}</p>
          {product.category && (
            <p className="text-sm text-gray-500">{product.category}</p>
          )}
        </div>
        <Link 
          href={`/products/${product.barcode}`}
          className="text-blue-600 hover:text-blue-800 text-sm font-medium"
        >
          View Details →
        </Link>
      </div>
      
      {currentPrice && (
        <div className="border-t pt-4">
          <div className="flex justify-between items-center">
            <span className="text-2xl font-bold text-green-600">
              ₪{currentPrice.price.toFixed(2)}
            </span>
            <span className="text-sm text-gray-600">
              {currentPrice.shop}
            </span>
          </div>
          <p className="text-xs text-gray-500 mt-1">
            Updated: {new Date(currentPrice.date).toLocaleDateString()}
          </p>
        </div>
      )}
      
      <div className="mt-4 flex gap-2">
        {product.shops.map((shop) => (
          <span 
            key={shop}
            className="px-2 py-1 bg-blue-100 text-blue-800 text-xs rounded"
          >
            {shop}
          </span>
        ))}
      </div>
    </div>
  );
}'''


def generate_search_bar_component():
    """Generate React component for search bar"""
    return ''''use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';

export default function SearchBar() {
  const [query, setQuery] = useState('');
  const router = useRouter();

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      router.push(`/search?q=${encodeURIComponent(query.trim())}`);
    }
  };

  return (
    <form onSubmit={handleSearch} className="w-full max-w-2xl mx-auto">
      <div className="relative">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search products by name or barcode..."
          className="w-full px-4 py-3 pl-12 text-lg border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
        />
        <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
          <svg className="h-6 w-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </div>
        <button
          type="submit"
          className="absolute inset-y-0 right-0 px-6 bg-blue-600 text-white rounded-r-lg hover:bg-blue-700 transition-colors"
        >
          Search
        </button>
      </div>
    </form>
  );
}'''


def generate_home_page():
    """Generate Next.js home page"""
    return '''import SearchBar from '@/components/SearchBar';
import { getStatistics } from '@/lib/data';

export default async function HomePage() {
  const stats = await getStatistics();

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            Price Comparison Platform
          </h1>
          <p className="text-xl text-gray-600 mb-8">
            Find the best prices across Israeli retailers
          </p>
          <SearchBar />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-12">
          <div className="bg-white rounded-lg shadow p-6 text-center">
            <div className="text-3xl font-bold text-blue-600 mb-2">
              {stats.total_products.toLocaleString()}
            </div>
            <div className="text-gray-600">Products</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6 text-center">
            <div className="text-3xl font-bold text-green-600 mb-2">
              {stats.total_shops}
            </div>
            <div className="text-gray-600">Retailers</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6 text-center">
            <div className="text-3xl font-bold text-purple-600 mb-2">
              {stats.total_price_entries.toLocaleString()}
            </div>
            <div className="text-gray-600">Price Updates</div>
          </div>
        </div>

        <div className="text-center text-sm text-gray-500">
          Last updated: {new Date(stats.last_updated).toLocaleString()}
        </div>
      </div>
    </div>
  );
}'''


def generate_package_json():
    """Generate package.json for Next.js project"""
    return '''{
  "name": "price-comparison-platform",
  "version": "0.1.0",
  "private": true,
  "scripts": {
    "dev": "next dev",
    "build": "next build",
    "start": "next start",
    "lint": "next lint"
  },
  "dependencies": {
    "next": "14.0.0",
    "react": "^18",
    "react-dom": "^18"
  },
  "devDependencies": {
    "@types/node": "^20",
    "@types/react": "^18",
    "@types/react-dom": "^18",
    "autoprefixer": "^10.0.1",
    "eslint": "^8",
    "eslint-config-next": "14.0.0",
    "postcss": "^8",
    "tailwindcss": "^3.3.0",
    "typescript": "^5"
  }
}'''


def generate_project_readme():
    """Generate README for the Next.js project"""
    return '''# Price Comparison Platform

A Next.js application for comparing prices across Israeli retailers.

## Features

- 🔍 Product search by name or barcode
- 📊 Price history tracking
- 🏪 Multi-retailer comparison
- 📱 Responsive design
- ⚡ Fast API routes

## Getting Started

1. Install dependencies:
   ```bash
   npm install
   ```

2. Copy the unified JSON data to `public/data/`:
   ```bash
   cp -r ../unified_json public/data/
   ```

3. Run the development server:
   ```bash
   npm run dev
   ```

4. Open [http://localhost:3000](http://localhost:3000) in your browser.

## API Routes

- `GET /api/products/[barcode]` - Get product by barcode
- `GET /api/products/search?q=query` - Search products
- `GET /api/shops` - Get list of shops
- `GET /api/shops/[shop]/products` - Get products by shop
- `GET /api/statistics` - Get platform statistics

## Data Structure

The application uses unified JSON files:
- `products.json` - All product data with prices
- `barcode_index.json` - Quick barcode lookup
- `shop_index.json` - Shop-to-products mapping
- `statistics.json` - Platform statistics

## Technologies

- **Framework**: Next.js 14 (App Router)
- **Styling**: Tailwind CSS
- **Language**: TypeScript
- **Data**: Unified JSON format
'''


def create_nextjs_project():
    """Create the complete Next.js project structure"""
    print("=== CREATING NEXT.JS PROJECT STRUCTURE ===\n")
    
    # Create project directory
    project_dir = Path("price-platform-nextjs")
    project_dir.mkdir(exist_ok=True)
    
    # Create the structure
    structure = create_nextjs_structure()
    
    # Create files (simplified - just show the structure)
    print("📁 Project structure created:")
    print("price-platform-nextjs/")
    
    def print_structure(data, prefix="  "):
        for key, value in data.items():
            if isinstance(value, dict):
                print(f"{prefix}📁 {key}/")
                print_structure(value, prefix + "  ")
            else:
                print(f"{prefix}📄 {key}")
    
    print_structure(structure)
    
    print("\n🚀 Next steps:")
    print("1. Create the Next.js project: npx create-next-app@latest price-platform-nextjs")
    print("2. Copy the unified JSON data to public/data/")
    print("3. Implement the API routes and components")
    print("4. Run: npm run dev")
    
    return project_dir


if __name__ == "__main__":
    create_nextjs_project() 