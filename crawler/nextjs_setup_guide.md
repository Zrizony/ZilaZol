# Next.js Integration Guide for Price Comparison Platform

## 🎯 Overview

This guide shows how to integrate the unified JSON data with a Next.js application for both frontend and backend.

## 📁 Project Structure

```
price-platform-nextjs/
├── app/
│   ├── api/                    # Backend API routes
│   │   ├── products/
│   │   │   ├── [barcode]/
│   │   │   │   └── route.ts    # GET /api/products/[barcode]
│   │   │   └── search/
│   │   │       └── route.ts    # GET /api/products/search?q=query
│   │   ├── shops/
│   │   │   ├── route.ts        # GET /api/shops
│   │   │   └── [shop]/
│   │   │       └── products/
│   │   │           └── route.ts # GET /api/shops/[shop]/products
│   │   └── statistics/
│   │       └── route.ts        # GET /api/statistics
│   ├── components/             # React components
│   │   ├── ProductCard.tsx
│   │   ├── SearchBar.tsx
│   │   └── PriceHistory.tsx
│   ├── lib/                    # Data access layer
│   │   ├── data.ts
│   │   └── types.ts
│   ├── page.tsx               # Home page
│   ├── products/
│   │   └── [barcode]/
│   │       └── page.tsx       # Product detail page
│   └── layout.tsx
├── public/
│   └── data/                  # Unified JSON files
│       ├── products.json
│       ├── barcode_index.json
│       ├── shop_index.json
│       └── statistics.json
└── package.json
```

## 🚀 Quick Setup

### 1. Create Next.js Project

```bash
npx create-next-app@latest price-platform-nextjs --typescript --tailwind --app
cd price-platform-nextjs
```

### 2. Copy Data Files

```bash
# Copy unified JSON data to public folder
cp -r ../unified_json public/data/
```

### 3. Install Dependencies

```bash
npm install
```

## 📊 Data Integration

### 1. Create Data Access Layer (`app/lib/data.ts`)

```typescript
import { PriceDataAPI } from './types';

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
```

### 2. Create TypeScript Types (`app/lib/types.ts`)

```typescript
export interface Product {
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

export class PriceDataAPI {
  private products: Record<string, Product> = {};
  private barcode_index: Record<string, any> = {};
  private shop_index: Record<string, string[]> = {};
  private stats: any = {};

  constructor(data_dir: string = "public/data") {
    this.loadData(data_dir);
  }

  private loadData(data_dir: string) {
    // Load JSON files from public/data
    // Implementation details below
  }

  get_product_by_barcode(barcode: string): Product | null {
    return this.products[barcode] || null;
  }

  search_products(query: string, limit: number = 10): any[] {
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

  get_statistics(): any {
    return {
      total_products: Object.keys(this.products).length,
      total_shops: Object.keys(this.shop_index).length,
      last_updated: this.stats.generated_at || 'Unknown'
    };
  }
}
```

## 🔌 API Routes

### 1. Product by Barcode (`app/api/products/[barcode]/route.ts`)

```typescript
import { NextRequest, NextResponse } from 'next/server';
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
}
```

### 2. Product Search (`app/api/products/search/route.ts`)

```typescript
import { NextRequest, NextResponse } from 'next/server';
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
}
```

### 3. Statistics (`app/api/statistics/route.ts`)

```typescript
import { NextResponse } from 'next/server';
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
}
```

## 🎨 Frontend Components

### 1. Product Card (`app/components/ProductCard.tsx`)

```typescript
'use client';

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
        </div>
      )}
    </div>
  );
}
```

### 2. Search Bar (`app/components/SearchBar.tsx`)

```typescript
'use client';

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
        <button
          type="submit"
          className="absolute inset-y-0 right-0 px-6 bg-blue-600 text-white rounded-r-lg hover:bg-blue-700 transition-colors"
        >
          Search
        </button>
      </div>
    </form>
  );
}
```

## 🏠 Pages

### 1. Home Page (`app/page.tsx`)

```typescript
import SearchBar from '@/components/SearchBar';
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

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
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
      </div>
    </div>
  );
}
```

## 🔄 Data Loading Implementation

For the `PriceDataAPI` class, you'll need to implement the data loading:

```typescript
private async loadData(data_dir: string) {
  try {
    // Load products
    const productsResponse = await fetch('/data/products.json');
    const productsData = await productsResponse.json();
    this.products = productsData.products;
    this.stats = productsData.metadata;
    
    // Load barcode index
    const barcodeResponse = await fetch('/data/barcode_index.json');
    const barcodeData = await barcodeResponse.json();
    this.barcode_index = barcodeData.barcodes;
    
    // Load shop index
    const shopResponse = await fetch('/data/shop_index.json');
    const shopData = await shopResponse.json();
    this.shop_index = shopData.shops;
  } catch (error) {
    console.error('Error loading data:', error);
  }
}
```

## 🚀 Running the Application

```bash
# Development
npm run dev

# Production build
npm run build
npm start
```

## 📱 API Endpoints

- `GET /api/products/[barcode]` - Get product by barcode
- `GET /api/products/search?q=query&limit=10` - Search products
- `GET /api/shops` - Get list of shops
- `GET /api/shops/[shop]/products?limit=50` - Get products by shop
- `GET /api/statistics` - Get platform statistics

## 🎯 Benefits of This Approach

1. **Unified Data**: Single source of truth from unified JSON
2. **Type Safety**: Full TypeScript support
3. **Server-Side Rendering**: Fast initial page loads
4. **API Routes**: Built-in backend functionality
5. **Responsive Design**: Tailwind CSS for mobile-first design
6. **SEO Friendly**: Server-side rendering for search engines

This structure gives you a complete full-stack application with the unified JSON data as the foundation! 