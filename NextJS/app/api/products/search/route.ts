import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const query = searchParams.get('q');

    if (!query || query.trim().length === 0) {
      return NextResponse.json([]);
    }

    const searchTerm = query.trim();
    
    // Log search query for debugging
    console.log(`[Product Search] Query: "${searchTerm}"`);

    // Search products by name or barcode
    const products = await prisma.product.findMany({
      where: {
        OR: [
          {
            name: {
              contains: searchTerm,
              mode: 'insensitive',
            },
          },
          {
            barcode: {
              contains: searchTerm,
            },
          },
        ],
      },
      include: {
        prices: {
          include: {
            retailer: {
              select: {
                name: true,
                slug: true,
              },
            },
            store: {
              select: {
                name: true,
              },
            },
          },
          orderBy: {
            timestamp: 'desc',
          },
          take: 10, // Limit to most recent 10 prices per product
        },
      },
      take: 10, // Limit to 10 products
    });

    // Transform the data for the frontend
    const results = products.map((product) => ({
      productId: product.id,
      productName: product.name,
      barcode: product.barcode,
      brand: product.brand,
      quantity: product.quantity,
      unit: product.unit,
      imageUrl: (product as any).imageUrl || null,
      prices: product.prices.map((price) => {
        // Log suspicious prices for debugging
        if (price.price > 1000) {
          console.warn(`[Product Search] Suspicious price detected:`, {
            retailer: price.retailer.slug,
            productBarcode: product.barcode,
            price: price.price,
            isOnSale: price.isOnSale,
          });
        }
        return {
          retailerName: price.retailer.name,
          retailerSlug: price.retailer.slug,
          storeName: price.store?.name || null,
          price: price.price,
          isOnSale: price.isOnSale,
          timestamp: price.timestamp.toISOString(),
        };
      }),
    }));

    console.log(`[Product Search] Found ${results.length} products, total prices: ${results.reduce((sum, p) => sum + p.prices.length, 0)}`);
    return NextResponse.json(results);
  } catch (error) {
    console.error('Product search failed:', error);
    return NextResponse.json(
      { error: 'Failed to search products', details: String(error) },
      { status: 500 }
    );
  }
}

