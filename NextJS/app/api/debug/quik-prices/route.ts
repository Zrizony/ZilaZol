import { NextResponse } from 'next/server';
import { prisma } from '@/lib/db';

export async function GET() {
  try {
    // Find Quik retailer
    const quik = await prisma.retailer.findUnique({
      where: { slug: 'quik' },
    });

    if (!quik) {
      return NextResponse.json({ error: 'Quik retailer not found' }, { status: 404 });
    }

    // Get some recent prices from Quik with suspiciously high values
    const highPrices = await prisma.priceSnapshot.findMany({
      where: {
        retailerId: quik.id,
        price: {
          gt: 100, // Prices over 100 ILS
        },
      },
      include: {
        product: {
          select: {
            barcode: true,
            name: true,
          },
        },
        store: {
          select: {
            externalId: true,
            name: true,
          },
        },
      },
      orderBy: {
        timestamp: 'desc',
      },
      take: 20,
    });

    // Get sample of all Quik prices
    const allPrices = await prisma.priceSnapshot.findMany({
      where: {
        retailerId: quik.id,
      },
      include: {
        product: {
          select: {
            barcode: true,
            name: true,
          },
        },
        store: {
          select: {
            externalId: true,
            name: true,
          },
        },
      },
      orderBy: {
        timestamp: 'desc',
      },
      take: 50,
    });

    // Calculate statistics
    const prices = allPrices.map(p => p.price);
    const stats = {
      count: prices.length,
      min: prices.length > 0 ? Math.min(...prices) : 0,
      max: prices.length > 0 ? Math.max(...prices) : 0,
      avg: prices.length > 0 ? prices.reduce((a, b) => a + b, 0) / prices.length : 0,
      highPriceCount: highPrices.length,
    };

    return NextResponse.json({
      retailer: {
        id: quik.id,
        name: quik.name,
        slug: quik.slug,
      },
      statistics: stats,
      highPrices: highPrices.map(p => ({
        id: p.id,
        productBarcode: p.product.barcode,
        productName: p.product.name,
        storeId: p.store?.externalId,
        storeName: p.store?.name,
        price: p.price,
        isOnSale: p.isOnSale,
        timestamp: p.timestamp.toISOString(),
      })),
      samplePrices: allPrices.slice(0, 10).map(p => ({
        productBarcode: p.product.barcode,
        productName: p.product.name,
        price: p.price,
        isOnSale: p.isOnSale,
        timestamp: p.timestamp.toISOString(),
      })),
    });
  } catch (error) {
    console.error('Debug query failed:', error);
    return NextResponse.json(
      { error: 'Failed to query', details: String(error) },
      { status: 500 }
    );
  }
}

