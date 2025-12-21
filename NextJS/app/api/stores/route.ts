import { NextResponse } from "next/server";
import { prisma } from "@/lib/db";

export async function GET() {
  try {
    const stores = await prisma.store.findMany({
      include: {
        retailer: {
          select: {
            id: true,
            name: true,
            slug: true,
          },
        },
      },
      orderBy: { createdAt: "desc" },
    });

    return NextResponse.json(stores);
  } catch (error) {
    console.error("Database query failed:", error);
    return NextResponse.json(
      { error: "Failed to query database", details: String(error) },
      { status: 500 }
    );
  }
}

