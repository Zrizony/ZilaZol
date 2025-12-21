import { NextResponse } from "next/server";
import { prisma } from "@/lib/db";

export async function GET() {
  try {
    const retailers = await prisma.retailer.findMany({
      orderBy: { createdAt: "desc" },
    });

    return NextResponse.json(retailers);
  } catch (error) {
    console.error("Database query failed:", error);
    return NextResponse.json(
      { error: "Failed to query database", details: String(error) },
      { status: 500 }
    );
  }
}


