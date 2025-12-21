import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

async function main() {
  console.log("🌱 Seeding database...");

  // Create a retailer
  const retailer = await prisma.retailer.upsert({
    where: { slug: "test-retailer" },
    update: {},
    create: {
      name: "Test Retailer",
      slug: "test-retailer",
    },
  });

  console.log("✅ Created retailer:", retailer.name);

  // Create a store
  const store = await prisma.store.upsert({
    where: { externalId: "store-001" },
    update: {},
    create: {
      retailerId: retailer.id,
      name: "Test Store",
      city: "Tel Aviv",
      externalId: "store-001",
    },
  });

  console.log("✅ Created store:", store.name);

  // Create a product
  const product = await prisma.product.upsert({
    where: { barcode: "1234567890123" },
    update: {},
    create: {
      barcode: "1234567890123",
      name: "Test Product",
      brand: "Test Brand",
      size: 500,
      unit: "g",
      category: "Food",
    },
  });

  console.log("✅ Created product:", product.name);

  // Create a price snapshot
  const priceSnapshot = await prisma.priceSnapshot.create({
    data: {
      productId: product.id,
      retailerId: retailer.id,
      storeId: store.id,
      price: 29.90,
      currency: "ILS",
      isOnSale: false,
      timestamp: new Date(),
    },
  });

  console.log("✅ Created price snapshot:", priceSnapshot.id);
  console.log("🎉 Seeding completed!");
}

main()
  .catch((e) => {
    console.error("❌ Seeding failed:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });


