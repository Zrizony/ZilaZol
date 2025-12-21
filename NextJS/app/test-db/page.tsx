import { prisma } from "@/lib/db";

export default async function TestDbPage() {
  let retailers;
  let error: string | null = null;

  try {
    retailers = await prisma.retailer.findMany({
      orderBy: { createdAt: "desc" },
    });
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
    retailers = [];
  }

  return (
    <div style={{ padding: "2rem", fontFamily: "system-ui, sans-serif" }}>
      <h1>Database Test Page</h1>
      <p>This page queries the database directly using Prisma.</p>

      {error && (
        <div style={{ color: "red", marginTop: "1rem" }}>
          <strong>Error:</strong> {error}
        </div>
      )}

      {!error && retailers.length === 0 && (
        <div style={{ marginTop: "1rem", color: "#666" }}>
          No retailers found. Run <code>npm run seed</code> to add test data.
        </div>
      )}

      {!error && retailers.length > 0 && (
        <div style={{ marginTop: "1rem" }}>
          <h2>Retailers ({retailers.length})</h2>
          <ul>
            {retailers.map((retailer) => (
              <li key={retailer.id}>
                <strong>{retailer.name}</strong> ({retailer.slug}) - Created:{" "}
                {retailer.createdAt.toLocaleString()}
              </li>
            ))}
          </ul>
        </div>
      )}

      <div style={{ marginTop: "2rem", paddingTop: "1rem", borderTop: "1px solid #ddd" }}>
        <p>
          <a href="/api/test-db">View JSON API</a> | <a href="/">Home</a>
        </p>
      </div>
    </div>
  );
}


