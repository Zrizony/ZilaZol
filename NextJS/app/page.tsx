export default function Home() {
  return (
    <div style={{ padding: "2rem", fontFamily: "system-ui, sans-serif" }}>
      <h1>ZilaZol Next.js Foundation</h1>
      <p>This is a minimal Next.js app connected to PostgreSQL via Prisma.</p>

      <div style={{ marginTop: "2rem" }}>
        <h2>Test Endpoints</h2>
        <ul style={{ marginTop: "1rem", listStyle: "none" }}>
          <li style={{ marginBottom: "0.5rem" }}>
            <a href="/test-db" style={{ color: "#0066cc" }}>
              /test-db
            </a>{" "}
            - Server-side rendered page that queries the database
          </li>
          <li style={{ marginBottom: "0.5rem" }}>
            <a href="/api/test-db" style={{ color: "#0066cc" }}>
              /api/test-db
            </a>{" "}
            - API route that returns JSON from the database
          </li>
        </ul>
      </div>

      <div style={{ marginTop: "2rem", padding: "1rem", background: "#f5f5f5", borderRadius: "4px" }}>
        <h3>Setup Instructions</h3>
        <ol style={{ marginLeft: "1.5rem", marginTop: "0.5rem" }}>
          <li>Copy <code>.env.example</code> to <code>.env</code> and set your <code>DATABASE_URL</code></li>
          <li>Run <code>npm install</code></li>
          <li>Run <code>npx prisma migrate dev</code> to create the database schema</li>
          <li>Run <code>npm run seed</code> to add test data (optional)</li>
          <li>Run <code>npm run dev</code> to start the development server</li>
        </ol>
      </div>
    </div>
  );
}


