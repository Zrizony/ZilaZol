# Supers Next.js Foundation

A minimal Next.js application with Prisma ORM and PostgreSQL database connection.

## Overview

This is a clean foundation that demonstrates:
- Next.js App Router with TypeScript
- Prisma ORM integration
- PostgreSQL database connection
- Server-side rendering with database queries
- API routes with database queries

## Database Schema

The application includes 4 models:
- **Retailer** - Retailer information
- **Store** - Store locations (linked to retailers)
- **Product** - Product catalog (by barcode)
- **PriceSnapshot** - Historical price data (links products, retailers, and stores)

## Setup Instructions

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure Database

Create a `.env` file in the `NextJS` directory:

```bash
DATABASE_URL="postgres://USER:PASSWORD@HOST:5432/DATABASE"
```

Replace `USER`, `PASSWORD`, `HOST`, and `DATABASE` with your PostgreSQL credentials.

**Example:**
```bash
DATABASE_URL="postgres://postgres:password@localhost:5432/supers"
```

### 3. Run Database Migrations

Create the database schema:

```bash
npx prisma migrate dev --name init
```

This will:
- Create the database tables based on `prisma/schema.prisma`
- Generate the Prisma Client
- Create a migration file

### 4. Seed Test Data (Optional)

Add sample data to verify everything works:

```bash
npm run seed
```

This creates:
- 1 Retailer
- 1 Store
- 1 Product
- 1 PriceSnapshot

### 5. Start Development Server

```bash
npm run dev
```

The app will be available at `http://localhost:3000`

## Testing the Connection

### Server-Side Page

Visit: **http://localhost:3000/test-db**

This page queries the database directly using a server component and displays the results.

### API Route

Visit: **http://localhost:3000/api/test-db**

This API route returns JSON data from the database. Returns an empty array `[]` if no data exists.

## Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run start` - Start production server
- `npm run lint` - Run ESLint
- `npm run seed` - Seed the database with test data
- `npm run db:push` - Push schema changes to database (without migrations)
- `npm run db:migrate` - Create and apply migrations
- `npm run db:studio` - Open Prisma Studio (database GUI)

## Project Structure

```
NextJS/
├── app/
│   ├── api/
│   │   └── test-db/
│   │       └── route.ts          # API route example
│   ├── test-db/
│   │   └── page.tsx              # Server component page example
│   ├── layout.tsx                # Root layout
│   ├── page.tsx                  # Home page
│   └── globals.css               # Global styles
├── lib/
│   └── db.ts                     # Prisma client singleton
├── prisma/
│   ├── schema.prisma             # Database schema
│   └── seed.ts                   # Seed script
├── .env.example                  # Environment variables template
├── package.json
├── tsconfig.json
└── next.config.js
```

## Troubleshooting

### Database Connection Issues

- Verify your `DATABASE_URL` is correct
- Ensure PostgreSQL is running
- Check that the database exists
- Verify network connectivity to the database host

### Prisma Client Not Generated

If you see errors about Prisma Client not being found:

```bash
npx prisma generate
```

### Migration Issues

If migrations fail:

```bash
# Reset the database (WARNING: deletes all data)
npx prisma migrate reset

# Or push schema without migrations
npx prisma db push
```

## Next Steps

This foundation is ready for you to build upon. You can:

1. Add more API routes in `app/api/`
2. Create more pages in `app/`
3. Extend the Prisma schema with additional models
4. Add authentication
5. Build UI components
6. Integrate with the Supers crawler

## Notes

- The Prisma client is configured as a singleton to prevent multiple instances in development
- All database queries use the `@/lib/db` helper
- The seed script uses `upsert` to be idempotent (safe to run multiple times)
- TypeScript is strictly configured for type safety

