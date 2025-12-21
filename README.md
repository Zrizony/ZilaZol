# ZilaZol

A serverless price crawler for Israeli retailers that automatically collects price data and stores it in a PostgreSQL database.

## 🎯 What It Does

- **Crawls** Israeli retailer websites using browser automation (Playwright)
- **Downloads** price files (XML, ZIP, GZ) from retailer sites
- **Parses** XML data to extract products, prices, and store information
- **Stores** everything directly in a Supabase PostgreSQL database
- **Runs automatically** daily via GitHub Actions

## 📁 Project Structure

```
.
├── ZilaZol/          # Python crawler (main application)
│   ├── crawler/      # Core crawling logic
│   ├── run_crawler.py # Standalone script for GitHub Actions
│   └── requirements.txt
├── NextJS/           # Next.js frontend (web interface)
│   ├── app/          # Next.js app router pages
│   ├── prisma/       # Database schema and migrations
│   └── lib/          # Database utilities
└── .github/workflows/ # GitHub Actions automation
    └── daily-crawler.yml # Scheduled daily crawler job
```

## 🚀 How It Works

1. **GitHub Actions** triggers the crawler daily at 07:00 UTC
2. **Playwright** opens browser and navigates to retailer websites
3. **Downloads** price files (handles authentication for protected sites)
4. **Parses** XML files to extract:
   - Products (barcode, name, brand, quantity, unit)
   - Prices (regular and promotional)
   - Stores (location, address, city)
5. **Saves** all data to PostgreSQL database via `DATABASE_URL`

## 🛠️ Setup

### Prerequisites

- Python 3.11+
- Node.js 18+ (for Next.js frontend)
- PostgreSQL database (Supabase recommended)
- GitHub account (for Actions)

### Local Development

#### 1. Crawler Setup

```bash
cd ZilaZol

# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Set environment variables
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
export LOG_LEVEL="INFO"

# Run crawler
python run_crawler.py
```

#### 2. Next.js Frontend Setup

```bash
cd NextJS

# Install dependencies
npm install

# Set up database connection
cp .env.example .env
# Edit .env and add your DATABASE_URL

# Run database migrations
npx prisma migrate dev

# Start development server
npm run dev
```

## 🔧 Configuration

### Environment Variables

**Required:**
- `DATABASE_URL` - PostgreSQL connection string (Supabase format)

**Optional:**
- `LOG_LEVEL` - Logging level (default: INFO)
- `RETAILER_CREDS_JSON` - JSON object with retailer credentials

### Retailer Configuration

Retailers are configured in `ZilaZol/data/retailers.json`. Each retailer can have:
- Multiple sources (URLs)
- Authentication credentials
- Custom adapters (PublishedPrices, Bina, Generic, Wolt)

## 📊 Supported Retailers

The crawler supports multiple retailer types:

- **PublishedPrices** - Cerberus-based retailers (requires authentication)
- **Bina Projects** - Retailers using Bina platform
- **Generic** - Public websites with downloadable price files
- **Wolt** - Wolt-based retailers

## 🤖 Automation

### GitHub Actions

The crawler runs automatically via GitHub Actions:

- **Schedule:** Daily at 07:00 UTC
- **Trigger:** Manual (workflow_dispatch) or scheduled
- **Environment:** Ubuntu latest with Python 3.11

To enable:
1. Add `DATABASE_URL` secret in GitHub Settings → Secrets
2. Push code to `main` branch
3. Workflow runs automatically

## 📝 Database Schema

The database stores:

- **Retailers** - Retailer information
- **Stores** - Store locations and details
- **Products** - Product catalog (barcode, name, brand, etc.)
- **Price Snapshots** - Historical price data with timestamps

See `NextJS/prisma/schema.prisma` for full schema definition.

## 🧪 Testing

```bash
# Run crawler locally
cd ZilaZol
python run_crawler.py

# Test Next.js API
cd NextJS
npm run dev
# Visit http://localhost:3000/api/test-db
```

## 📄 License

[Add your license here]

## 🤝 Contributing

[Add contribution guidelines here]

