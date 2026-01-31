# Supers Price Crawler

## What it does
- Crawls Israeli retailers for price data using Playwright automation
- Supports multiple retailer types: PublishedPrices (Cerberus), Bina Projects, and generic public sites
- Downloads files (ZIP/GZ/XML) and parses them directly
- Saves parsed data to PostgreSQL database
- Runs automatically via GitHub Actions

## Features
- **Multi-adapter architecture** for different retailer types
- **Authentication support** for protected sites (PublishedPrices)
- **Folder navigation** for retailers like Super Yuda
- **Duplicate detection** using MD5 hashes
- **Direct database storage** - no intermediate file storage
- **Structured logging** for debugging and monitoring
- **Memory monitoring** to track resource usage

## Environment Variables

### Required
- `DATABASE_URL` - PostgreSQL connection string (saves parsed data to database)
- `RETAILER_CREDS_JSON` - JSON object with retailer credentials

### Optional
- `LOG_LEVEL` - Logging level (default: INFO)

## Configuration

### Credentials
Set `RETAILER_CREDS_JSON` as a JSON object mapping retailer keys to credentials:

```json
{
  "doralon": {"username": "doralon", "password": ""},
  "TivTaam": {"username": "TivTaam", "password": ""},
  "SalachD": {"username": "SalachD", "password": "12345"},
  "yuda_ho": {"username": "yuda_ho", "password": "Yud@147"}
}
```

## Usage

### Local Development

```bash
# Set up environment
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
export RETAILER_CREDS_JSON='{"cerberus":{"username":"USER","password":"PASS"}}'
export LOG_LEVEL=INFO

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Run crawler for all retailers
python run_crawler.py

# Run crawler for specific retailer
python run_crawler.py --retailer=shufersal

# Run crawler for public retailers only (no login required)
python run_crawler.py --type=public

# Run crawler for authenticated retailers only
python run_crawler.py --type=auth
```

### Command Line Options

```bash
# Single retailer
python run_crawler.py --retailer=shufersal --timeout=300

# Filter by type
python run_crawler.py --type=public    # Public retailers only
python run_crawler.py --type=auth      # Authenticated retailers only

# Timeout (in minutes, default: 300 = 5 hours)
python run_crawler.py --retailer=shufersal --timeout=60
```

## Automation

### GitHub Actions

The crawler runs automatically via GitHub Actions:

- **Schedule:** Daily at 07:00 UTC
- **Trigger:** Manual (`workflow_dispatch`) or scheduled
- **Environment:** Ubuntu latest with Python 3.11
- **Strategy:** Matrix strategy - each retailer runs in parallel
- **Timeout:** 330 minutes per retailer (5.5 hours, buffer before GitHub's 6h limit)

**Workflow file:** `.github/workflows/daily-crawler.yml`

**To enable:**
1. Add `DATABASE_URL` secret in GitHub Settings → Secrets
2. Add `RETAILER_CREDS_JSON` secret (if using authenticated retailers)
3. Push code to `main` branch
4. Workflow runs automatically on schedule or can be triggered manually

**Matrix Strategy:**
- Each retailer runs in its own GitHub Actions job
- Jobs run in parallel (up to GitHub Actions concurrency limits)
- Failed jobs don't block other retailers (`fail-fast: false`)

## How It Works

1. **GitHub Actions** triggers workflow (scheduled or manual)
2. **Playwright** opens browser and navigates to retailer websites
3. **Downloads** price files (handles authentication for protected sites)
4. **Parses** XML files in memory to extract:
   - Products (barcode, name, brand, quantity, unit)
   - Prices (regular and promotional)
   - Stores (location, address, city)
5. **Saves** all data directly to PostgreSQL database via `DATABASE_URL`

## Special Cases

### Super Yuda Folder Navigation
Super Yuda requires navigation to the "Yuda" folder after login. The crawler:
1. Logs into PublishedPrices
2. Navigates to the "Yuda" folder
3. Collects and downloads files from that folder

### Duplicate Detection
- Files are deduplicated by MD5 hash within each run
- Duplicate files are skipped and counted

### Price Normalization
- Store 89 saves prices in Agoras (×100) instead of Shekels
- Automatically normalized: if price > 1000 and storeId is 89, divide by 100

## Logging

The crawler uses structured logging:

```
2024-12-01T14:30:22Z INFO run.start run_id=20241201T143022Z-abc12345 retailers=5
2024-12-01T14:30:22Z INFO mem.stats rss_mb=850.4 vms_mb=3200.0 note=run.start run_id=20241201T143022Z-abc12345
2024-12-01T14:30:23Z INFO login.start retailer=publishedprices
2024-12-01T14:30:25Z INFO login.success retailer=publishedprices
2024-12-01T14:30:26Z INFO folder.navigate retailer=publishedprices folder=Yuda
2024-12-01T14:30:28Z INFO folder.navigate.success retailer=publishedprices folder=Yuda method=direct
2024-12-01T14:30:30Z INFO db.saved retailer=superyuda count=1234/1234
```

### Memory Monitoring

The crawler includes memory usage instrumentation using `psutil` to track RAM consumption:

**Memory log format:**
```
mem.stats rss_mb=1470.2 vms_mb=4000.0 note=before_retailer id=shufersal
```

- `rss_mb`: Resident Set Size (actual RAM used) in MiB
- `vms_mb`: Virtual Memory Size in MiB
- `note`: Context describing when the measurement was taken

**Checkpoints logged:**
- Run start/end (`run.start`, `run_all.done_before_manifest`)
- Before/after each retailer (`before_retailer`, `after_retailer`)
- Link collection phases (`bina.before_collect_links`, `generic.after_collect_links`)
- Download phases (`bina.before_downloads`, `generic.after_downloads`)

### Concurrency Limiting

The crawler uses `asyncio.Semaphore(3)` to limit concurrent retailers:
- Maximum 3 Playwright browsers running simultaneously
- Prevents memory spikes from crawling 30+ retailers in parallel
- Each retailer waits for a semaphore slot before starting

## Database Schema

The crawler saves data to PostgreSQL with the following structure:

- **Retailers** - Retailer information (slug, name, needCreds)
- **Stores** - Store locations and details (externalId, name, city, address)
- **Products** - Product catalog (barcode, name, brand, quantity, unit, isWeighted)
- **Price Snapshots** - Historical price data (productId, retailerId, storeId, price, isOnSale, timestamp)

See `NextJS/prisma/schema.prisma` for full schema definition.

## Troubleshooting

### Database Connection Issues
- Verify your `DATABASE_URL` is correct
- Ensure PostgreSQL is running and accessible
- Check network connectivity to the database host

### Memory Issues
- Monitor `mem.stats` logs to track memory usage
- If hitting limits, reduce concurrency (currently 3)
- Consider running fewer retailers per job

### Timeout Issues
- Default timeout is 300 minutes (5 hours)
- Increase with `--timeout` flag if needed
- GitHub Actions has a 6-hour limit per job

### Authentication Failures
- Verify `RETAILER_CREDS_JSON` is correctly formatted
- Check credentials in `data/retailers.json`
- Ensure tenant keys match between config and credentials

## Project Structure

```
Supers/
├── crawler/              # Core crawling logic
│   ├── adapters/        # Retailer-specific adapters
│   │   ├── publishedprices.py
│   │   ├── bina.py
│   │   ├── generic.py
│   │   └── wolt_dateindex.py
│   ├── core.py          # Main crawler orchestration
│   ├── db.py            # Database operations
│   ├── parsers.py       # XML parsing logic
│   ├── download.py      # File download utilities
│   └── ...
├── data/
│   └── retailers.json   # Retailer configuration
├── scripts/             # Utility scripts
├── run_crawler.py       # Entry point for GitHub Actions
└── requirements.txt     # Python dependencies
```
