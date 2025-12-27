# Supers Price Crawler

## What it does
- Crawls Israeli retailers for price data using Playwright automation
- Supports multiple retailer types: PublishedPrices (Cerberus), Bina Projects, and generic public sites
- Downloads files (ZIP/GZ/XML) and uploads to Google Cloud Storage
- Parses XML files to JSONL format for analysis
- Provides HTTP API for triggering crawls

## Features
- **Multi-adapter architecture** for different retailer types
- **Authentication support** for protected sites (PublishedPrices)
- **Folder navigation** for retailers like Super Yuda
- **Duplicate detection** using MD5 hashes
- **Manifest generation** with file metadata
- **Structured logging** for Cloud Run compatibility

## HTTP Endpoints
- `POST /run` - Trigger crawler (async, returns immediately)
  - Returns `200 OK` instantly with `{"status": "accepted", ...}`
  - Starts crawler in background thread
  - Designed for Cloud Scheduler (no timeout/503 errors)
- `GET /health` - Health check
- `GET /version` - Version info (legacy)
- `GET /__version` - Active version (RELEASE/COMMIT_SHA)
- `GET /__env` - Environment variables (non-sensitive)
- `POST /__smoke` - GCS smoke test (uploads test file to bucket)
- `GET /retailers` - Debug retailer discovery

### /run Endpoint Behavior

**Important**: The `/run` endpoint is **async-safe** and returns immediately:

1. Client (Cloud Scheduler) calls `/run?group=public`
2. Endpoint validates config and starts crawler in background thread
3. Returns `200 OK` **immediately** (within milliseconds)
4. Crawler runs in background, uploads to GCS, logs to Cloud Run
5. Client is not blocked waiting for crawl to complete

**Response format**:
```json
{
  "status": "accepted",
  "message": "Crawler started in background",
  "group": "public",
  "retailers_count": 15
}
```

**Why this matters**:
- Cloud Scheduler won't timeout or return 503 errors
- Cold starts don't cause request failures
- Multiple scheduler jobs can run concurrently without blocking each other

## Environment Variables

### Required
- `GCS_BUCKET` - Google Cloud Storage bucket name (preferred)
- `RETAILER_CREDS_JSON` - JSON object with retailer credentials

### Optional
- `PRICES_BUCKET` or `BUCKET_NAME` - Fallback bucket names
- `LOG_LEVEL` - Logging level (default: INFO)
- `DATABASE_URL` - PostgreSQL connection string (saves parsed data to database)

## Configuration

### Bucket Configuration
The crawler accepts bucket configuration in order of preference:
1. `GCS_BUCKET` (preferred)
2. `PRICES_BUCKET` (legacy)
3. `BUCKET_NAME` (legacy)

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
export GCS_BUCKET=your-test-bucket
export RETAILER_CREDS_JSON='{"cerberus":{"username":"USER","password":"PASS"}}'
export LOG_LEVEL=INFO

# Install dependencies
pip install -r requirements.txt

# Run the application
python app.py
# or with gunicorn
gunicorn app:app --bind 0.0.0.0:8080
```

### Trigger Crawler
```bash
# Run all enabled retailers
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json"

# Run specific retailer
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{"retailer":"superyuda"}'

# Dry run (no actual crawling)
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{"dry_run":true}'
```

### Group-Based Crawling

The `/run` endpoint supports a `group` query parameter to crawl only specific subsets of retailers:

- **`/run?group=creds`** - Crawls only retailers that require credentials (PublishedPrices retailers with `tenantKey` or custom `creds_key`)
- **`/run?group=public`** - Crawls only retailers that don't require credentials (public sites, Bina Projects, etc.)
- **`/run`** (no group parameter) - Crawls all enabled retailers (default behavior)

This allows splitting crawls into separate Cloud Scheduler jobs for better control and cost optimization:

```bash
# Crawl only credentialed retailers (PublishedPrices, etc.)
curl -X POST "http://localhost:8080/run?group=creds" \
  -H "Content-Type: application/json"

# Crawl only public retailers (Bina, generic sites, Wolt)
curl -X POST "http://localhost:8080/run?group=public" \
  -H "Content-Type: application/json"
```

### Single Retailer Debugging

For local debugging and testing, use the `slug` query parameter to target a single retailer:

```bash
# Debug a single retailer by slug
curl -X POST "http://localhost:8080/run?slug=supercofix" \
  -H "Content-Type: application/json"

# Combine with group filter
curl -X POST "http://localhost:8080/run?group=creds&slug=supercofix" \
  -H "Content-Type: application/json"
```

The manifest will contain only the specified retailer, making it easy to diagnose issues.

### Manifest Analysis

Use the manifest summarizer to quickly identify which retailers have issues:

```bash
python scripts/summarize_manifest.py manifests/20241201T123456Z-abc.json
```

This outputs a table showing downloads, reasons for failures, and errors grouped by type.

## Deployment

### Automatic Deployment (GitHub → Cloud Build → Cloud Run)

Each commit to `main` triggers a Cloud Build that:
1. Builds Docker image tagged with `$COMMIT_SHA`
2. Pushes to `gcr.io/$PROJECT_ID/supers:$COMMIT_SHA`
3. Deploys to Cloud Run with 100% traffic
4. Sets `RELEASE=$COMMIT_SHA` and `GCS_BUCKET=civic-ripsaw-466109-e2-crawler-data`

### Manual Deployment

Deploy manually with a unique tag:

```bash
chmod +x scripts/*.sh
./scripts/deploy.sh
```

This will:
- Build and push image with timestamp tag
- Deploy to Cloud Run service `price-crawler` in `me-west1`
- Set environment variables automatically

### Verification

Verify the deployed service:

```bash
./scripts/verify.sh
```

This checks:
- `/__version` → Returns the release/tag you just deployed
- `/__env` → Shows `GCS_BUCKET=civic-ripsaw-466109-e2-crawler-data`
- `/__smoke` → Returns `ok:true` and creates a GCS object at `smoke/<version>/...`

**Expected output:**
- `/__version` shows the deployed tag
- `/__env` shows `GCS_BUCKET` set correctly
- `/__smoke` returns `{"ok": true, "bucket": "...", "key": "smoke/.../..."}`

**Cloud Run Logs** should show:
- `startup version=<tag>`
- `bucket.config=civic-ripsaw-466109-e2-crawler-data`
- `smoke.uploaded bucket=... key=...`

### Cloud Scheduler Configuration

**Cloud Run Configuration**:
- **Region**: `me-west1`
- **Memory**: `16Gi` (required to prevent OOM kills)
- **CPU**: `4 vCPU` (supports concurrent Playwright browsers and parsing)
- **Timeout**: `3600s` (1 hour)
- **Concurrency**: 3 retailers crawled simultaneously (prevents memory exhaustion)

**Cloud Scheduler**:
- **Region**: Can be any region (cross-region calls are supported)
- Cloud Scheduler may be in `europe-west1` or `me-west1` - both work fine
- The `/run` endpoint returns immediately, so Scheduler doesn't wait for crawl completion

#### Why 16Gi Memory?

The crawler uses Playwright browsers which are memory-intensive:
- Each browser instance: ~1-2 GB RAM
- With 3 concurrent crawlers: ~3-6 GB
- Peak usage with parsing/uploads: ~8-12 GB
- **16Gi provides safe headroom** to prevent OOM container kills

**Previous issue**: Running with 8Gi caused frequent OOM kills (`Container terminated on signal 9`), which caused 503 errors to Cloud Scheduler.

**Memory Instrumentation**: The crawler now includes memory usage logging at key checkpoints:
- Run start/end
- Before/after each retailer
- Before/after link collection
- Before/after downloads

Check logs for `mem.stats` entries to monitor actual memory usage and optimize resource allocation.

#### Concurrency Limiting

The crawler uses `asyncio.Semaphore(3)` to limit concurrent retailers:
- Maximum 3 Playwright browsers running simultaneously
- Prevents memory spikes from crawling 30+ retailers in parallel
- Each retailer waits for a semaphore slot before starting

#### Testing Scheduler Jobs

```bash
# Get the service URL
SERVICE_URL=$(gcloud run services describe price-crawler \
  --region me-west1 \
  --format='value(status.url)')

# Test the /run endpoint manually
curl -X POST "${SERVICE_URL}/run?group=public" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)"

# Should return immediately with:
# {"status": "accepted", "message": "Crawler started in background", ...}
```

#### Troubleshooting 503 Errors

If you see `URL_UNREACHABLE_UNREACHABLE_5xx` or 503 errors:

1. **Check for OOM kills in Cloud Run logs**:
   ```bash
   gcloud logging read 'resource.type=cloud_run_revision 
     AND resource.labels.service_name=price-crawler 
     AND textPayload=~"Memory limit"' \
     --limit=10 \
     --format=json
   ```
   
   If you see `Memory limit of X MiB exceeded` → Container is OOM-killed → Increase memory

2. **Check Cloud Run resource configuration**:
   ```bash
   gcloud run services describe price-crawler \
     --region me-west1 \
     --format='value(spec.template.spec.containers[0].resources.limits)'
   ```
   
   Should show: `memory: 16Gi, cpu: "4"`

3. **Verify /run endpoint responds quickly**:
   ```bash
   curl -X POST "${SERVICE_URL}/run?group=public" \
     -H "Authorization: Bearer $(gcloud auth print-identity-token)"
   
   # Should return in < 1 second with:
   # {"status": "accepted", "message": "Crawler started in background", ...}
   ```

4. **Check Cloud Run logs** for `background.crawler.start` and `background.crawler.done` messages:
   ```bash
   gcloud logging read 'resource.type=cloud_run_revision 
     AND resource.labels.service_name=price-crawler 
     AND textPayload=~"background.crawler"' \
     --limit=20 \
     --format=json
   ```

## GCS Layout

Files are uploaded with the following structure:
```
raw/
  <retailer_id>/
    <run_id>/
      <filename>.zip
      <filename>.xml
      manifest.json
```

### Manifest Format
Each run generates a `manifest.json` with file metadata:
```json
{
  "run_id": "20241201T143022Z-abc12345",
  "retailer_id": "superyuda",
  "retailer_name": "סופר יודה",
  "timestamp": "2024-12-01T14:30:22Z",
  "files": [
    {
      "filename": "prices.xml",
      "gcs_path": "raw/superyuda/20241201T143022Z-abc12345/prices.xml",
      "md5_hex": "a1b2c3d4e5f6...",
      "bytes": 1024000,
      "ts": "2024-12-01T14:30:22Z"
    }
  ]
}
```

## Special Cases

### Super Yuda Folder Navigation
Super Yuda requires navigation to the "Yuda" folder after login. The crawler:
1. Logs into PublishedPrices
2. Navigates to the "Yuda" folder
3. Collects and downloads files from that folder

### Duplicate Detection
- Files are deduplicated by MD5 hash within each run
- Each uploaded blob has `md5_hex` metadata set
- Duplicate files are skipped and counted

## Logging

The crawler uses structured logging suitable for Cloud Run:
```
2024-12-01T14:30:22Z INFO run.start run_id=20241201T143022Z-abc12345 retailers=5
2024-12-01T14:30:22Z INFO mem.stats rss_mb=850.4 vms_mb=3200.0 note=run.start run_id=20241201T143022Z-abc12345
2024-12-01T14:30:23Z INFO login.start retailer=publishedprices
2024-12-01T14:30:25Z INFO login.success retailer=publishedprices
2024-12-01T14:30:26Z INFO folder.navigate retailer=publishedprices folder=Yuda
2024-12-01T14:30:28Z INFO folder.navigate.success retailer=publishedprices folder=Yuda method=direct
2024-12-01T14:30:30Z INFO upload.ok retailer=superyuda file=prices.xml gcs_path=raw/superyuda/20241201T143022Z-abc12345/prices.xml
2024-12-01T14:30:32Z INFO manifest.written retailer=superyuda run_id=20241201T143022Z-abc12345 files=3
```

### Memory Monitoring

The crawler includes memory usage instrumentation using `psutil` to track RAM consumption:

**Memory log format**:
```
mem.stats rss_mb=1470.2 vms_mb=4000.0 note=before_retailer id=shufersal
```

- `rss_mb`: Resident Set Size (actual RAM used) in MiB
- `vms_mb`: Virtual Memory Size in MiB
- `note`: Context describing when the measurement was taken

**Checkpoints logged**:
- Run start/end (`run.start`, `run_all.done_before_manifest`)
- Before/after each retailer (`before_retailer`, `after_retailer`)
- Link collection phases (`bina.before_collect_links`, `generic.after_collect_links`)
- Download phases (`bina.before_downloads`, `generic.after_downloads`)

**Query memory logs**:
```bash
# Get all memory stats from recent runs
gcloud logging read 'resource.type=cloud_run_revision 
  AND resource.labels.service_name=price-crawler 
  AND textPayload=~"mem.stats"' \
  --limit=100 \
  --format=json | jq -r '.[] | .textPayload' | grep "mem.stats"

# Find peak memory usage
gcloud logging read 'resource.type=cloud_run_revision 
  AND resource.labels.service_name=price-crawler 
  AND textPayload=~"mem.stats"' \
  --limit=1000 \
  --format=json | jq -r '.[] | .textPayload' | \
  grep "mem.stats" | grep -oP 'rss_mb=\K[0-9.]+' | sort -n | tail -1
```

Use these logs to:
- Verify actual memory usage vs. allocated resources
- Identify memory leaks or growth patterns
- Optimize Cloud Run memory allocation (reduce if consistently low, increase if hitting limits)