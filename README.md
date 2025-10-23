# ZilaZol Price Crawler

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
- `POST /run` - Trigger crawler (supports JSON payload)
- `GET /health` - Health check
- `GET /version` - Version info
- `GET /retailers` - Debug retailer discovery

## Environment Variables

### Required
- `GCS_BUCKET` - Google Cloud Storage bucket name (preferred)
- `RETAILER_CREDS_JSON` - JSON object with retailer credentials

### Optional
- `PRICES_BUCKET` or `BUCKET_NAME` - Fallback bucket names
- `LOG_LEVEL` - Logging level (default: INFO)

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
2024-12-01T14:30:23Z INFO login.start retailer=publishedprices
2024-12-01T14:30:25Z INFO login.success retailer=publishedprices
2024-12-01T14:30:26Z INFO folder.navigate retailer=publishedprices folder=Yuda
2024-12-01T14:30:28Z INFO folder.navigate.success retailer=publishedprices folder=Yuda method=direct
2024-12-01T14:30:30Z INFO upload.ok retailer=superyuda file=prices.xml gcs_path=raw/superyuda/20241201T143022Z-abc12345/prices.xml
2024-12-01T14:30:32Z INFO manifest.written retailer=superyuda run_id=20241201T143022Z-abc12345 files=3
```