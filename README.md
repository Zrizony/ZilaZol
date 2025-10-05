# ZilaZol Price Crawler (from scratch)

## What it does
- Scrapes gov.il for retailers (“לצפייה במחירים”).
- Fans out with Playwright + aiohttp to fetch files (ZIP/GZ/XML).
- Uploads to GCS at `raw/<retailer_key>/<run_id>/<filename>`, writes `manifest.json`.
- Skips duplicates by MD5 (stored in object metadata).
- HTTP:
  - `POST /run` (Scheduler target)
  - `GET  /retailers` (debug discovery)
  - `GET  /healthz`

## Configure
Set these env vars in Cloud Run:
- `GCS_BUCKET` = your bucket
- `RETAILER_CREDS_JSON` = JSON mapping retailer keys → creds, e.g.:

```json
{
  "אלמשהדאוי_קינג_סטור_בעמ": {"username": "USER", "password": "PASS"},
  "נתיב_החסד_סופר_חסד_בעמ": {"username": "USER", "password": "PASS"}
}
