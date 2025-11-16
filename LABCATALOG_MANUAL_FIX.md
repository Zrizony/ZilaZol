# Labcatalog Retailers - Manual URL Discovery Required

## Problem

Two retailers (`victorymarket` and `chokent`) are currently disabled because they point to the labcatalog homepage (`https://laibcatalog.co.il/`) instead of their retailer-specific pages.

## Status

- **victorymarket** (ויקטורי רשת סופרמרקטים): Disabled - URL needs verification
- **chokent** (ח. כהן סוכנות מזון ומשקאות): Disabled - URL needs verification

## Steps to Fix

### 1. Manual URL Discovery

Visit `https://laibcatalog.co.il/` in a browser and:

1. Look for links or navigation to retailer-specific sections
2. Check for subdirectories like:
   - `/victory/` or `/victorymarket/`
   - `/chokent/` or `/cohen/`
   - Any retailer-specific landing pages

3. Inspect the page source for hidden links or API endpoints

4. Check robots.txt: `https://laibcatalog.co.il/robots.txt`

5. Try common URL patterns:
   ```
   https://laibcatalog.co.il/victory/
   https://laibcatalog.co.il/victorymarket/
   https://laibcatalog.co.il/chokent/
   https://laibcatalog.co.il/cohen/
   https://laibcatalog.co.il/prices/victory/
   https://laibcatalog.co.il/prices/chokent/
   ```

### 2. Verify File Availability

Once you find a potential URL, verify it has downloadable price files:

```bash
curl -X POST "http://localhost:8080/run?slug=victorymarket" \
  -H "Content-Type: application/json"
```

Check the manifest for:
- `links > 0`
- `downloads > 0`
- No `no_dom_links` reason

### 3. Update Configuration

Edit `data/retailers.json`:

```json
{
  "id": "victorymarket",
  "name": "ויקטורי רשת סופרמרקטים בע\"מ",
  "url": "https://laibcatalog.co.il/CORRECT_PATH_HERE/",
  "host": "laibcatalog.co.il",
  "enabled": true,
  "tags": ["public", "no-login", "labcatalog"],
  "download_patterns": [".xml", ".gz", ".zip"]
}
```

Remove the `disabled_reason` and `notes` fields once fixed.

### 4. Test and Verify

```bash
# Test locally
python scripts/summarize_manifest.py manifests/latest.json

# Deploy and test in production
curl -X POST "https://your-service-url/run?group=public" \
  -H "Content-Type: application/json"
```

## Alternative: Contact Labcatalog

If manual discovery fails:

1. Contact labcatalog.co.il support
2. Ask for the correct URLs for Victory Market and Chokent price files
3. Request API documentation if available

## Notes

- Labcatalog appears to be a shared platform for multiple retailers
- Each retailer likely has a dedicated subdirectory or query parameter
- The generic adapter should work once the correct URL is configured
- Consider creating a specialized `labcatalog` adapter if multiple retailers use this platform

## Current Adapter

These retailers use the `generic` adapter which:
- Navigates to the URL
- Collects all `.gz` and `.zip` links from the page
- Downloads and processes files

This should work fine once the correct URL is configured.

