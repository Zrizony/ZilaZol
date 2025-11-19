# Crawler Diagnosis & Action Plan

## Most Likely Root Causes (Ranked by Probability & Impact)

### 1. **CRITICAL: Daemon Thread Killing Container** (Primary cause of SIGKILLs)

**Problem**: In `app.py` line 143, the crawler thread is created with `daemon=True`. When Cloud Run receives the 200 response, it considers the request complete. If there are no active HTTP connections, Cloud Run may terminate the container. Daemon threads don't keep the process alive, so the container gets SIGKILLed mid-crawl.

**Evidence**:
- SIGKILL messages appear after `/run` returns 200
- No explicit OOM messages (memory is fine)
- Manifest upload fails (container killed before completion)
- Multiple worker PIDs killed (Gunicorn workers restarting)

**Impact**: HIGH - This explains all three symptoms (SIGKILLs, manifest failures, and potentially incomplete crawls)

### 2. **HIGH: Link Discovery Failure - Frame Scanning Order**

**Problem**: 
- `bina_adapter()` calls `collect_links_on_page()` FIRST (line 361), which only checks the main frame
- `collect_links_on_page()` in `generic.py` doesn't scan child frames at all
- Bina sites put links in iframes, so main-frame-only scan returns 0 links
- Fallback to `bina_collect_links()` happens, but by then the page state may have changed
- `bina_collect_gz_links()` has a bug: it waits for selector in main frame (line 90) which times out, then tries frames - but the timeout exception prevents proper frame scanning

**Evidence**:
- All retailers show `links=0` even though links exist in iframes
- `bina_collect_gz_links()` exists but isn't being called effectively
- Generic adapter never checks frames

**Impact**: HIGH - Explains why all retailers report `links=0`

### 3. **MEDIUM: Manifest Upload Not Resilient**

**Problem**:
- Manifest upload happens synchronously at the very end (line 216 in `core.py`)
- Uses blocking `upload_from_string()` with no retry logic
- If container is killed (issue #1), manifest upload fails with `HTTPSConnectionPool` errors
- No timeout handling for GCS upload

**Impact**: MEDIUM - Symptom of issue #1, but should be hardened

### 4. **LOW: Concurrency May Accumulate Memory**

**Problem**:
- Each retailer creates a NEW Playwright instance (`async with async_playwright()` in `crawl_retailer`)
- With concurrency=3, that's 3 browsers × ~1-2GB = 3-6GB base
- Chromium processes can accumulate memory over time
- However, local psutil shows we're well below 16Gi, so this is likely not the primary issue

**Impact**: LOW - Memory instrumentation will confirm, but unlikely to be root cause

---

## Action Plan (Prioritized)

### **PRIORITY 1: Fix Daemon Thread Issue** ⚠️ CRITICAL

**File**: `app.py`

**Change**: Make the background thread non-daemon AND ensure Flask/Gunicorn keeps the process alive

**Implementation**:
```python
# Line 140-146: Change daemon=True to daemon=False
thread = threading.Thread(
    target=_run_crawler_background,
    args=(retailers, group or "all"),
    daemon=False,  # Changed from True - keeps process alive
    name=f"crawler-{group or 'all'}"
)
thread.start()

# Add logging to track thread lifecycle
logger.info("marker.run.accepted group=%s retailers=%d thread=%s daemon=%s", 
           group or "all", len(retailers), thread.name, thread.daemon)
```

**Why this works**: Non-daemon threads keep the process alive. Cloud Run will keep the container running as long as there are active threads. The HTTP request returns 200 immediately, but the thread continues running.

**Alternative if needed**: If Cloud Run still kills containers, we may need to add a health check endpoint that the crawler pings periodically, or use Cloud Tasks/Cloud Run Jobs instead of background threads.

---

### **PRIORITY 2: Fix Bina Link Discovery** ⚠️ HIGH

**File**: `crawler/adapters/bina.py`

**Change**: Fix the link collection order and frame scanning logic

**Implementation**:

1. **Fix `bina_collect_gz_links()` to not wait for selector in main frame first**:
```python
async def bina_collect_gz_links(page: Page) -> List[str]:
    """
    Collect all .gz (and .zip) links from ALL frames (main + child frames).
    Returns absolute URLs.
    """
    selector = "a[href$='.gz'], a[href*='.gz'], a[href$='.zip'], a[href*='.zip']"
    hrefs: Set[str] = set()
    
    # Scan ALL frames without waiting for selector (non-blocking)
    for frame in page.frames:
        try:
            # Use locator.count() which doesn't throw if selector doesn't exist
            count = await frame.locator(selector).count()
            if count == 0:
                continue
            
            # Extract links from this frame
            vals = await frame.eval_on_selector_all(selector, "els => els.map(a => a.href)")
            for h in vals or []:
                if h:
                    hrefs.add(h)
        except Exception as e:
            logger.debug("bina.frame_scan_error frame=%s error=%s", frame.url, str(e))
            continue
    
    if not hrefs:
        return []
    
    # Normalize URLs...
    # (rest of function unchanged)
```

2. **Change `bina_adapter()` to call Bina-specific collection FIRST**:
```python
# Line 359-364: Change order - use bina_collect_links FIRST, not collect_links_on_page
log_memory(logger, f"bina.before_collect_links retailer={retailer_id}")

# Try Bina-specific collection first (handles frames properly)
links = await bina_collect_links(page, retailer_id)

# Fallback to generic collection only if Bina-specific found nothing
if not links:
    links = await collect_links_on_page(page, source.get("download_patterns") or source.get("patterns"))

log_memory(logger, f"bina.after_collect_links retailer={retailer_id} count={len(links)}")
```

3. **Add better logging to diagnose frame issues**:
```python
# In bina_collect_links(), add frame count logging:
logger.info("bina.frame_scan retailer=%s total_frames=%d", retailer_id, len(page.frames))
for i, frame in enumerate(page.frames):
    logger.debug("bina.frame[%d] url=%s name=%s", i, frame.url, frame.name)
```

---

### **PRIORITY 3: Fix Generic Adapter Frame Scanning** ⚠️ HIGH

**File**: `crawler/adapters/generic.py`

**Change**: Make `collect_links_on_page()` scan child frames

**Implementation**:
```python
async def collect_links_on_page(page: Page, patterns: Optional[List[str]] = None) -> List[str]:
    """Collect download links from main frame AND all child frames."""
    # ... existing selector building code ...
    
    hrefs = set()
    
    # Scan ALL frames (main + child frames)
    for frame in page.frames:
        for sel in selectors:
            try:
                count = await frame.locator(sel).count()
                if count == 0:
                    continue
                
                vals = await frame.eval_on_selector_all(sel, "els => els.map(a => a.href)")
                for h in (vals or []):
                    if h and (looks_like_price_file(h) or h.lower().endswith(tuple(pat))):
                        hrefs.add(h)
            except Exception:
                continue
    
    return sorted(hrefs)
```

---

### **PRIORITY 4: Harden Manifest Upload** ⚠️ MEDIUM

**File**: `crawler/core.py` and `crawler/gcs.py`

**Change**: Add retry logic and make upload non-blocking

**Implementation**:

1. **Add retry wrapper to `upload_to_gcs()`**:
```python
# In crawler/gcs.py
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def upload_to_gcs(...):
    """Upload with retry logic."""
    blob = bucket.blob(blob_path)
    # Use async-compatible upload
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: blob.upload_from_string(data, content_type=content_type))
    # ... rest unchanged
```

**Note**: This requires `tenacity` package. Alternatively, implement simple retry loop:
```python
async def upload_to_gcs(...):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            blob = bucket.blob(blob_path)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: blob.upload_from_string(data, content_type=content_type))
            # ... metadata setting ...
            return
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning("gcs.upload.retry attempt=%d/%d error=%s", attempt+1, max_retries, str(e))
            await asyncio.sleep(2 ** attempt)
```

2. **Add timeout protection in `run_all()`**:
```python
# Line 216: Wrap manifest upload with timeout
try:
    bucket = get_bucket()
    if bucket:
        manifest_key = f"manifests/{run_id}.json"
        manifest_data = json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8')
        
        # Upload with 30s timeout
        await asyncio.wait_for(
            upload_to_gcs(bucket, manifest_key, manifest_data, content_type="application/json"),
            timeout=30.0
        )
        logger.info("run.manifest bucket=%s key=%s retailers=%d", BUCKET, manifest_key, len(manifest_retailers))
except asyncio.TimeoutError:
    logger.error("run.manifest.timeout key=%s", manifest_key)
except Exception as e:
    logger.error("run.manifest.failed error=%s", str(e))
```

---

### **PRIORITY 5: Add Diagnostic Logging** ⚠️ LOW (but helpful)

**Files**: Multiple

**Add logging to verify fixes**:

1. **Thread lifecycle tracking** (`app.py`):
```python
# In _run_crawler_background():
logger.info("background.crawler.thread_start thread_id=%s", threading.current_thread().ident)
# ... existing code ...
logger.info("background.crawler.thread_end thread_id=%s", threading.current_thread().ident)
```

2. **Frame discovery logging** (`bina.py`):
```python
# In bina_adapter(), after page.goto():
logger.info("bina.page_loaded retailer=%s url=%s frames=%d", 
           retailer_id, page.url, len(page.frames))
for i, frame in enumerate(page.frames):
    logger.debug("bina.frame[%d] url=%s name=%s", i, frame.url or "N/A", frame.name or "N/A")
```

3. **Memory tracking around critical sections**:
```python
# Already added, but verify it's working:
# - Before/after each retailer
# - Before/after link collection
# - Before/after downloads
```

---

## Implementation Order

1. **Fix daemon thread** (Priority 1) - This will likely resolve SIGKILLs immediately
2. **Fix Bina link discovery** (Priority 2) - This will fix `links=0` issue
3. **Fix generic adapter frames** (Priority 3) - Ensures all adapters work
4. **Harden manifest upload** (Priority 4) - Prevents failures even if container is killed
5. **Add diagnostics** (Priority 5) - Helps verify fixes and catch future issues

---

## Verification Steps

After implementing Priority 1-3:

1. **Check logs for thread lifecycle**:
   ```bash
   gcloud logging read 'resource.type=cloud_run_revision 
     AND textPayload=~"background.crawler"' \
     --limit=50
   ```
   Should see `thread_start` and `thread_end` without SIGKILL in between.

2. **Check for link discovery**:
   ```bash
   gcloud logging read 'textPayload=~"links.discovered"' \
     --limit=20
   ```
   Should see `count > 0` for Bina retailers.

3. **Check memory stats**:
   ```bash
   gcloud logging read 'textPayload=~"mem.stats"' \
     --limit=100 | grep "rss_mb"
   ```
   Verify memory stays well below 16Gi.

4. **Check manifest success**:
   ```bash
   gcloud logging read 'textPayload=~"run.manifest"' \
     --limit=10
   ```
   Should see `run.manifest bucket=...` success messages, not failures.

---

## Additional Considerations

### If daemon=False doesn't solve SIGKILLs:

Cloud Run may have a "request timeout" separate from the container timeout. Consider:

1. **Add periodic health pings** from the crawler thread to keep connection alive
2. **Use Cloud Run Jobs** instead of HTTP-triggered containers (better for long-running tasks)
3. **Use Cloud Tasks** to queue crawler work (more resilient)

### If frame scanning still fails:

1. Add explicit waits for iframe loading:
   ```python
   await page.wait_for_load_state("networkidle")
   await page.wait_for_timeout(2000)  # Wait for iframes to render
   ```

2. Try navigating into frames explicitly:
   ```python
   frame = await page.frame(url="*Main.aspx*")
   if frame:
       links = await frame.eval_on_selector_all(...)
   ```

3. Add screenshot on failure to debug:
   ```python
   if not links:
       await page.screenshot(path=f"debug_{retailer_id}_no_links.png")
   ```

---

## Expected Outcomes

After implementing Priority 1-3:
- ✅ No more SIGKILL messages (container stays alive)
- ✅ Retailers report `links > 0` (frame scanning works)
- ✅ Manifest uploads succeed (container completes crawl)
- ✅ Memory stays below 16Gi (confirmed via instrumentation)

After Priority 4-5:
- ✅ Manifest uploads resilient to transient failures
- ✅ Better diagnostics for future debugging

