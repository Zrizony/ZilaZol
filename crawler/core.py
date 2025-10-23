# crawler/core.py
from __future__ import annotations
import asyncio
import io
import json
import os
import re
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import uuid
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

import aiofiles
import gzip
import zipfile
from playwright.async_api import async_playwright, Page
from google.cloud import storage

from . import logger

# ----------------------------
# Data Classes
# ----------------------------

@dataclass
class RetailerResult:
    retailer_id: str
    source_url: str
    errors: List[str]
    adapter: str
    links_found: int = 0
    files_downloaded: int = 0
    skipped_dupes: int = 0
    xml: int = 0
    gz: int = 0
    zips: int = 0
    subpath: Optional[str] = None
    
    def as_dict(self):
        d = asdict(self)
        d["ts"] = datetime.now(timezone.utc).isoformat()
        return d

# ----------------------------
# Settings / Constants
# ----------------------------

# Unified bucket environment variable handling
BUCKET = (
    os.getenv("GCS_BUCKET")
    or os.getenv("PRICES_BUCKET")
    or os.getenv("BUCKET_NAME")
)
if not BUCKET:
    raise RuntimeError("No bucket configured. Set GCS_BUCKET (preferred) or PRICES_BUCKET/BUCKET_NAME.")
LOCAL_DOWNLOAD_DIR = os.getenv("LOCAL_DOWNLOAD_DIR", "downloads")
LOCAL_JSON_DIR = os.getenv("LOCAL_JSON_DIR", "json_out")
SCREENSHOTS_DIR = os.getenv("SCREENSHOTS_DIR", "screenshots")
MAX_LOGIN_RETRIES = int(os.getenv("MAX_RETRIES_LOGIN", "3"))


# Load credentials from environment variable
RAW_CREDS = os.getenv("RETAILER_CREDS_JSON", "{}")
try:
    CREDS = json.loads(RAW_CREDS)
except Exception as e:
    raise RuntimeError(f"Invalid RETAILER_CREDS_JSON: {e}")

PUBLISHED_HOST = "url.publishedprices.co.il"
DOWNLOAD_SUFFIXES = (".xml", ".gz", ".zip")

# ----------------------------
# Data Models
# ----------------------------

@dataclass
class RetailerResult:
    retailer_id: str
    source_url: str
    files_downloaded: int = 0
    xml_parsed_rows: int = 0
    zips: int = 0
    gz: int = 0
    skipped_dupes: int = 0
    errors: List[str] = None
    adapter: str = "unknown"
    subpath: Optional[str] = None

    def as_dict(self):
        d = asdict(self)
        d["ts"] = datetime.now(timezone.utc).isoformat()
        return d

# ----------------------------
# Helpers
# ----------------------------


def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-.]+", "_", s).strip("_")[:120]

def md5_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()

def sniff_file_type(data: bytes, fname: str) -> str:
    """Returns 'xml' | 'gz' | 'zip' | 'unknown'"""
    lower = fname.lower()
    if data.startswith(b"\x1f\x8b"):  # gzip
        return "gz"
    if data.startswith(b"PK"):        # zip
        return "zip"
    if lower.endswith(".xml"):
        return "xml"
    if lower.endswith(".gz"):
        # mislabelled gz that is actually zip
        if data.startswith(b"PK"):
            return "zip"
        return "gz"
    if lower.endswith(".zip"):
        return "zip"
    return "unknown"


def ensure_dirs(*paths: str):
    for p in paths:
        os.makedirs(p, exist_ok=True)


# ----------------------------
# GCS
# ----------------------------

def get_bucket() -> Optional[storage.Bucket]:
    if not BUCKET:
        return None
    client = storage.Client()
    return client.bucket(BUCKET)

async def upload_to_gcs(
    bucket: storage.Bucket,
    blob_path: str,
    data: bytes,
    content_type: str = "application/octet-stream",
    md5_hex: str = None,
):
    blob = bucket.blob(blob_path)
    blob.upload_from_string(data, content_type=content_type)
    
    # Set MD5 metadata if provided
    if md5_hex:
        blob.metadata = {"md5_hex": md5_hex}
        blob.patch()

# ----------------------------
# XML → JSONL parser
# ----------------------------

from lxml import etree

def _first_text(elem, *paths) -> Optional[str]:
    for p in paths:
        r = elem.find(p)
        if r is not None and (t := (r.text or "").strip()):
            return t
    return None

def parse_prices_xml(xml_bytes: bytes, company: str) -> List[dict]:
    rows: List[dict] = []
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return rows

    items = root.findall(".//Item")
    if not items:
        items = list(root)

    for it in items:
        name = _first_text(
            it,
            "ItemName",
            "ManufacturerItemDescription",
            "Description",
            "itemname",
            "name",
        )
        barcode = _first_text(
            it, "ItemCode", "Barcode", "ItemBarcode", "itemcode", "barcode", "Code"
        )
        price = _first_text(it, "ItemPrice", "Price", "price")
        date = _first_text(
            it, "PriceUpdateDate", "UpdateDate", "LastUpdateDate", "date"
        )

        if not (barcode or name or price):
            continue

        rows.append(
            {
                "name": name,
                "barcode": barcode,
                "date": date,
                "price": price,
                "company": company,
            }
        )
    return rows

# ----------------------------
# Playwright Actions
# ----------------------------

async def new_context(pw):
    browser = await pw.chromium.launch(
        headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
    )
    ctx = await browser.new_context(locale="he-IL")
    return browser, ctx

async def screenshot_after_login(page: Page, display_name: str):
    ensure_dirs(SCREENSHOTS_DIR)
    fname = f"{safe_name(display_name)}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.png"
    await page.screenshot(path=os.path.join(SCREENSHOTS_DIR, fname), full_page=True)

# ----------------------------
# PublishedPrices Adapter
# ----------------------------

async def crawl_publishedprices(page: Page, retailer: dict, creds: dict, run_id: str) -> RetailerResult:
    """
    Main entrypoint for publishedprices adapter
    retailer: dict from retailers.json (has name, url, adapter='publishedprices', cred_key, optional 'folder')
    creds: {'username': ..., 'password': ...}
    """
    retailer_id = retailer.get("id", "unknown")
    retailer_name = retailer.get("name", "Unknown")
    
    result = RetailerResult(
        retailer_id=retailer_id,
        source_url=retailer.get("url", ""),
        errors=[],
        adapter="publishedprices"
    )
    
    logger.info("publishedprices: retailer=%s", retailer_name)
    
    try:
        # Step 1: Login
        await publishedprices_login(page, creds["username"], creds.get("password", ""))
        logger.info("publishedprices: logged_in=True")
        
        # Step 2: Handle folder navigation (Super Yuda special case)
        folder = retailer.get("folder")
        if folder:
            await publishedprices_navigate_to_folder(page, folder)
            result.subpath = folder
        
        # Step 3: Collect files
        links = await publishedprices_collect_links(page)
        result.links_found = len(links)
        logger.info("publishedprices: links=%d", len(links))
        
        # Step 4: Download and process files
        seen_hashes: Set[str] = set()
        seen_names: Set[str] = set()
        manifest_entries: List[dict] = []
        
        for link in links:
            try:
                # Download file
                data = await fetch_url_bytes(page, link)
                md5_hash = md5_bytes(data)
                
                # Check for duplicates
                if md5_hash in seen_hashes:
                    result.skipped_dupes += 1
                    continue
                
                # Normalize filename for name-based dedupe
                filename = os.path.basename(urlparse(link).path)
                normalized_name = f"{retailer_id}/{filename.lower()}"
                if normalized_name in seen_names:
                    result.skipped_dupes += 1
                    continue
                
                # Add to seen sets
                seen_hashes.add(md5_hash)
                seen_names.add(normalized_name)
                
                # Upload to GCS with new path structure
                bucket = get_bucket()
                if bucket:
                    blob_path = f"raw/{retailer_id}/{run_id}/{filename}"
                    await upload_to_gcs(bucket, blob_path, data, md5_hex=md5_hash)
                    
                    # Add to manifest
                    manifest_entries.append({
                        "filename": filename,
                        "gcs_path": blob_path,
                        "md5_hex": md5_hash,
                        "bytes": len(data),
                        "ts": datetime.now(timezone.utc).isoformat()
                    })
                    
                    logger.info("upload.ok retailer=%s file=%s gcs_path=%s", retailer_id, filename, blob_path)
                
                # Parse XML if applicable
                if filename.lower().endswith(('.xml', '.gz', '.zip')):
                    await _maybe_parse_to_jsonl(retailer_id, filename, data)
                
                # Update counters
                if filename.lower().endswith('.zip'):
                    result.zips += 1
                if filename.lower().endswith('.gz'):
                    result.gz += 1
                result.files_downloaded += 1
                
            except Exception as e:
                result.errors.append(f"download_error:{e}")
                logger.error("upload.failed retailer=%s file=%s err=%s", retailer_id, filename, str(e))
                continue
        
        # Write manifest.json
        if manifest_entries and bucket:
            try:
                manifest_data = json.dumps({
                    "run_id": run_id,
                    "retailer_id": retailer_id,
                    "retailer_name": retailer.get("name", "Unknown"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "files": manifest_entries
                }, indent=2)
                
                manifest_path = f"raw/{retailer_id}/{run_id}/manifest.json"
                await upload_to_gcs(bucket, manifest_path, manifest_data.encode('utf-8'), "application/json")
                logger.info("manifest.written retailer=%s run_id=%s files=%d", retailer_id, run_id, len(manifest_entries))
            except Exception as e:
                logger.error("manifest.failed retailer=%s err=%s", retailer_id, str(e))
                
    except Exception as e:
        result.errors.append(f"fatal:{e}")
        logger.error(f"publishedprices error for {retailer_name}: {e}")
    
    return result

async def publishedprices_login(page: Page, username: str, password: str):
    """Login to publishedprices with robust selector handling and explicit waits"""
    logger.info("login.start retailer=publishedprices")
    
    await page.goto("https://url.publishedprices.co.il/login", wait_until="domcontentloaded", timeout=90000)
    
    # Try username selectors
    username_selectors = ["input[name='username']", "#username", "input[name='Email']", "input[type='email']"]
    for sel in username_selectors:
        if await page.locator(sel).count():
            await page.fill(sel, username)
            break
    
    # Try password selectors
    password_selectors = ["input[name='password']", "#password", "input[type='password']"]
    for sel in password_selectors:
        if password and await page.locator(sel).count():
            await page.fill(sel, password)
            break
    
    # Try submit selectors
    submit_selectors = ["button[type='submit']", "input[type='submit']", "button:has-text('כניסה')", "button:has-text('Login')"]
    for sel in submit_selectors:
        if await page.locator(sel).count():
            await page.click(sel)
            break
    
    # Wait for successful login with explicit UI signal
    try:
        # Wait for URL change or file manager elements
        await page.wait_for_url("**/file**", timeout=25000)
        logger.info("login.success retailer=publishedprices")
    except:
        # Fallback: navigate to file page and wait for file manager
        await page.goto("https://url.publishedprices.co.il/file", wait_until="domcontentloaded", timeout=25000)
        # Wait for file manager to load
        await page.wait_for_selector("table, div#filemanager, div.dataTables_wrapper", timeout=15000)
        logger.info("login.success retailer=publishedprices")

async def publishedprices_navigate_to_folder(page: Page, folder: str):
    """Navigate to specific folder with robust waits and retries"""
    logger.info("folder.navigate retailer=publishedprices folder=%s", folder)
    
    # Wait for file/folder tree to render
    await page.wait_for_selector("table, div#filemanager, div.dataTables_wrapper", timeout=15000)
    await page.wait_for_load_state("networkidle", timeout=10000)
    
    # First try direct navigation
    target_url = f"https://url.publishedprices.co.il/file/cdup/{folder.strip('/')}/"
    try:
        await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(1000)
        
        # Check if we have files listed
        links = await publishedprices_collect_links(page)
        if links:
            logger.info("folder.navigate.success retailer=publishedprices folder=%s method=direct", folder)
            return  # Success, we have files
    except Exception as e:
        logger.warning("folder.navigate.direct_failed retailer=publishedprices folder=%s error=%s", folder, str(e))
    
    # Fallback: go to /file and click the folder
    await page.goto("https://url.publishedprices.co.il/file", wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_selector("table, div#filemanager, div.dataTables_wrapper", timeout=15000)
    await page.wait_for_load_state("networkidle", timeout=10000)
    
    # Try clicking folder by name with retries
    for attempt in range(2):
        try:
            # Try multiple selectors for folder clicking
            folder_selectors = [
                f"a:has-text('{folder}')",
                f"tr:has(td:has-text('{folder}')) a[href]",
                f"td:has-text('{folder}') a",
                f"*:has-text('{folder}'):not(script):not(style)"
            ]
            
            for sel in folder_selectors:
                if await page.locator(sel).count():
                    await page.click(sel)
                    await page.wait_for_timeout(1500)
                    await page.wait_for_load_state("networkidle", timeout=10000)
                    
                    # Verify we're in the folder by checking for files
                    links = await publishedprices_collect_links(page)
                    if links:
                        logger.info("folder.navigate.success retailer=publishedprices folder=%s method=click attempt=%d", folder, attempt + 1)
                        return
                    break
        except Exception as e:
            logger.warning("folder.navigate.click_failed retailer=publishedprices folder=%s attempt=%d error=%s", folder, attempt + 1, str(e))
            if attempt == 0:
                await page.wait_for_timeout(2000)  # Wait before retry
    
    logger.error("folder.not_found retailer=publishedprices folder=%s", folder)

async def publishedprices_collect_links(page: Page) -> List[str]:
    """Collect download links from publishedprices file manager"""
    # Wait for page to load
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(500)
    
    # Get all hrefs
    hrefs = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => a.getAttribute('href'))"
    )
    
    # Normalize to absolute URLs and filter
    links = []
    for h in (hrefs or []):
        if not h:
            continue
        try:
            h_abs = await page.evaluate("u => new URL(u, location.href).href", h)
            low = h_abs.lower()
            if low.endswith((".xml", ".gz", ".zip")) or "download" in low:
                links.append(h_abs)
        except Exception:
            pass
    
    return sorted(set(links))

async def collect_links_on_page(page) -> list[str]:
    """Collect download links with broader filters for generic sites"""
    # Expanded selectors for better link discovery
    selectors = [
        "a[download]",
        "a[href*='download']",
        "a[href*='file']",
        "a[href$='.xml' i]",
        "a[href$='.gz' i]",
        "a[href$='.zip' i]",
        "a[href*='.xml?' i]",
        "a[href*='.gz?' i]",
        "a[href*='.zip?' i]",
    ]
    
    hrefs = set()
    for sel in selectors:
        if await page.locator(sel).count():
            vals = await page.eval_on_selector_all(sel, "els => els.map(a => a.href)")
            for h in (vals or []):
                if h and h.lower().endswith(DOWNLOAD_SUFFIXES):
                    hrefs.add(h)
    
    return sorted(hrefs)

# ----------------------------
# Adapters
# ----------------------------


async def bina_adapter(page: Page, source: dict, retailer_id: str, seen_hashes: Set[str], seen_names: Set[str], run_id: str) -> RetailerResult:
    """Bina projects adapter (no login)"""
    result = RetailerResult(
        retailer_id=retailer_id,
        source_url=source.get("url", ""),
        errors=[],
        adapter="bina"
    )
    
    try:
        # Navigate to page with proper wait conditions
        await page.goto(source.get("url", ""), wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=15000)
        # Additional wait for dynamic content
        await page.wait_for_timeout(2000)
        
        # Collect download links
        links = await collect_links_on_page(page)
        result.links_found = len(links)
        logger.info(f"retailer={retailer_id} source={source.get('url')} adapter=bina links={len(links)}")
        
        # Process each link
        for link in links:
            try:
                data = await fetch_url_bytes(page, link)
                md5_hash = md5_bytes(data)
                
                # Check for duplicates
                if md5_hash in seen_hashes:
                    result.skipped_dupes += 1
                    continue
                
                # Normalize filename for name-based dedupe
                filename = os.path.basename(urlparse(link).path)
                normalized_name = f"{retailer_id}/{filename.lower()}"
                if normalized_name in seen_names:
                    result.skipped_dupes += 1
                    continue
                
                # Add to seen sets
                seen_hashes.add(md5_hash)
                seen_names.add(normalized_name)
                
                # Upload to GCS
                bucket = get_bucket()
                if bucket:
                    blob_path = f"raw/{retailer_id}/{run_id}/{filename}"
                    await upload_to_gcs(bucket, blob_path, data)
                
                # Parse XML if applicable
                if filename.lower().endswith(('.xml', '.gz', '.zip')):
                    await _maybe_parse_to_jsonl(retailer_id, filename, data)
                
                # Update counters
                if filename.lower().endswith('.zip'):
                    result.zips += 1
                if filename.lower().endswith('.gz'):
                    result.gz += 1
                result.files_downloaded += 1
                
            except Exception as e:
                result.errors.append(f"download_error:{e}")
                continue
                
    except Exception as e:
        result.errors.append(f"fatal:{e}")
    
    return result

async def generic_adapter(page: Page, source: dict, retailer_id: str, seen_hashes: Set[str], seen_names: Set[str], run_id: str) -> RetailerResult:
    """Generic HTTP adapter (no login)"""
    result = RetailerResult(
        retailer_id=retailer_id,
        source_url=source.get("url", ""),
        errors=[],
        adapter="generic"
    )
    
    try:
        # Navigate to page with proper wait conditions
        await page.goto(source.get("url", ""), wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=15000)
        # Additional wait for dynamic content
        await page.wait_for_timeout(2000)
        
        # Collect download links with retry logic
        links = await collect_links_on_page(page)
        
        # If no links found, retry with additional wait
        if not links:
            await page.wait_for_load_state("networkidle", timeout=8000)
            await page.wait_for_timeout(800)
            links = await collect_links_on_page(page)
        
        result.links_found = len(links)
        
        # If still no links, take screenshot and log
        if not links:
            ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            fname = f"{retailer_id}_generic_no_links_{ts}.png"
            await page.screenshot(path=os.path.join(SCREENSHOTS_DIR, fname), full_page=True)
            logger.warning(f"[{retailer_id}] No links found at {page.url}. Saved screenshot: {fname}")
        
        logger.info(f"retailer={retailer_id} source={source.get('url')} adapter=generic links={len(links)}")
        
        # Process each link
        for link in links:
            try:
                data = await fetch_url_bytes(page, link)
                md5_hash = md5_bytes(data)
                
                # Check for duplicates
                if md5_hash in seen_hashes:
                    result.skipped_dupes += 1
                    continue
                
                # Normalize filename for name-based dedupe
                filename = os.path.basename(urlparse(link).path)
                normalized_name = f"{retailer_id}/{filename.lower()}"
                if normalized_name in seen_names:
                    result.skipped_dupes += 1
                    continue
                
                # Add to seen sets
                seen_hashes.add(md5_hash)
                seen_names.add(normalized_name)
                
                # Upload to GCS
                bucket = get_bucket()
                if bucket:
                    blob_path = f"raw/{retailer_id}/{run_id}/{filename}"
                    await upload_to_gcs(bucket, blob_path, data)
                
                # Parse XML if applicable
                if filename.lower().endswith(('.xml', '.gz', '.zip')):
                    await _maybe_parse_to_jsonl(retailer_id, filename, data)
                
                # Update counters
                if filename.lower().endswith('.zip'):
                    result.zips += 1
                if filename.lower().endswith('.gz'):
                    result.gz += 1
                result.files_downloaded += 1
                
            except Exception as e:
                result.errors.append(f"download_error:{e}")
                continue
                
    except Exception as e:
        result.errors.append(f"fatal:{e}")
    
    return result

# ----------------------------
# Helper Functions
# ----------------------------

async def fetch_url_bytes(page: Page, url: str) -> bytes:
    resp = await page.request.get(url, timeout=90000)
    if not resp.ok:
        raise RuntimeError(f"download_failed status={resp.status}")
    return await resp.body()

async def _maybe_parse_to_jsonl(retailer_id: str, filename: str, data: bytes):
    """Parse XML to JSONL and save to GCS"""
    try:
        # Use file-type sniffing instead of trusting extensions
        ftype = sniff_file_type(data, filename)
        xml_bytes: Optional[bytes] = None

        if ftype == "xml":
            xml_bytes = data
        elif ftype == "gz":
            try:
                xml_bytes = gzip.decompress(data)
            except Exception:
                # if gz decompress fails, try treating as zip (just in case)
                if data.startswith(b"PK"):
                    ftype = "zip"
                else:
                    logger.warning(f"Failed to decompress gz: {filename}")
                    return
        elif ftype == "zip":
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as z:
                    for n in z.namelist():
                        if n.lower().endswith(".xml"):
                            xml_bytes = z.read(n)
                            break
            except Exception:
                logger.warning(f"Failed to read zip: {filename}")
                return

        if xml_bytes:
            rows = parse_prices_xml(xml_bytes, company=retailer_id)
            if rows:
                # Save to GCS
                bucket = get_bucket()
                if bucket:
                    jsonl_data = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
                    blob_path = f"json/{retailer_id}/{os.path.splitext(filename)[0]}.jsonl"
                    await upload_to_gcs(bucket, blob_path, jsonl_data.encode('utf-8'), "application/json")
    except Exception as e:
        logger.warning(f"Failed to parse {filename}: {e}")

# ----------------------------
# Main Crawl Logic
# ----------------------------

async def crawl_retailer(retailer: dict, run_id: str) -> List[dict]:
    """Crawl a single retailer with all its sources"""
    retailer_id = retailer.get("id", "unknown")
    retailer_name = retailer.get("name", "Unknown")
    
    
    # Get sources, sorted by priority if present
    sources = retailer.get("sources", [])
    if not sources:
        # Fallback to single URL (legacy format)
        url = retailer.get("url", "")
        host = retailer.get("host", "")
        if url:
            sources = [{"url": url, "host": host}]
        else:
            logger.warning(f"No sources found for retailer {retailer_id}")
            return []
    
    # Sort by priority
    sources.sort(key=lambda s: s.get("priority", 999))
    
    # Deduplication sets (per retailer)
    seen_hashes: Set[str] = set()
    seen_names: Set[str] = set()
    dupe_count = 0
    
    results = []

    async with async_playwright() as pw:
        browser, ctx = await new_context(pw)
        page = await ctx.new_page()

        try:
            for source in sources:
                source_url = source.get("url", "")
                if not source_url:
                    continue

                # Determine adapter based on host/type
                host = source.get("host", "").lower()
                adapter_type = "generic"
                
                if PUBLISHED_HOST in host or "publishedprices" in host:
                    adapter_type = "publishedprices"
                elif "binaprojects" in host:
                    adapter_type = "bina"
                
                # Run appropriate adapter
                if adapter_type == "publishedprices":
                    # Get credentials for publishedprices
                    creds_key = source.get("creds_key") or retailer.get("tenantKey")
                    if not creds_key or creds_key not in CREDS:
                        error_msg = f"no_credentials_mapped for key '{creds_key}'"
                        logger.error(f"credentials.missing retailer={retailer_id} creds_key={creds_key}")
                        result = RetailerResult(
                            retailer_id=retailer_id,
                            source_url=source_url,
                            errors=[error_msg],
                            adapter="publishedprices"
                        )
                    else:
                        credentials = CREDS[creds_key]
                        result = await crawl_publishedprices(page, retailer, credentials, run_id)
                elif adapter_type == "bina":
                    result = await bina_adapter(page, source, retailer_id, seen_hashes, seen_names, run_id)
                else:
                    result = await generic_adapter(page, source, retailer_id, seen_hashes, seen_names, run_id)
                
                results.append(result)
                
                # Log results
                logger.info(f"retailer={retailer_id} source={source_url} adapter={adapter_type} "
                          f"links={result.links_found} downloaded={result.files_downloaded} "
                          f"skipped_dupe={result.skipped_dupes}")
                
        finally:
            await ctx.close()
            await browser.close()

    return results

# ----------------------------
# Orchestrator
# ----------------------------

async def run_all(retailers: List[dict]) -> List[dict]:
    """Run all retailers concurrently"""
    # Generate run ID for this execution
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + str(uuid.uuid4())[:8]
    logger.info("run.start run_id=%s retailers=%d", run_id, len(retailers))
    
    # Warn if BUCKET is missing
    if not BUCKET:
        logger.warning("No bucket configured - GCS uploads will be skipped")

    tasks = []
    for retailer in retailers:
        if retailer.get("enabled", True):
            tasks.append(crawl_retailer(retailer, run_id))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)

    out: List[dict] = []
    for retailer_results in results:
        if isinstance(retailer_results, Exception):
            out.append({"error": str(retailer_results)})
        else:
            for result in retailer_results:
                out.append(result.as_dict())
    
    return out