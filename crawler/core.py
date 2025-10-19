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
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

import aiofiles
import gzip
import zipfile
from playwright.async_api import async_playwright, Page
from google.cloud import storage

from . import logger

# ----------------------------
# Settings / Constants
# ----------------------------

PRICES_BUCKET = os.getenv("PRICES_BUCKET") or os.getenv("BUCKET_NAME")
LOCAL_DOWNLOAD_DIR = os.getenv("LOCAL_DOWNLOAD_DIR", "downloads")
LOCAL_JSON_DIR = os.getenv("LOCAL_JSON_DIR", "json_out")
SCREENSHOTS_DIR = os.getenv("SCREENSHOTS_DIR", "screenshots")
MAX_LOGIN_RETRIES = int(os.getenv("MAX_RETRIES_LOGIN", "3"))


# Per-tenant creds (public list per your input)
CREDS: Dict[str, Dict[str, str]] = {
    "doralon": {"username": "doralon"},
    "TivTaam": {"username": "TivTaam"},
    "yohananof": {"username": "yohananof"},
    "osherad": {"username": "osherad"},
    "SalachD": {"username": "SalachD", "password": "12345"},
    "Stop_Market": {"username": "Stop_Market"},
    "politzer": {"username": "politzer"},
    "Paz_bo": {"username": "Paz_bo", "password": "paz468"},
    "yuda_ho": {"username": "yuda_ho", "password": "Yud@147"},
    "freshmarket": {"username": "freshmarket"},
    "Keshet": {"username": "Keshet"},
    "RamiLevi": {"username": "RamiLevi"},
    "SuperCofixApp": {"username": "SuperCofixApp"},
}

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


def ensure_dirs(*paths: str):
    for p in paths:
        os.makedirs(p, exist_ok=True)


# ----------------------------
# GCS
# ----------------------------

def get_bucket() -> Optional[storage.Bucket]:
    if not PRICES_BUCKET:
        return None
    client = storage.Client()
    return client.bucket(PRICES_BUCKET)

async def upload_to_gcs(
    bucket: storage.Bucket,
    blob_path: str,
    data: bytes,
    content_type: str = "application/octet-stream",
):
    blob = bucket.blob(blob_path)
    blob.upload_from_string(data, content_type=content_type)

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

async def publishedprices_login(page, login_url: str, username: str, password: str):
    await page.goto(login_url, wait_until="domcontentloaded", timeout=90000)
    # Try common selectors; don't fail if password is empty
    for sel in ["input[name='username']", "#username", "input[type='email']"]:
        if await page.locator(sel).count():
            await page.fill(sel, username)
            break
    for sel in ["input[name='password']", "#password", "input[type='password']"]:
        if password and await page.locator(sel).count():
            await page.fill(sel, password)
            break
    for sel in ["button[type='submit']", "input[type='submit']",
                "button:has-text('כניסה')", "button:has-text('Login')"]:
        if await page.locator(sel).count():
            await page.click(sel)
            break
    try:
        await page.wait_for_url("**/file**", timeout=25000)
    except:
        await page.goto(f"https://{PUBLISHED_HOST}/file", wait_until="domcontentloaded")

async def publishedprices_open_subpath(page, subpath: str):
    # Direct Cerberus path first
    direct = f"https://{PUBLISHED_HOST}/file/cdup/{subpath}/"
    try:
        await page.goto(direct, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=15000)
        await page.wait_for_timeout(2000)
        return
    except:
        pass
    # Fallback: click folder link by text
    loc = page.locator("table a", has_text=subpath)
    if await loc.count() > 0:
        await loc.first.click()
        await page.wait_for_load_state("networkidle", timeout=20000)
        await page.wait_for_timeout(2000)

async def collect_links_on_page(page) -> list[str]:
    hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(a => a.href)")
    return [h for h in hrefs or [] if h.lower().endswith(DOWNLOAD_SUFFIXES)]

# ----------------------------
# Adapters
# ----------------------------

async def publishedprices_adapter(page: Page, source: dict, retailer: dict, retailer_id: str, seen_hashes: Set[str], seen_names: Set[str]) -> RetailerResult:
    """PublishedPrices adapter with optional subfolder support"""
    result = RetailerResult(
        retailer_id=retailer_id,
        source_url=source.get("url", ""),
        errors=[],
        adapter="publishedprices"
    )
    
    # Get credentials from retailer level (authRef/tenantKey) or source level (creds_key)
    creds_key = source.get("creds_key") or retailer.get("tenantKey")
    if not creds_key or creds_key not in CREDS:
        result.errors.append("no_credentials_mapped")
        return result
    
    credentials = CREDS[creds_key]
    username = credentials.get("username")
    password = credentials.get("password", "")
    
    try:
        # Login
        await publishedprices_login(f"https://{PUBLISHED_HOST}/login", username, password)
        
        # Wait for page to fully load after login
        await page.wait_for_load_state("networkidle", timeout=15000)
        await page.wait_for_timeout(2000)
        
        # Navigate to subfolder if specified
        subpath = source.get("subpath")
        # Special case: Super Yuda needs Yuda subfolder
        if not subpath and retailer_id == "superyuda":
            subpath = "Yuda"
        if subpath:
            result.subpath = subpath
            await publishedprices_open_subpath(page, subpath)
            # Wait for subfolder to load
            await page.wait_for_load_state("networkidle", timeout=15000)
            await page.wait_for_timeout(2000)
        
        # Collect download links
        links = await collect_links_on_page(page)
        logger.info(f"retailer={retailer_id} source={source.get('url')} adapter=publishedprices links={len(links)}")
        
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
                    blob_path = f"raw/{retailer_id}/{filename}"
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

async def bina_adapter(page: Page, source: dict, retailer_id: str, seen_hashes: Set[str], seen_names: Set[str]) -> RetailerResult:
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
                    blob_path = f"raw/{retailer_id}/{filename}"
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

async def generic_adapter(page: Page, source: dict, retailer_id: str, seen_hashes: Set[str], seen_names: Set[str]) -> RetailerResult:
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
        
        # Collect download links
        links = await collect_links_on_page(page)
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
                    blob_path = f"raw/{retailer_id}/{filename}"
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
        # Detect content → xml bytes
        xml_bytes: Optional[bytes] = None
        lf = filename.lower()

        if lf.endswith(".xml"):
            xml_bytes = data
        elif lf.endswith(".gz"):
            # Check if it's actually a ZIP file first (common case)
            if data.startswith(b'PK'):
                logger.debug(f"File {filename} appears to be ZIP despite .gz extension, using ZIP decompression")
                try:
                    with zipfile.ZipFile(io.BytesIO(data)) as z:
                        for n in z.namelist():
                            if n.lower().endswith(".xml"):
                                xml_bytes = z.read(n)
                                break
                except Exception as zip_error:
                    logger.warning(f"Failed to parse {filename} as ZIP: {zip_error}")
                    return
            else:
                # Try GZIP for genuine .gz files
                try:
                    xml_bytes = gzip.decompress(data)
                except Exception as gzip_error:
                    logger.warning(f"Failed to parse {filename} as GZIP: {gzip_error}")
                    return
        elif lf.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                for n in z.namelist():
                    if n.lower().endswith(".xml"):
                        xml_bytes = z.read(n)
                        break

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

async def crawl_retailer(retailer: dict) -> List[dict]:
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
                    result = await publishedprices_adapter(page, source, retailer, retailer_id, seen_hashes, seen_names)
                elif adapter_type == "bina":
                    result = await bina_adapter(page, source, retailer_id, seen_hashes, seen_names)
                else:
                    result = await generic_adapter(page, source, retailer_id, seen_hashes, seen_names)
                
                results.append(result)
                
                # Log results
                logger.info(f"retailer={retailer_id} source={source_url} adapter={adapter_type} "
                          f"links={len(await collect_links_on_page(page))} downloaded={result.files_downloaded} "
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
    tasks = []
    for retailer in retailers:
        if retailer.get("enabled", True):
            tasks.append(crawl_retailer(retailer))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    out: List[dict] = []
    for retailer_results in results:
        if isinstance(retailer_results, Exception):
            out.append({"error": str(retailer_results)})
        else:
            for result in retailer_results:
                out.append(result.as_dict())
    
    return out