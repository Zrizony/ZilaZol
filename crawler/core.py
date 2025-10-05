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
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse

import aiofiles
import gzip
import zipfile
from playwright.async_api import async_playwright, Page
from tenacity import retry, stop_after_attempt, wait_fixed
from google.cloud import storage

from . import logger

# ----------------------------
# Settings / Constants
# ----------------------------

PRICES_BUCKET = os.getenv("PRICES_BUCKET") or os.getenv(
    "BUCKET_NAME"
)  # either env works
LOCAL_DOWNLOAD_DIR = os.getenv("LOCAL_DOWNLOAD_DIR", "downloads")
LOCAL_JSON_DIR = os.getenv("LOCAL_JSON_DIR", "json_out")
SCREENSHOTS_DIR = os.getenv("SCREENSHOTS_DIR", "screenshots")
MAX_LOGIN_RETRIES = int(os.getenv("MAX_RETRIES_LOGIN", "3"))

IGNORE_DISPLAY_SUBSTRINGS = ["וולט", "סטופ מרקט"]

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

# Map retailer display-name substrings → key in CREDS
SHOP_TO_CREDKEY: Dict[str, str] = {
    "דור אלון": "doralon",
    "טיב טעם": "TivTaam",
    "יוחננוף": "yohananof",
    "אושר עד": "osherad",
    "סאלח דבאח": "SalachD",
    "פוליצר": "politzer",
    "פז ": "Paz_bo",
    "קשת טעמים": "Keshet",
    "רמי לוי": "RamiLevi",
    "קופיקס": "SuperCofixApp",
    "פרשמרקט": "freshmarket",
}

PUBLISHED_PRICES_HOST = "url.publishedprices.co.il"

# ----------------------------
# Data Models
# ----------------------------


@dataclass
class RetailerResult:
    display_name: str
    portal_url: str
    files_downloaded: int = 0
    xml_parsed_rows: int = 0
    zips: int = 0
    gz: int = 0
    errors: List[str] = None

    def as_dict(self):
        d = asdict(self)
        d["ts"] = datetime.now(timezone.utc).isoformat()
        return d


# ----------------------------
# Helpers
# ----------------------------


def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-.]+", "_", s).strip("_")[:120]


def md5_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def parse_datetime_from_filename(name: str) -> Optional[datetime]:
    # Looks for YYYYMMDDHHMM or 202506200531, or 2025-06-20-000020 type tokens
    m = re.search(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})[-_]?(\d{2})(\d{2})", name)
    if not m:
        return None
    try:
        return datetime(
            int(m.group(1)),
            int(m.group(2)),
            int(m.group(3)),
            int(m.group(4)),
            int(m.group(5)),
            tzinfo=timezone.utc,
        )
    except Exception:
        return None


def ensure_dirs(*paths: str):
    for p in paths:
        os.makedirs(p, exist_ok=True)


def should_ignore(display_name: str) -> bool:
    return any(bad in (display_name or "") for bad in IGNORE_DISPLAY_SUBSTRINGS)


def get_cred_for(display_name: str) -> Optional[Dict[str, str]]:
    for k, credkey in SHOP_TO_CREDKEY.items():
        if k in display_name:
            return CREDS.get(credkey)
    return None


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
# XML → JSONL parser (best effort across schemas)
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

    # Common-ish structures: Items/Item or root/Item
    items = root.findall(".//Item")
    if not items:
        items = list(root)

    for it in items:
        # Try a range of common tag names
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

        # Skip useless rows
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
    fname = f"{safe_name(display_name)}_{ts()}.png"
    await page.screenshot(path=os.path.join(SCREENSHOTS_DIR, fname), full_page=True)


@retry(stop=stop_after_attempt(MAX_LOGIN_RETRIES), wait=wait_fixed(2))
async def login_publishedprices(
    page: Page, base_url: str, username: str, password: Optional[str]
):
    # Force to /login first
    if "/login" not in base_url.lower():
        base_url = f"https://{PUBLISHED_PRICES_HOST}/login"
    await page.goto(base_url, wait_until="domcontentloaded", timeout=60000)

    # Fill username
    for sel in [
        "input[name='username']",
        "#username",
        "input[name='Email']",
        "input[type='email']",
    ]:
        if await page.locator(sel).count():
            await page.fill(sel, username)
            break

    # Password optional
    filled_pw = False
    for sel in ["input[name='password']", "#password", "input[type='password']"]:
        if await page.locator(sel).count() and password:
            await page.fill(sel, password)
            filled_pw = True
            break

    # Submit
    for sel in [
        "button[type='submit']",
        "input[type='submit']",
        "button:has-text('כניסה')",
        "button:has-text('Login')",
    ]:
        if await page.locator(sel).count():
            await page.click(sel)
            break

    # Wait for redirect to /file; if not, navigate explicitly
    try:
        await page.wait_for_url("**/file", timeout=20000)
    except Exception:
        await page.goto(
            "https://url.publishedprices.co.il/file", wait_until="load", timeout=25000
        )

    await page.wait_for_load_state("networkidle", timeout=10000)


async def collect_download_links(page: Page) -> List[str]:
    hrefs = await page.eval_on_selector_all(
        "a[href]", "els => els.map(a => a.getAttribute('href'))"
    )
    hrefs = [h for h in (hrefs or []) if h]
    out = []
    for h in hrefs:
        h_abs = await page.evaluate("u => new URL(u, location.href).href", h)
        if (
            h_abs.lower().endswith((".xml", ".gz", ".zip"))
            or "download" in h_abs.lower()
        ):
            out.append(h_abs)
    return sorted(set(out))


async def fetch_url_bytes(page: Page, url: str) -> bytes:
    # Try clicking if same page link; otherwise just fetch via page request
    resp = await page.request.get(url, timeout=90000)
    if not resp.ok:
        raise RuntimeError(f"download_failed status={resp.status}")
    return await resp.body()


# ----------------------------
# Main crawl per retailer
# ----------------------------


async def crawl_one(display_name: str, portal_url: str) -> RetailerResult:
    result = RetailerResult(display_name=display_name, portal_url=portal_url, errors=[])
    if should_ignore(display_name):
        logger.info("Skipping (ignored): %s", display_name)
        return result

    # Retailer directory
    retailer_dir = os.path.join(LOCAL_DOWNLOAD_DIR, safe_name(display_name))
    retailer_json_dir = os.path.join(LOCAL_JSON_DIR, safe_name(display_name))
    ensure_dirs(retailer_dir, retailer_json_dir, SCREENSHOTS_DIR)

    # GCS
    bucket = get_bucket()
    gcs_prefix = f"raw/{safe_name(display_name)}"

    parsed_host = urlparse(portal_url).netloc.lower()

    async with async_playwright() as pw:
        browser, ctx = await new_context(pw)
        page = await ctx.new_page()

        try:
            # Special adapter for PublishedPrices tenants
            if PUBLISHED_PRICES_HOST in parsed_host:
                cred = get_cred_for(display_name)
                if not cred:
                    result.errors.append("no_credentials_mapped")
                    logger.warning("No creds for %s", display_name)
                    await ctx.close()
                    await browser.close()
                    return result

                await login_publishedprices(
                    page, portal_url, cred.get("username"), cred.get("password")
                )
                await screenshot_after_login(page, display_name)
                links = await collect_download_links(page)

            else:
                # Generic: go to portal and collect .xml/.gz/.zip links
                await page.goto(
                    portal_url, wait_until="domcontentloaded", timeout=60000
                )
                await page.wait_for_load_state("networkidle", timeout=10000)
                await screenshot_after_login(
                    page, display_name
                )  # snapshot even if no login
                links = await collect_download_links(page)

            if not links:
                result.errors.append("no_download_links_found")
                logger.warning("No download links for %s", display_name)
                await ctx.close()
                await browser.close()
                return result

            for href in links:
                try:
                    data = await fetch_url_bytes(page, href)
                except Exception as e:
                    result.errors.append(f"download_error:{e}")
                    continue

                # Local save
                fname = (
                    safe_name(os.path.basename(urlparse(href).path))
                    or f"file_{md5_bytes(data)}"
                )
                local_path = os.path.join(retailer_dir, fname)
                async with aiofiles.open(local_path, "wb") as f:
                    await f.write(data)

                # Upload to GCS
                if bucket:
                    await upload_to_gcs(bucket, f"{gcs_prefix}/{fname}", data)

                # Counters
                lower = fname.lower()
                if lower.endswith(".zip"):
                    result.zips += 1
                if lower.endswith(".gz"):
                    result.gz += 1
                result.files_downloaded += 1

                # Parse → JSONL (only XML content)
                await _maybe_parse_to_jsonl(
                    display_name, retailer_json_dir, fname, data
                )

        except Exception as e:
            result.errors.append(f"fatal:{e}")
        finally:
            await ctx.close()
            await browser.close()

    return result


async def _maybe_parse_to_jsonl(
    display_name: str, json_dir: str, fname: str, data: bytes
):
    # Detect content → xml bytes
    xml_bytes: Optional[bytes] = None
    lf = fname.lower()

    try:
        if lf.endswith(".xml"):
            xml_bytes = data
        elif lf.endswith(".gz"):
            xml_bytes = gzip.decompress(data)
        elif lf.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                # take all .xml entries
                for n in z.namelist():
                    if n.lower().endswith(".xml"):
                        xml_bytes = z.read(n)
                        await _write_jsonl_for_xml(display_name, json_dir, n, xml_bytes)
                return
    except Exception:
        return

    if xml_bytes:
        await _write_jsonl_for_xml(display_name, json_dir, fname, xml_bytes)


async def _write_jsonl_for_xml(
    display_name: str, json_dir: str, source_name: str, xml_bytes: bytes
):
    # Choose output name based on source timestamp; if newer, replace
    base = os.path.splitext(os.path.basename(source_name))[0]
    out_name = f"{base}.jsonl"
    out_path = os.path.join(json_dir, out_name)

    # Newer-than logic (prefer filename-embedded timestamp)
    new_dt = parse_datetime_from_filename(source_name) or datetime.now(timezone.utc)
    if os.path.exists(out_path):
        # quick stamp compare via filename; if equal, overwrite, else keep the latest
        existing_dt = parse_datetime_from_filename(
            os.path.basename(out_path)
        ) or datetime.fromtimestamp(os.path.getmtime(out_path), tz=timezone.utc)
        if existing_dt and new_dt and new_dt <= existing_dt:
            # Existing is same/newer → skip
            return

    company = display_name
    rows = parse_prices_xml(xml_bytes, company=company)
    if not rows:
        return

    os.makedirs(json_dir, exist_ok=True)
    async with aiofiles.open(out_path, "w", encoding="utf-8") as f:
        for r in rows:
            await f.write(json.dumps(r, ensure_ascii=False) + "\n")


# ----------------------------
# Orchestrator
# ----------------------------


async def run_all(retailers: List[Tuple[str, str]]) -> List[dict]:
    """Run all retailers concurrently; input is [(display_name, portal_url)]."""
    tasks = []
    for display, url in retailers:
        tasks.append(crawl_one(display, url))
    results = await asyncio.gather(*tasks, return_exceptions=True)

    out: List[dict] = []
    for r in results:
        if isinstance(r, Exception):
            out.append({"error": str(r)})
        else:
            out.append(r.as_dict())
    return out
