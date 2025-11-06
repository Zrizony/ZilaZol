# crawler/adapters/bina.py
from __future__ import annotations
import re
from typing import List, Set

from playwright.async_api import Page

from .. import logger
from ..models import RetailerResult
from ..archive_utils import sniff_kind, md5_hex
from ..download import fetch_url
from ..gcs import get_bucket, upload_to_gcs
from ..parsers import parse_from_blob
from ..adapters.base import collect_links_on_page


async def bina_collect_links(page: Page) -> List[str]:
    """Collect links from Bina Projects sites with iframe/postback handling."""
    frame = page.frame(url=re.compile(r"/Main\.aspx", re.I)) or page.main_frame
    for candidate in ["מחיר מלא", "Price Full", "PriceFull"]:
        try:
            await frame.get_by_text(candidate, exact=False).click(timeout=2000)
            break
        except Exception:
            pass
    for btn in ["חפש", "Search", "רענן"]:
        try:
            await frame.get_by_role("button", name=re.compile(btn, re.I)).click(timeout=1500)
        except Exception:
            pass
    try:
        await frame.wait_for_selector("a[href*='.zip'], a[href*='.gz'], a:has-text('Price')", timeout=6000)
        hrefs = await frame.eval_on_selector_all(
            "a",
            "els => els.map(a => a.href).filter(u => u && (u.toLowerCase().includes('.zip') || u.toLowerCase().includes('.gz') || u.toLowerCase().includes('pricefull')))"
        )
        hrefs = list(dict.fromkeys(hrefs))
        return hrefs
    except Exception:
        captured: Set[str] = set()
        def _on_response(resp):
            try:
                url = (getattr(resp, "url", "") or "").lower()
                if any(p in url for p in (".zip", ".gz", "pricefull")):
                    captured.add(resp.url)
            except Exception:
                pass
        page.on("response", _on_response)
        for _ in range(3):
            try:
                await frame.locator("text=רענן").click(timeout=1000)
            except Exception:
                pass
            await page.wait_for_timeout(1000)
        return list(captured)


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
        
        # Collect download links via dedicated bina strategy with fallback
        links = await collect_links_on_page(page, source.get("download_patterns") or source.get("patterns"))
        if not links:
            links = await bina_collect_links(page)
        result.links_found = len(links)
        logger.info("links.discovered slug=%s count=%d", retailer_id, len(links))
        
        # Process each link
        bucket = get_bucket()
        for link in links:
            try:
                data, resp, filename = await fetch_url(page, link)
                kind = sniff_kind(data)
                md5_hash = md5_hex(data)
                
                # Check for duplicates
                if md5_hash in seen_hashes:
                    result.skipped_dupes += 1
                    continue
                
                # Normalize filename for name-based dedupe
                normalized_name = f"{retailer_id}/{filename.lower()}"
                if normalized_name in seen_names:
                    result.skipped_dupes += 1
                    continue
                
                # Add to seen sets
                seen_hashes.add(md5_hash)
                seen_names.add(normalized_name)
                
                # Upload to GCS
                if bucket:
                    blob_path = f"raw/{retailer_id}/{run_id}/{md5_hash}_{filename}"
                    await upload_to_gcs(bucket, blob_path, data, metadata={"md5_hex": md5_hash, "source_filename": filename})
                
                # Unified parse (logs file.downloaded, extracts, parses, logs file.processed)
                await parse_from_blob(data, filename, retailer_id, run_id)
                
                # Update counters based on sniffed kind (not filename extension)
                if kind == "zip":
                    result.zips += 1
                elif kind == "gz":
                    result.gz += 1
                result.files_downloaded += 1
                
            except Exception as e:
                result.errors.append(f"download_error:{e}")
                continue
                
    except Exception as e:
        result.errors.append(f"fatal:{e}")
    
    return result

