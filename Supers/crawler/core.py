# crawler/core.py
from __future__ import annotations
import asyncio
import gc
from datetime import datetime, timezone
import uuid
from typing import List, Set

from playwright.async_api import async_playwright

from . import logger
from .constants import PUBLISHED_HOST
from .credentials import CREDS
from .models import RetailerResult
from .playwright_helpers import new_context
from .adapters import crawl_publishedprices, bina_adapter, generic_adapter, wolt_dateindex_adapter
from .memory_utils import log_memory


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
    
    # Sort by priority (descending - higher priority first)
    sources.sort(key=lambda s: s.get("priority", 0), reverse=True)
    
    # Deduplication sets (per retailer, shared across sources)
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

                # Determine adapter based on explicit config or host/type
                adapter_type = source.get("adapter") or retailer.get("adapter")
                
                if not adapter_type:
                    # Auto-detect based on host
                    host = source.get("host", "").lower()
                    if PUBLISHED_HOST in host or "publishedprices" in host:
                        adapter_type = "publishedprices"
                    elif "binaprojects" in host:
                        adapter_type = "bina"
                    else:
                        adapter_type = "generic"
                
                # Run appropriate adapter
                if adapter_type == "publishedprices":
                    # Get credentials for publishedprices
                    creds_key = source.get("creds_key") or retailer.get("tenantKey")
                    if not creds_key:
                        error_msg = f"no_credentials_mapped for key '{creds_key}'"
                        logger.error(f"credentials.missing retailer={retailer_id} creds_key={creds_key}")
                        result = RetailerResult(
                            retailer_id=retailer_id,
                            source_url=source_url,
                            errors=[error_msg],
                            adapter="publishedprices",
                            reasons=["credentials_missing"]
                        )
                    else:
                        # Case-insensitive credential lookup
                        if creds_key not in CREDS:
                            # Try case-insensitive match
                            creds_key_lower = creds_key.lower()
                            matched_key = None
                            for key in CREDS.keys():
                                if key.lower() == creds_key_lower:
                                    matched_key = key
                                    break
                            if matched_key:
                                creds_key = matched_key
                                logger.debug(f"credentials.case_match retailer={retailer_id} original={source.get('creds_key') or retailer.get('tenantKey')} matched={creds_key}")
                            else:
                                error_msg = f"no_credentials_mapped for key '{creds_key}'"
                                logger.error(f"credentials.missing retailer={retailer_id} creds_key={creds_key}")
                                result = RetailerResult(
                                    retailer_id=retailer_id,
                                    source_url=source_url,
                                    errors=[error_msg],
                                    adapter="publishedprices",
                                    reasons=["credentials_missing"]
                                )
                        
                        # If we have a valid creds_key (either original or matched), proceed
                        if creds_key in CREDS:
                            credentials = CREDS[creds_key]
                            result = await crawl_publishedprices(page, retailer, credentials, run_id)
                elif adapter_type == "bina":
                    result = await bina_adapter(page, source, retailer_id, seen_hashes, seen_names, run_id)
                elif adapter_type == "wolt_dateindex":
                    result = await wolt_dateindex_adapter(page, source, retailer_id, seen_hashes, seen_names, run_id)
                else:
                    result = await generic_adapter(page, source, retailer_id, seen_hashes, seen_names, run_id)
                
                results.append(result)
                
                # Log results
                logger.info(f"retailer={retailer_id} source={source_url} adapter={adapter_type} "
                          f"links={result.links_found} downloaded={result.files_downloaded} "
                          f"skipped_dupe={result.skipped_dupes}")
                
                # Short-circuit: if this source downloaded files, stop trying other sources
                if result.files_downloaded > 0:
                    logger.info("source.chosen retailer=%s url=%s downloaded=%d", retailer_id, source_url, result.files_downloaded)
                    break
                else:
                    logger.info("source.skipped retailer=%s url=%s reason=no_downloads", retailer_id, source_url)
                
        finally:
            await ctx.close()
            await browser.close()
            # Explicit cleanup to free memory
            del page, ctx, browser
            gc.collect()

    return results


async def run_all(retailers: List[dict]) -> List[dict]:
    """
    Run all retailers with concurrency limiting to prevent OOM.
    
    Uses a semaphore to limit concurrent crawlers to 3 at a time,
    preventing memory exhaustion from running too many Playwright
    browsers simultaneously.
    """
    # Generate run ID for this execution
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + str(uuid.uuid4())[:8]
    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("run.start run_id=%s retailers=%d concurrency_limit=3", run_id, len(retailers))
    log_memory(logger, f"run.start run_id={run_id}")
    
    # All data is saved directly to database
    # Semaphore to limit concurrent crawlers (prevents OOM from too many browsers)
    sem = asyncio.Semaphore(3)
    
    async def limited_crawl(retailer: dict):
        slug = retailer.get("id", retailer.get("name", "unknown"))
        async with sem:
            logger.debug("retailer.start id=%s acquiring_semaphore", slug)
            log_memory(logger, f"before_retailer id={slug}")
            try:
                result = await crawl_retailer(retailer, run_id)
                return result
            finally:
                gc.collect()
                log_memory(logger, f"after_retailer id={slug}")
                logger.debug("retailer.done id=%s releasing_semaphore", slug)
    
    tasks = []
    for retailer in retailers:
        if retailer.get("enabled", True):
            tasks.append(limited_crawl(retailer))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)

    log_memory(logger, "run_all.done_before_manifest")

    out: List[dict] = []
    manifest_retailers = []
    
    for retailer_results in results:
        if isinstance(retailer_results, Exception):
            error_entry = {"error": str(retailer_results)}
            out.append(error_entry)
            manifest_retailers.append({
                "slug": "unknown",
                "adapter": "unknown",
                "source": "",
                "links": 0,
                "downloads": 0,
                "skipped_dupes": 0,
                "reasons": ["exception"],
                "error": str(retailer_results)
            })
        else:
            for result in retailer_results:
                result_dict = result.as_dict()
                out.append(result_dict)
                # Add to manifest
                manifest_retailers.append({
                    "slug": result.retailer_id,
                    "adapter": result.adapter,
                    "source": result.source_url,
                    "links": result.links_found,
                    "downloads": result.files_downloaded,
                    "skipped_dupes": result.skipped_dupes,
                    "reasons": result.reasons,
                    "errors": result.errors if result.errors else []
                })
    
    return out
