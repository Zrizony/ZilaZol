# crawler/core.py
from __future__ import annotations
import asyncio
import gc
from datetime import datetime, timezone
import uuid
from typing import List, Set

from playwright.async_api import async_playwright

from . import logger
from .constants import BUCKET, PUBLISHED_HOST
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
                    if not creds_key or creds_key not in CREDS:
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
    
    # Warn if BUCKET is missing
    if not BUCKET:
        logger.warning("No bucket configured - GCS uploads will be skipped")

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
    
    # Generate and upload per-run manifest with retry and timeout protection
    if BUCKET:
        try:
            from .gcs import get_bucket, upload_to_gcs
            import json
            
            manifest = {
                "run_id": run_id,
                "started_at": started_at,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "retailers": manifest_retailers,
                "summary": {
                    "total_retailers": len(manifest_retailers),
                    "total_links": sum(r.get("links", 0) for r in manifest_retailers),
                    "total_downloads": sum(r.get("downloads", 0) for r in manifest_retailers),
                    "total_skipped_dupes": sum(r.get("skipped_dupes", 0) for r in manifest_retailers)
                }
            }
            
            bucket = get_bucket()
            if bucket:
                manifest_key = f"manifests/{run_id}.json"
                manifest_data = json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8')
                
                # Upload with timeout protection (30s) and retry logic
                max_retries = 3
                uploaded = False
                for attempt in range(max_retries):
                    try:
                        # Upload with timeout protection
                        await asyncio.wait_for(
                            upload_to_gcs(bucket, manifest_key, manifest_data, content_type="application/json"),
                            timeout=30.0
                        )
                        logger.info("run.manifest bucket=%s key=%s retailers=%d", BUCKET, manifest_key, len(manifest_retailers))
                        uploaded = True
                        break
                    except asyncio.TimeoutError:
                        logger.warning("run.manifest.timeout attempt=%d/%d key=%s", attempt+1, max_retries, manifest_key)
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    except Exception as e:
                        logger.warning("run.manifest.retry attempt=%d/%d error=%s", attempt+1, max_retries, str(e))
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            raise
                
                if not uploaded:
                    logger.error("run.manifest.failed_all_retries key=%s", manifest_key)
        except Exception as e:
            logger.error("run.manifest.failed error=%s", str(e))
    
    return out
