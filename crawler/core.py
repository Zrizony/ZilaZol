# crawler/core.py
from __future__ import annotations
import asyncio
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
                
        finally:
            await ctx.close()
            await browser.close()

    return results


async def run_all(retailers: List[dict]) -> List[dict]:
    """Run all retailers concurrently"""
    # Generate run ID for this execution
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + str(uuid.uuid4())[:8]
    started_at = datetime.now(timezone.utc).isoformat()
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
    
    # Generate and upload per-run manifest
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
                await upload_to_gcs(bucket, manifest_key, manifest_data, content_type="application/json")
                logger.info("run.manifest bucket=%s key=%s retailers=%d", BUCKET, manifest_key, len(manifest_retailers))
        except Exception as e:
            logger.error("run.manifest.failed error=%s", str(e))
    
    return out
