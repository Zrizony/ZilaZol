#!/usr/bin/env python3
"""
Standalone script to run the crawler.
Used by GitHub Actions for scheduled crawls.

Usage:
    python run_crawler.py --retailer=shufersal     # Crawl single retailer
    python run_crawler.py --type=public             # Crawl all public retailers
    python run_crawler.py --type=auth               # Crawl all auth retailers
    python run_crawler.py                           # Crawl all retailers (default)
"""
from __future__ import annotations
import argparse
import asyncio
import sys
import json
from pathlib import Path
from typing import Optional
from crawler.core import run_all
from crawler.config import get_retailers, _requires_credentials
from crawler.db import upsert_retailer, get_pool, close_pool, fetch_retailer_slugs
from crawler import logger

async def sync_retailers_from_json():
    """Sync retailers from JSON file to database before crawling"""
    try:
        json_path = Path(__file__).parent / "data" / "retailers.json"
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        retailers = data.get("retailers", [])
        logger.info(f"Syncing {len(retailers)} retailers from JSON to database...")
        
        pool = await get_pool()
        if not pool:
            logger.warning("Database not available - skipping retailer sync")
            return
        
        synced = 0
        for retailer in retailers:
            retailer_id = retailer.get("id")
            retailer_name = retailer.get("name")
            
            if retailer_id and retailer_name and retailer.get("enabled", True):
                need_creds = _requires_credentials(retailer)
                db_id = await upsert_retailer(retailer_id, retailer_name, need_creds)
                if db_id:
                    synced += 1
        
        logger.info(f"Synced {synced} retailers to database")
    except Exception as e:
        logger.warning(f"Retailer sync failed (continuing anyway): {e}")

async def get_retailers_for_crawl(crawl_type: Optional[str] = None):
    """
    Get retailers for crawling, filtered by type.
    
    Args:
        crawl_type: 'public' (no login), 'auth' (login required), or None (all)
    
    Returns:
        List of retailer config dictionaries from JSON, filtered by DB needCreds flag
    """
    # Sync retailers from JSON to DB first (ensures DB is up to date)
    await sync_retailers_from_json()
    
    # Fetch retailer slugs from database filtered by needCreds
    pool = await get_pool()
    if not pool:
        logger.warning("Database not available - falling back to JSON config")
        # Fallback to JSON-based filtering
        if crawl_type == "public":
            return [r for r in get_retailers("public") if r.get("enabled", True)]
        elif crawl_type == "auth":
            return [r for r in get_retailers("creds") if r.get("enabled", True)]
        else:
            return [r for r in get_retailers() if r.get("enabled", True)]
    
    # Determine filter based on crawl_type
    need_creds_filter = None
    if crawl_type == "public":
        need_creds_filter = False
    elif crawl_type == "auth":
        need_creds_filter = True
    # If crawl_type is None, need_creds_filter stays None (fetch all)
    
    # Fetch slugs from database
    db_slugs = await fetch_retailer_slugs(need_creds_filter)
    logger.info(f"Found {len(db_slugs)} retailers in database (type={crawl_type or 'all'})")
    
    # Load full retailer configs from JSON for those slugs
    all_retailers = get_retailers()
    retailers_by_slug = {r.get("id"): r for r in all_retailers}
    
    # Filter to only enabled retailers that exist in DB
    filtered_retailers = []
    for slug in db_slugs:
        retailer = retailers_by_slug.get(slug)
        if retailer and retailer.get("enabled", True):
            filtered_retailers.append(retailer)
    
    return filtered_retailers

async def main():
    """Run crawler for retailers filtered by type or single retailer"""
    parser = argparse.ArgumentParser(description="Run price crawler")
    parser.add_argument(
        "--type",
        choices=["public", "auth"],
        help="Crawl type: 'public' (no login) or 'auth' (login required)"
    )
    parser.add_argument(
        "--retailer",
        type=str,
        help="Crawl a single retailer by slug (e.g., 'shufersal', 'ramilevi')"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,  # 5 hours default timeout per retailer
        help="Timeout in minutes for crawling a single retailer (default: 300 = 5 hours)"
    )
    args = parser.parse_args()
    
    try:
        # If --retailer is specified, crawl only that retailer
        if args.retailer:
            # Sync retailers first
            await sync_retailers_from_json()
            
            # Load retailer config from JSON
            all_retailers = get_retailers()
            retailer_config = None
            for r in all_retailers:
                if r.get("id") == args.retailer and r.get("enabled", True):
                    retailer_config = r
                    break
            
            if not retailer_config:
                logger.error(f"Retailer '{args.retailer}' not found or disabled")
                sys.exit(1)
            
            retailers = [retailer_config]
            logger.info(f"crawler.start retailer={args.retailer} timeout={args.timeout}min")
        else:
            # Get retailers filtered by type
            retailers = await get_retailers_for_crawl(args.type)
            
            if not retailers:
                logger.warning(f"No enabled retailers found for type={args.type or 'all'}")
                sys.exit(0)
            
            logger.info(f"crawler.start type={args.type or 'all'} retailers={len(retailers)} timeout={args.timeout}min")
        
        if not retailers:
            logger.warning(f"No retailers to crawl")
            sys.exit(0)
        
        # Run crawler with timeout
        timeout_seconds = args.timeout * 60
        try:
            results = await asyncio.wait_for(run_all(retailers), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout reached - stopping run gracefully. Partial data saved. timeout={args.timeout}min retailers={len(retailers)}")
            sys.exit(0)
        
        # Count successes and errors
        success_count = sum(1 for r in results if not r.get("errors"))
        error_count = len(results) - success_count
        
        logger.info(
            "crawler.complete retailers=%d success=%d errors=%d",
            len(retailers),
            success_count,
            error_count
        )
        
        # Exit with error code if any failures
        if error_count > 0:
            sys.exit(1)
        
        sys.exit(0)
        
    except Exception as e:
        logger.exception("crawler.failed error=%s", str(e))
        sys.exit(1)
    finally:
        await close_pool()

if __name__ == "__main__":
    asyncio.run(main())

