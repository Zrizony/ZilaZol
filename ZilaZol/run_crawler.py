#!/usr/bin/env python3
"""
Standalone script to run the crawler.
Used by GitHub Actions for scheduled crawls.
"""
from __future__ import annotations
import asyncio
import sys
import json
from pathlib import Path
from crawler.core import run_all
from crawler.config import get_retailers
from crawler.db import upsert_retailer, get_pool, close_pool
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
                db_id = await upsert_retailer(retailer_id, retailer_name)
                if db_id:
                    synced += 1
        
        logger.info(f"Synced {synced} retailers to database")
    except Exception as e:
        logger.warning(f"Retailer sync failed (continuing anyway): {e}")

async def main():
    """Run crawler for all enabled retailers"""
    try:
        # Sync retailers from JSON to database first
        await sync_retailers_from_json()
        
        # Get all enabled retailers
        retailers = [r for r in get_retailers() if r.get("enabled", True)]
        
        if not retailers:
            logger.warning("No enabled retailers found")
            sys.exit(0)
        
        logger.info("crawler.start retailers=%d", len(retailers))
        
        # Run crawler
        results = await run_all(retailers)
        
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

