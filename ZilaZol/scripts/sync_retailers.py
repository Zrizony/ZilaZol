#!/usr/bin/env python3
"""
Sync retailers from retailers.json to the database.
This script will add any missing retailers and update existing ones.
"""
from __future__ import annotations
import asyncio
import json
import os
import sys
from pathlib import Path

# Add parent directory to path to import crawler modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from crawler.db import upsert_retailer, get_pool, close_pool
from crawler import logger

async def sync_retailers():
    """Sync all retailers from JSON file to database"""
    try:
        # Load retailers.json
        json_path = Path(__file__).parent.parent / "data" / "retailers.json"
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        retailers = data.get("retailers", [])
        logger.info(f"Found {len(retailers)} retailers in JSON file")
        
        # Check database connection
        pool = await get_pool()
        if not pool:
            logger.error("Failed to connect to database. Check DATABASE_URL environment variable.")
            sys.exit(1)
        
        # Sync each retailer
        synced = 0
        skipped = 0
        
        for retailer in retailers:
            retailer_id = retailer.get("id")
            retailer_name = retailer.get("name")
            
            if not retailer_id or not retailer_name:
                logger.warning(f"Skipping retailer with missing id or name: {retailer}")
                skipped += 1
                continue
            
            # Check if enabled (only sync enabled retailers by default)
            if not retailer.get("enabled", True):
                logger.debug(f"Skipping disabled retailer: {retailer_id}")
                skipped += 1
                continue
            
            # Upsert retailer (will insert if new, update if exists)
            db_id = await upsert_retailer(retailer_id, retailer_name)
            if db_id:
                synced += 1
                logger.info(f"✓ Synced: {retailer_id} ({retailer_name})")
            else:
                logger.warning(f"✗ Failed: {retailer_id}")
                skipped += 1
        
        logger.info(f"Sync complete: {synced} synced, {skipped} skipped")
        return synced
        
    except Exception as e:
        logger.exception(f"Sync failed: {e}")
        sys.exit(1)
    finally:
        await close_pool()

if __name__ == "__main__":
    asyncio.run(sync_retailers())

