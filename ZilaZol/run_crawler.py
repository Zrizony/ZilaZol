#!/usr/bin/env python3
"""
Standalone script to run the crawler.
Used by GitHub Actions for scheduled crawls.
"""
from __future__ import annotations
import asyncio
import sys
from crawler.core import run_all
from crawler.config import get_retailers
from crawler import logger

async def main():
    """Run crawler for all enabled retailers"""
    try:
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

if __name__ == "__main__":
    asyncio.run(main())

