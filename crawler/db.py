# crawler/db.py
"""
Database integration for storing crawled data in PostgreSQL.
Connects to the same database as the Next.js app.
"""
from __future__ import annotations
import os
import asyncpg
from typing import Optional, List, Dict
from datetime import datetime

from . import logger


# Global connection pool
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> Optional[asyncpg.Pool]:
    """Get or create database connection pool."""
    global _pool
    
    if _pool is not None:
        return _pool
    
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.warning("DATABASE_URL not set - database writes will be skipped")
        return None
    
    try:
        # Parse connection string and create pool
        _pool = await asyncpg.create_pool(
            database_url,
            min_size=1,
            max_size=5,
            command_timeout=60,
        )
        logger.info("db.pool.created")
        return _pool
    except Exception as e:
        logger.error("db.pool.failed error=%s", str(e))
        return None


async def close_pool():
    """Close database connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("db.pool.closed")


async def upsert_retailer(retailer_id: str, name: str) -> Optional[int]:
    """
    Upsert a retailer in the database.
    Returns the retailer's database ID, or None if failed.
    """
    pool = await get_pool()
    if not pool:
        return None
    
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO retailers (slug, name, "createdAt", "updatedAt")
                VALUES ($1, $2, NOW(), NOW())
                ON CONFLICT (slug) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    "updatedAt" = NOW()
                RETURNING id
            """, retailer_id, name)
            return row['id'] if row else None
    except Exception as e:
        logger.error("db.retailer.upsert.failed retailer=%s error=%s", retailer_id, str(e))
        return None


async def upsert_product(barcode: str, name: str, brand: Optional[str] = None, 
                         size: Optional[float] = None, unit: Optional[str] = None,
                         category: Optional[str] = None) -> Optional[int]:
    """
    Upsert a product in the database.
    Returns the product's database ID, or None if failed.
    """
    pool = await get_pool()
    if not pool:
        return None
    
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO products (barcode, name, brand, size, unit, category, "createdAt", "updatedAt")
                VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
                ON CONFLICT (barcode) 
                DO UPDATE SET 
                    name = COALESCE(EXCLUDED.name, products.name),
                    brand = COALESCE(EXCLUDED.brand, products.brand),
                    size = COALESCE(EXCLUDED.size, products.size),
                    unit = COALESCE(EXCLUDED.unit, products.unit),
                    category = COALESCE(EXCLUDED.category, products.category),
                    "updatedAt" = NOW()
                RETURNING id
            """, barcode, name, brand, size, unit, category)
            return row['id'] if row else None
    except Exception as e:
        logger.error("db.product.upsert.failed barcode=%s error=%s", barcode, str(e))
        return None


async def create_price_snapshot(product_id: int, retailer_id: int, price: float,
                                currency: str = "ILS", is_on_sale: bool = False,
                                timestamp: Optional[datetime] = None,
                                store_id: Optional[int] = None) -> Optional[int]:
    """
    Create a price snapshot in the database.
    Returns the snapshot's database ID, or None if failed.
    """
    pool = await get_pool()
    if not pool:
        return None
    
    if timestamp is None:
        timestamp = datetime.utcnow()
    
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO price_snapshots 
                    (product_id, retailer_id, store_id, price, currency, "isOnSale", timestamp, "createdAt", "updatedAt")
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
                RETURNING id
            """, product_id, retailer_id, store_id, price, currency, is_on_sale, timestamp)
            return row['id'] if row else None
    except Exception as e:
        logger.error("db.price_snapshot.create.failed product_id=%d retailer_id=%d error=%s", 
                    product_id, retailer_id, str(e))
        return None


async def save_parsed_prices(rows: List[Dict], retailer_id: str, retailer_name: str) -> int:
    """
    Save parsed price data to database.
    
    Args:
        rows: List of dicts with keys: name, barcode, price, date, company
        retailer_id: Retailer slug/ID
        retailer_name: Retailer display name
    
    Returns:
        Number of price snapshots successfully saved
    """
    if not rows:
        return 0
    
    # Upsert retailer first
    db_retailer_id = await upsert_retailer(retailer_id, retailer_name)
    if not db_retailer_id:
        logger.warning("db.save_parsed_prices.skipped retailer=%s reason=retailer_upsert_failed", retailer_id)
        return 0
    
    saved_count = 0
    
    for row in rows:
        barcode = row.get("barcode", "").strip()
        name = row.get("name", "").strip()
        price_str = row.get("price", "").strip()
        
        if not barcode or not name or not price_str:
            continue
        
        # Parse price
        try:
            price = float(price_str)
        except (ValueError, TypeError):
            logger.debug("db.price.parse_failed barcode=%s price=%s", barcode, price_str)
            continue
        
        # Parse date if available
        timestamp = None
        date_str = row.get("date")
        if date_str:
            try:
                # Try common date formats
                for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%d-%m-%Y"]:
                    try:
                        timestamp = datetime.strptime(date_str.strip(), fmt)
                        break
                    except ValueError:
                        continue
            except Exception:
                pass
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Upsert product
        db_product_id = await upsert_product(
            barcode=barcode,
            name=name,
            brand=None,  # Could extract from name if needed
            size=None,
            unit=None,
            category=None
        )
        
        if not db_product_id:
            continue
        
        # Create price snapshot
        snapshot_id = await create_price_snapshot(
            product_id=db_product_id,
            retailer_id=db_retailer_id,
            price=price,
            currency="ILS",
            is_on_sale=False,  # Could detect from context
            timestamp=timestamp,
            store_id=None  # Could extract/store if available
        )
        
        if snapshot_id:
            saved_count += 1
    
    logger.info("db.save_parsed_prices retailer=%s saved=%d total=%d", retailer_id, saved_count, len(rows))
    return saved_count

