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
                INSERT INTO retailers (slug, name, "isActive", "createdAt", "updatedAt")
                VALUES ($1, $2, true, NOW(), NOW())
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


async def upsert_store(retailer_db_id: int, external_id: str, name: str = None, 
                       city: str = None, address: str = None) -> Optional[int]:
    if not external_id: return None
    pool = await get_pool()
    if not pool: return None
    
    # If name/address provided, update them. If not, preserve existing values.
    display_name = name or f"Store {external_id}"
    
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO stores ("retailerId", "externalId", name, city, address, "createdAt", "updatedAt")
                VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                ON CONFLICT ("retailerId", "externalId") 
                DO UPDATE SET 
                    name = COALESCE(EXCLUDED.name, stores.name),
                    city = COALESCE(EXCLUDED.city, stores.city),
                    address = COALESCE(EXCLUDED.address, stores.address),
                    "updatedAt" = NOW()
                RETURNING id
            """, retailer_db_id, external_id, display_name, city, address)
            return row['id'] if row else None
    except Exception as e:
        logger.error("db.store.upsert.failed ext_id=%s error=%s", external_id, str(e))
        return None


async def upsert_product(barcode: str, name: Optional[str] = None, brand: Optional[str] = None, 
                         quantity: Optional[float] = None, unit: Optional[str] = None,
                         is_weighted: bool = False, category: Optional[str] = None) -> Optional[int]:
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
                INSERT INTO products (barcode, name, brand, quantity, unit, "isWeighted", category, "createdAt", "updatedAt")
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
                ON CONFLICT (barcode) 
                DO UPDATE SET 
                    name = COALESCE(EXCLUDED.name, products.name),
                    brand = COALESCE(EXCLUDED.brand, products.brand),
                    quantity = COALESCE(EXCLUDED.quantity, products.quantity),
                    unit = COALESCE(EXCLUDED.unit, products.unit),
                    "isWeighted" = COALESCE(EXCLUDED."isWeighted", products."isWeighted"),
                    "updatedAt" = NOW()
                RETURNING id
            """, barcode, name, brand, quantity, unit, is_weighted, category)
            return row['id'] if row else None
    except Exception as e:
        logger.error("db.product.upsert.failed barcode=%s error=%s", barcode, str(e))
        return None


async def create_price_snapshot(product_id: int, retailer_id: int, price: float,
                                store_id: Optional[int] = None, currency: str = "ILS", 
                                is_on_sale: bool = False, timestamp: Optional[datetime] = None,
                                seen_at: Optional[datetime] = None) -> Optional[int]:
    """
    Create a price snapshot in the database.
    store_id is optional (can be None for general prices).
    Returns the snapshot's database ID, or None if failed.
    """
    pool = await get_pool()
    if not pool:
        return None
    
    if timestamp is None:
        timestamp = datetime.utcnow()
    
    if seen_at is None:
        seen_at = datetime.utcnow()
    
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO price_snapshots 
                    (product_id, retailer_id, store_id, price, currency, "isOnSale", timestamp, "seenAt")
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id
            """, product_id, retailer_id, store_id, price, currency, is_on_sale, timestamp, seen_at)
            return row['id'] if row else None
    except Exception as e:
        logger.error("db.price_snapshot.create.failed product_id=%d retailer_id=%d store_id=%s error=%s", 
                    product_id, retailer_id, store_id, str(e))
        return None


async def save_parsed_stores(rows: List[Dict], retailer_id: str) -> int:
    """Bulk saver for Stores files."""
    db_retailer_id = await upsert_retailer(retailer_id, retailer_id)
    if not db_retailer_id: return 0
    
    count = 0
    for row in rows:
        if await upsert_store(
            retailer_db_id=db_retailer_id,
            external_id=row.get("external_id"),
            name=row.get("name"),
            city=row.get("city"),
            address=row.get("address")
        ):
            count += 1
    return count


async def save_parsed_prices(rows: List[Dict], retailer_id: str, retailer_name: str) -> int:
    """
    Save parsed price data to database.
    
    Args:
        rows: List of dicts with keys: name, barcode, price, date, company, store_id, brand, unit, quantity, is_weighted, is_on_sale
        retailer_id: Retailer slug/ID
        retailer_name: Retailer display name
    
    Returns:
        Number of price snapshots successfully saved
    """
    if not rows:
        return 0
    
    # 1. Upsert Retailer
    db_retailer_id = await upsert_retailer(retailer_id, retailer_name)
    if not db_retailer_id:
        logger.warning("db.save_parsed_prices.skipped retailer=%s reason=retailer_upsert_failed", retailer_id)
        return 0
    
    saved_count = 0
    
    # Cache store IDs locally to avoid excessive DB calls
    store_cache = {}
    
    # Track seen_at time (when we crawled this data)
    seen_at = datetime.utcnow()
    
    for row in rows:
        barcode = row.get("barcode", "").strip()
        name = row.get("name")
        if name:
            name = name.strip()
        price_str = row.get("price", "").strip()
        
        if not barcode or not price_str:
            continue
        
        try:
            price = float(price_str)
        except (ValueError, TypeError):
            continue
        
        # Parse timestamp from file (when price was reported)
        timestamp = datetime.utcnow()
        if row.get("date"):
            try:
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"]:
                    try:
                        timestamp = datetime.strptime(row.get("date").strip(), fmt)
                        break
                    except ValueError:
                        continue
            except Exception:
                pass
        
        # 2. Upsert Store (optional - can be None)
        db_store_id = None
        ext_store_id = row.get("store_id")
        
        if ext_store_id:
            if ext_store_id in store_cache:
                db_store_id = store_cache[ext_store_id]
            else:
                db_store_id = await upsert_store(db_retailer_id, ext_store_id)
                if db_store_id:
                    store_cache[ext_store_id] = db_store_id
        
        # 3. Upsert Product (with normalization fields)
        db_product_id = await upsert_product(
            barcode=barcode,
            name=name,
            brand=row.get("brand"),
            quantity=row.get("quantity"),
            unit=row.get("unit"),
            is_weighted=row.get("is_weighted", False),
            category=None
        )
        
        if not db_product_id:
            continue
        
        # 4. Create Snapshot (store_id is optional)
        snapshot_id = await create_price_snapshot(
            product_id=db_product_id,
            retailer_id=db_retailer_id,
            price=price,
            store_id=db_store_id,  # Optional - can be None
            is_on_sale=row.get("is_on_sale", False),
            timestamp=timestamp,  # When price was reported in file
            seen_at=seen_at  # When we crawled it
        )
        
        if snapshot_id:
            saved_count += 1
    
    logger.info("db.saved retailer=%s count=%d/%d", retailer_id, saved_count, len(rows))
    return saved_count

