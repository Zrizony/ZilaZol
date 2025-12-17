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
    global _pool
    if _pool is not None: return _pool
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.warning("DATABASE_URL not set")
        return None
    try:
        _pool = await asyncpg.create_pool(database_url, min_size=1, max_size=5, command_timeout=60)
        return _pool
    except Exception as e:
        logger.error("db.pool.failed error=%s", str(e))
        return None

async def close_pool():
    global _pool
    if _pool: await _pool.close(); _pool = None


async def upsert_retailer(retailer_id: str, name: str) -> Optional[int]:
    pool = await get_pool()
    if not pool: return None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO retailers (slug, name, "createdAt", "updatedAt")
                VALUES ($1, $2, NOW(), NOW())
                ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name, "updatedAt" = NOW()
                RETURNING id
            """, retailer_id, name)
            return row['id'] if row else None
    except Exception: return None


async def upsert_store(retailer_db_id: int, external_id: str, name: str = None, 
                       city: str = None, address: str = None) -> Optional[int]:
    if not external_id: return None
    pool = await get_pool()
    if not pool: return None
    
    display_name = name or f"Store {external_id}"
    
    try:
        async with pool.acquire() as conn:
            # Uses COALESCE to keep existing data if new data is NULL
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


async def upsert_product(barcode: str, name: str = None, brand: str = None, 
                         quantity: float = None, unit: str = None,
                         is_weighted: bool = False, category: str = None) -> Optional[int]:
    pool = await get_pool()
    if not pool: return None
    
    placeholder_name = name or f"Unknown ({barcode})"
    
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO products (barcode, name, brand, quantity, unit, "isWeighted", category, "createdAt", "updatedAt")
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
                ON CONFLICT (barcode) 
                DO UPDATE SET 
                    -- Only update if new value is NOT NULL (Protects data from Promo files)
                    name = COALESCE(EXCLUDED.name, products.name),
                    brand = COALESCE(EXCLUDED.brand, products.brand),
                    quantity = COALESCE(EXCLUDED.quantity, products.quantity),
                    unit = COALESCE(EXCLUDED.unit, products.unit),
                    "isWeighted" = COALESCE(EXCLUDED."isWeighted", products."isWeighted"),
                    "updatedAt" = NOW()
                RETURNING id
            """, barcode, name if name else placeholder_name, brand, quantity, unit, is_weighted, category)
            return row['id'] if row else None
    except Exception: return None


async def create_price_snapshot(product_id: int, retailer_id: int, price: float,
                                currency: str = "ILS", is_on_sale: bool = False,
                                timestamp: Optional[datetime] = None,
                                store_id: Optional[int] = None) -> Optional[int]:
    pool = await get_pool()
    if not pool: return None
    if timestamp is None: timestamp = datetime.utcnow()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO price_snapshots 
                    (product_id, retailer_id, store_id, price, currency, "isOnSale", timestamp, "seenAt")
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                RETURNING id
            """, product_id, retailer_id, store_id, price, currency, is_on_sale, timestamp)
            return row['id'] if row else None
    except Exception: return None


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
    if not rows: return 0
    db_retailer_id = await upsert_retailer(retailer_id, retailer_name)
    if not db_retailer_id: return 0
    
    saved_count = 0
    store_cache = {}

    for row in rows:
        price_str = row.get("price", "")
        try: price = float(price_str)
        except: continue
        
        timestamp = datetime.utcnow()
        if row.get("date"):
            try: timestamp = datetime.strptime(row.get("date")[:19], "%Y-%m-%d %H:%M:%S")
            except: pass

        db_store_id = None
        ext_store_id = row.get("store_id")
        if ext_store_id:
            if ext_store_id in store_cache:
                db_store_id = store_cache[ext_store_id]
            else:
                db_store_id = await upsert_store(db_retailer_id, ext_store_id)
                if db_store_id: store_cache[ext_store_id] = db_store_id

        db_product_id = await upsert_product(
            barcode=row.get("barcode", ""),
            name=row.get("name"),
            brand=row.get("brand"),
            quantity=row.get("quantity"),
            unit=row.get("unit"),
            is_weighted=row.get("is_weighted", False)
        )
        
        if db_product_id:
            await create_price_snapshot(
                product_id=db_product_id,
                retailer_id=db_retailer_id,
                store_id=db_store_id,
                price=price,
                is_on_sale=row.get("is_on_sale", False),
                timestamp=timestamp
            )
            saved_count += 1
    
    logger.info("db.saved retailer=%s count=%d/%d", retailer_id, saved_count, len(rows))
    return saved_count

