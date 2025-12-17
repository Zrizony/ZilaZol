# crawler/db.py
from __future__ import annotations
import os
import asyncpg
from typing import Optional, List, Dict
from datetime import datetime
from . import logger

_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> Optional[asyncpg.Pool]:
    global _pool
    if _pool: return _pool
    if not os.getenv("DATABASE_URL"): return None
    _pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"), min_size=1, max_size=5)
    return _pool

async def upsert_retailer(retailer_id: str, name: str) -> Optional[int]:
    pool = await get_pool()
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO retailers (slug, name, "createdAt", "updatedAt")
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (slug) DO UPDATE SET "updatedAt" = NOW()
            RETURNING id
        """, retailer_id, name)
        return row['id'] if row else None

async def upsert_store(retailer_db_id: int, external_id: str, name: str = None, 
                       city: str = None, address: str = None) -> Optional[int]:
    if not external_id: return None
    pool = await get_pool()
    if not pool: return None
    
    display_name = name or f"Store {external_id}"
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

async def upsert_product(barcode: str, name: str = None, brand: str = None, 
                         quantity: float = None, unit: str = None,
                         is_weighted: bool = False) -> Optional[int]:
    pool = await get_pool()
    if not pool: return None
    
    placeholder_name = name or f"Unknown ({barcode})"
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO products (barcode, name, brand, quantity, unit, "isWeighted", "createdAt", "updatedAt")
            VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
            ON CONFLICT (barcode) 
            DO UPDATE SET 
                name = COALESCE(EXCLUDED.name, products.name),
                brand = COALESCE(EXCLUDED.brand, products.brand),
                quantity = COALESCE(EXCLUDED.quantity, products.quantity),
                unit = COALESCE(EXCLUDED.unit, products.unit),
                "isWeighted" = COALESCE(EXCLUDED."isWeighted", products."isWeighted"),
                "updatedAt" = NOW()
            RETURNING id
        """, barcode, name if name else placeholder_name, brand, quantity, unit, is_weighted)
        return row['id'] if row else None

async def create_price_snapshot(product_id: int, retailer_id: int, price: float,
                                is_on_sale: bool, timestamp: datetime, store_id: Optional[int]) -> Optional[int]:
    pool = await get_pool()
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO price_snapshots 
                (product_id, retailer_id, store_id, price, "isOnSale", timestamp, "seenAt")
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            RETURNING id
        """, product_id, retailer_id, store_id, price, is_on_sale, timestamp)
        return row['id'] if row else None

async def save_parsed_stores(rows: List[Dict], retailer_id: str) -> int:
    db_retailer_id = await upsert_retailer(retailer_id, retailer_id)
    if not db_retailer_id: return 0
    count = 0
    for row in rows:
        if await upsert_store(db_retailer_id, row.get("external_id"), row.get("name"), row.get("city"), row.get("address")):
            count += 1
    return count

async def save_parsed_prices(rows: List[Dict], retailer_id: str, retailer_name: str) -> int:
    if not rows: return 0
    db_retailer_id = await upsert_retailer(retailer_id, retailer_name)
    if not db_retailer_id: return 0
    
    saved_count = 0
    store_cache = {}

    for row in rows:
        try:
            price = float(row.get("price", "0"))
        except: continue
        
        timestamp = datetime.utcnow()
        if row.get("date"):
            try: timestamp = datetime.strptime(row.get("date")[:19], "%Y-%m-%d %H:%M:%S")
            except: pass

        # 1. Upsert Store
        db_store_id = None
        ext_store_id = row.get("store_id")
        if ext_store_id:
            if ext_store_id in store_cache:
                db_store_id = store_cache[ext_store_id]
            else:
                db_store_id = await upsert_store(db_retailer_id, ext_store_id)
                if db_store_id: store_cache[ext_store_id] = db_store_id

        # 2. Upsert Product
        db_product_id = await upsert_product(
            barcode=row.get("barcode", ""),
            name=row.get("name"),
            brand=row.get("brand"),
            quantity=row.get("quantity"),
            unit=row.get("unit"),
            is_weighted=row.get("is_weighted", False)
        )
        
        # 3. Create Snapshot
        if db_product_id:
            try:
                await create_price_snapshot(
                    product_id=db_product_id,
                    retailer_id=db_retailer_id,
                    price=price,
                    is_on_sale=row.get("is_on_sale", False),
                    timestamp=timestamp,
                    store_id=db_store_id
                )
                saved_count += 1
            except Exception as e:
                logger.error(f"Snapshot insert failed: {e}")

    logger.info(f"db.saved retailer={retailer_id} count={saved_count}/{len(rows)}")
    return saved_count
