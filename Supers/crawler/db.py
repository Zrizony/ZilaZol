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

async def close_pool():
    """Close the database connection pool"""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

async def upsert_retailer(retailer_id: str, name: str, need_creds: Optional[bool] = None) -> Optional[int]:
    """
    Upsert retailer. If need_creds is None, preserve existing value on update.
    """
    pool = await get_pool()
    if not pool: return None
    async with pool.acquire() as conn:
        if need_creds is None:
            # Don't update needCreds - preserve existing value
            row = await conn.fetchrow("""
                INSERT INTO retailers (slug, name, "needCreds", "createdAt", "updatedAt")
                VALUES ($1, $2, false, NOW(), NOW())
                ON CONFLICT (slug) DO UPDATE SET 
                    name = EXCLUDED.name,
                    "updatedAt" = NOW()
                RETURNING id
            """, retailer_id, name)
        else:
            # Update needCreds explicitly
            row = await conn.fetchrow("""
                INSERT INTO retailers (slug, name, "needCreds", "createdAt", "updatedAt")
                VALUES ($1, $2, $3, NOW(), NOW())
                ON CONFLICT (slug) DO UPDATE SET 
                    name = EXCLUDED.name,
                    "needCreds" = EXCLUDED."needCreds",
                    "updatedAt" = NOW()
                RETURNING id
            """, retailer_id, name, need_creds)
        return row['id'] if row else None

async def fetch_retailer_slugs(need_creds: Optional[bool] = None) -> List[str]:
    """Fetch retailer slugs from database, optionally filtered by needCreds"""
    pool = await get_pool()
    if not pool: return []
    
    try:
        async with pool.acquire() as conn:
            if need_creds is None:
                # Fetch all enabled retailers
                rows = await conn.fetch("""
                    SELECT slug FROM retailers 
                    WHERE "isActive" = true
                    ORDER BY slug
                """)
            else:
                # Filter by needCreds
                rows = await conn.fetch("""
                    SELECT slug FROM retailers 
                    WHERE "isActive" = true AND "needCreds" = $1
                    ORDER BY slug
                """, need_creds)
            return [row['slug'] for row in rows]
    except Exception as e:
        logger.error(f"db.fetch_retailer_slugs.failed error={e}")
        return []

async def upsert_store(retailer_db_id: int, external_id: str, name: str = None, 
                       city: str = None, address: str = None) -> Optional[int]:
    if not external_id: return None
    pool = await get_pool()
    if not pool: return None
    
    display_name = name or f"Store {external_id}"
    
    # Log when we're trying to update with address/city data
    if address or city:
        logger.info(f"upsert_store retailer_id={retailer_db_id} ext_id={external_id} "
                   f"name={display_name} city={city} address={address}")
    
    async with pool.acquire() as conn:
        # Use COALESCE but allow NULL to overwrite if we explicitly want to clear it
        # However, we'll prefer non-NULL values: if EXCLUDED has a value, use it; otherwise keep existing
        row = await conn.fetchrow("""
            INSERT INTO stores ("retailerId", "externalId", name, city, address, "createdAt", "updatedAt")
            VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
            ON CONFLICT ("retailerId", "externalId") 
            DO UPDATE SET 
                name = COALESCE(NULLIF(EXCLUDED.name, ''), stores.name),
                city = COALESCE(NULLIF(EXCLUDED.city, ''), stores.city),
                address = COALESCE(NULLIF(EXCLUDED.address, ''), stores.address),
                "updatedAt" = NOW()
            RETURNING id
        """, retailer_db_id, external_id, display_name, city, address)
        return row['id'] if row else None

async def upsert_product(barcode: str, name: str = None, brand: str = None, 
                         quantity: float = None, unit: str = None,
                         is_weighted: bool = False, image_url: str = None) -> Optional[int]:
    pool = await get_pool()
    if not pool: return None
    
    placeholder_name = name or f"Unknown ({barcode})"
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO products (barcode, name, brand, quantity, unit, "isWeighted", "imageUrl", "createdAt", "updatedAt")
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
            ON CONFLICT (barcode) 
            DO UPDATE SET 
                name = COALESCE(NULLIF(EXCLUDED.name, ''), products.name),
                brand = COALESCE(NULLIF(EXCLUDED.brand, ''), products.brand),
                quantity = COALESCE(EXCLUDED.quantity, products.quantity),
                unit = COALESCE(NULLIF(EXCLUDED.unit, ''), products.unit),
                "isWeighted" = COALESCE(EXCLUDED."isWeighted", products."isWeighted"),
                "imageUrl" = COALESCE(NULLIF(EXCLUDED."imageUrl", ''), products."imageUrl"),
                "updatedAt" = NOW()
            RETURNING id
        """, barcode, name if name else placeholder_name, brand, quantity, unit, is_weighted, image_url)
        return row['id'] if row else None

async def create_price_snapshot(product_id: int, retailer_id: int, price: float,
                                is_on_sale: bool, timestamp: datetime, store_id: Optional[int]) -> Optional[int]:
    pool = await get_pool()
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO price_snapshots 
                ("productId", "retailerId", "storeId", price, "isOnSale", timestamp, "seenAt")
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

async def save_parsed_prices(rows: List[Dict], retailer_id: str, retailer_name: str, store_metadata: Dict = None) -> int:
    if not rows: return 0
    db_retailer_id = await upsert_retailer(retailer_id, retailer_name)
    if not db_retailer_id: return 0
    
    saved_count = 0
    store_cache = {}
    
    # Extract store metadata from the first row or from store_metadata parameter
    # store_metadata comes from parse_prices_xml and contains city/address from XML root
    default_store_name = store_metadata.get("name") if store_metadata else None
    default_store_city = store_metadata.get("city") if store_metadata else None
    default_store_address = store_metadata.get("address") if store_metadata else None

    for row in rows:
        try:
            price = float(row.get("price", "0"))
        except: continue
        
        timestamp = datetime.utcnow()
        if row.get("date"):
            try: timestamp = datetime.strptime(row.get("date")[:19], "%Y-%m-%d %H:%M:%S")
            except: pass

        # 1. Upsert Store with metadata (city, address) if available
        db_store_id = None
        ext_store_id = row.get("store_id") or (store_metadata.get("store_id") if store_metadata else None)
        if ext_store_id:
            if ext_store_id in store_cache:
                db_store_id = store_cache[ext_store_id]
            else:
                # Use metadata from XML if available, otherwise just use store_id
                db_store_id = await upsert_store(
                    db_retailer_id, 
                    ext_store_id,
                    name=default_store_name,
                    city=default_store_city,
                    address=default_store_address
                )
                if db_store_id: store_cache[ext_store_id] = db_store_id

        # 2. Upsert Product
        db_product_id = await upsert_product(
            barcode=row.get("barcode", ""),
            name=row.get("name"),
            brand=row.get("brand"),
            quantity=row.get("quantity"),
            unit=row.get("unit"),
            is_weighted=row.get("is_weighted", False),
            image_url=row.get("image_url")
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


async def fetch_stores_with_retailer() -> List[Dict]:
    """Fetch all stores with their retailer information."""
    pool = await get_pool()
    if not pool: return []
    
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    s.id,
                    s."externalId",
                    s.name,
                    s.city,
                    s.address,
                    s."createdAt",
                    s."updatedAt",
                    r.id as "retailerId",
                    r.name as "retailerName",
                    r.slug as "retailerSlug"
                FROM stores s
                INNER JOIN retailers r ON s."retailerId" = r.id
                ORDER BY s."createdAt" DESC
            """)
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"db.fetch_stores.failed error={e}")
        return []
