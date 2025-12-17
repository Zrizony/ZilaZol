# crawler/parsers.py
from __future__ import annotations
import re
from typing import List, Optional
from lxml import etree
from . import logger
from .archive_utils import iter_xml_entries, sniff_kind
from .gcs import get_bucket, upload_to_gcs
from .db import save_parsed_prices, save_parsed_stores


def _first_text(elem, *paths) -> Optional[str]:
    """Returns the RAW text found in paths. No cleaning/filtering."""
    for p in paths:
        r = elem.find(p)
        if r is not None and r.text:
            t = r.text.strip()
            if t: return t
    return None


def extract_store_id(filename: str) -> Optional[str]:
    """Extracts store ID from filename (e.g. '004' from 'PriceFull...-004-...')"""
    match = re.search(r"(\d+)-(\d+)-\d+", filename)
    if match: return match.group(2)
    return None


def parse_stores_xml(xml_bytes: bytes) -> List[dict]:
    rows = []
    try:
        root = etree.fromstring(xml_bytes)
        for store in root.findall(".//Store"):
            ext_id = _first_text(store, "StoreId", "StoreID", "storeid")
            if ext_id:
                rows.append({
                    "external_id": ext_id,
                    "name": _first_text(store, "StoreName", "StoreNm", "Name"),
                    "city": _first_text(store, "City", "CityName"),
                    "address": _first_text(store, "Address", "Street")
                })
    except Exception as e:
        logger.warning(f"Failed to parse stores XML: {e}")
    return rows


def parse_prices_xml(xml_bytes: bytes, company: str, store_id: str = None) -> List[dict]:
    rows: List[dict] = []
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return rows

    # 1. Handle PROMOS (Promo/PromoFull)
    # Price is in "DiscountedPrice", IS on sale
    for promo in root.findall(".//Promotion"):
        price = _first_text(promo, "DiscountedPrice", "DiscountRate")
        date = _first_text(promo, "PromotionUpdateDate", "UpdateDate", "PromotionStartDate")
        
        if not price: continue
        
        for item in promo.findall(".//Item"):
            barcode = _first_text(item, "ItemCode", "Barcode")
            if barcode:
                rows.append({
                    "barcode": barcode,
                    "price": price,
                    "date": date,
                    "company": company,
                    "store_id": store_id,
                    "is_on_sale": True,
                    "name": None # Promos often lack names
                })

    # 2. Handle PRICES (Price/PriceFull)
    # Price is in "ItemPrice", NOT on sale
    items = root.findall(".//Item")
    if not items: items = list(root)
    
    for it in items:
        barcode = _first_text(it, "ItemCode", "Barcode")
        price = _first_text(it, "ItemPrice", "Price")
        
        if not (barcode and price): continue

        # Extract Raw Metadata
        qty_str = _first_text(it, "Quantity", "Content", "QtyInPackage")
        qty = None
        if qty_str:
            try: qty = float(qty_str)
            except: pass # Keep None if not a valid number
        
        weighted_str = _first_text(it, "bIsWeighted", "BisWeighted")
        is_weighted = (weighted_str and weighted_str.lower() in ("1", "true", "y"))

        rows.append({
            "name": _first_text(it, "ItemName", "ItemNm", "ItemDescription", "Description"),
            "barcode": barcode,
            "date": _first_text(it, "PriceUpdateDate", "UpdateDate"),
            "price": price,
            "company": company,
            "store_id": store_id,
            "is_on_sale": False,
            "brand": _first_text(it, "ManufacturerName", "BrandName"),
            "unit": _first_text(it, "UnitQty", "UnitOfMeasure"),
            "quantity": qty,
            "is_weighted": is_weighted
        })
    return rows


async def parse_from_blob(data: bytes, filename_hint: str, retailer_id: str, run_id: str) -> int:
    kind = sniff_kind(data)
    logger.info("file.downloaded retailer=%s file=%s kind=%s bytes=%d", retailer_id, filename_hint, kind, len(data))
    
    # Extract store ID once per file
    is_store_file = "Store" in filename_hint and "Price" not in filename_hint
    store_ext_id = extract_store_id(filename_hint) if not is_store_file else None

    count = 0
    for inner_name, xml_bytes in iter_xml_entries(data, filename_hint=filename_hint):
        count += 1
        try:
            if is_store_file:
                rows = parse_stores_xml(xml_bytes)
                if rows: await save_parsed_stores(rows, retailer_id)
            else:
                rows = parse_prices_xml(xml_bytes, company=retailer_id, store_id=store_ext_id)
                if rows: 
                    # Use the ID as the name fallback to ensure we save *something*
                    await save_parsed_prices(rows, retailer_id, retailer_id)
        except Exception as e:
            logger.warning(f"Parse error: {e}")
    return count
