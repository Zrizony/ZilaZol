# crawler/parsers.py
from __future__ import annotations
import json
import os
import re
from typing import List, Optional

from lxml import etree

from . import logger
from .archive_utils import iter_xml_entries, sniff_kind
from .gcs import get_bucket, upload_to_gcs
from .db import save_parsed_prices, save_parsed_stores


def _first_text(elem, *paths) -> Optional[str]:
    """Extract first non-empty text from element using multiple XPath paths."""
    for p in paths:
        r = elem.find(p)
        if r is not None and (t := (r.text or "").strip()):
            return t
    return None


def extract_store_id(filename: str) -> Optional[str]:
    # Matches "7290...-045-2025..." -> returns "045"
    match = re.search(r"(\d+)-(\d+)-\d+", filename)
    if match:
        return match.group(2)
    return None


def parse_stores_xml(xml_bytes: bytes) -> List[dict]:
    """Parses Stores7290...xml to extract store metadata."""
    rows = []
    try:
        root = etree.fromstring(xml_bytes)
        # Traverse <SubChains> -> <SubChain> -> <Stores> -> <Store>
        for store in root.findall(".//Store"):
            ext_id = _first_text(store, "StoreId", "StoreID", "storeid")
            name = _first_text(store, "StoreName", "StoreNm", "Name")
            city = _first_text(store, "City", "CityName")
            address = _first_text(store, "Address", "Street")
            
            if ext_id:
                rows.append({
                    "external_id": ext_id,
                    "name": name,
                    "city": city,
                    "address": address
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

    # 1. Handle PROMOS (Promo/PromoFull) - Nested Structure
    promotions = root.findall(".//Promotion")
    if promotions:
        for promo in promotions:
            price = _first_text(promo, "DiscountedPrice", "DiscountRate")
            date = _first_text(promo, "PromotionUpdateDate", "UpdateDate", "PromotionStartDate")
            
            for item in promo.findall(".//Item"):
                barcode = _first_text(item, "ItemCode", "Barcode")
                if barcode and price:
                    rows.append({
                        "barcode": barcode,
                        "price": price,
                        "date": date,
                        "company": company,
                        "store_id": store_id,
                        "is_on_sale": True,
                        "name": None # Promos don't have names
                    })
        return rows

    # 2. Handle PRICES (Price/PriceFull) - Flat Structure
    items = root.findall(".//Item")
    if not items: items = list(root)

    for it in items:
        name = _first_text(it, "ItemName", "ItemDescription", "Description")
        barcode = _first_text(it, "ItemCode", "Barcode")
        price = _first_text(it, "ItemPrice", "Price")
        date = _first_text(it, "PriceUpdateDate", "UpdateDate")
        
        # Metadata (Brand, Unit, Qty)
        brand = _first_text(it, "ManufacturerName", "BrandName")
        unit = _first_text(it, "UnitQty", "UnitOfMeasure")
        qty_str = _first_text(it, "Quantity", "Content", "QtyInPackage")
        weighted_str = _first_text(it, "bIsWeighted", "BisWeighted")

        if not (barcode and price): continue
            
        is_weighted = False
        if weighted_str and weighted_str.lower() in ("1", "true", "y"):
            is_weighted = True
            
        qty = None
        if qty_str:
            try: qty = float(qty_str)
            except ValueError: pass

        rows.append({
            "name": name,
            "barcode": barcode,
            "date": date,
            "price": price,
            "company": company,
            "store_id": store_id,
            "is_on_sale": False,
            "brand": brand,
            "unit": unit,
            "quantity": qty,
            "is_weighted": is_weighted
        })
    return rows


async def parse_from_blob(data: bytes, filename_hint: str, retailer_id: str, run_id: str) -> int:
    kind = sniff_kind(data)
    logger.info("file.downloaded retailer=%s file=%s kind=%s bytes=%d", retailer_id, filename_hint, kind, len(data))
    
    xml_count = 0
    bucket = get_bucket()
    
    # Identify file type
    is_store_file = "Store" in filename_hint and "Price" not in filename_hint
    store_ext_id = extract_store_id(filename_hint) if not is_store_file else None

    for inner_name, xml_bytes in iter_xml_entries(data, filename_hint=filename_hint):
        xml_count += 1
        try:
            rows = []
            if is_store_file:
                # Parse Store Metadata
                rows = parse_stores_xml(xml_bytes)
                if rows:
                    await save_parsed_stores(rows, retailer_id)
                    logger.info("db.saved_stores count=%d", len(rows))
            else:
                # Parse Prices/Promos
                rows = parse_prices_xml(xml_bytes, company=retailer_id, store_id=store_ext_id)
                if rows:
                    retailer_name = retailer_id # (Simplify config lookup for brevity)
                    await save_parsed_prices(rows, retailer_id, retailer_name)
            
            # Upload JSONL to GCS (Optional, for backup)
            if rows and bucket:
                jsonl_data = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
                blob_path = f"json/{retailer_id}/{os.path.splitext(inner_name)[0]}.jsonl"
                await upload_to_gcs(bucket, blob_path, jsonl_data.encode('utf-8'), "application/json")
                
        except Exception as e:
            logger.warning("xml.parse_failed retailer=%s file=%s err=%s", retailer_id, filename_hint, e)
    
    return xml_count

