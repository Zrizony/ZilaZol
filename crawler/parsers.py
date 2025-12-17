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
    """
    Extracts the first non-empty text found.
    Returns the RAW string (stripped of whitespace).
    Does NOT filter out '0', 'Unknown', etc.
    """
    for p in paths:
        r = elem.find(p)
        if r is not None and r.text:
            t = r.text.strip()
            if t:
                return t
    return None


def extract_store_id(filename: str) -> Optional[str]:
    # Matches "7290...-045-2025..." -> returns "045"
    match = re.search(r"(\d+)-(\d+)-\d+", filename)
    if match:
        return match.group(2)
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
    promotions = root.findall(".//Promotion")
    if promotions:
        for promo in promotions:
            # For Promos, the price is the "DiscountedPrice"
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
                        "is_on_sale": True, # Flag as SALE price
                        "name": None
                    })
        return rows

    # 2. Handle PRICES (Price/PriceFull)
    items = root.findall(".//Item")
    if not items: items = list(root)

    for it in items:
        barcode = _first_text(it, "ItemCode", "Barcode")
        # For Regular Price files, the price is "ItemPrice"
        price = _first_text(it, "ItemPrice", "Price")
        
        if not (barcode and price): continue

        qty_str = _first_text(it, "Quantity", "Content", "QtyInPackage")
        qty = None
        if qty_str:
            try: qty = float(qty_str)
            except ValueError: pass

        weighted_str = _first_text(it, "bIsWeighted", "BisWeighted")
        is_weighted = False
        if weighted_str and weighted_str.lower() in ("1", "true", "y"):
            is_weighted = True

        rows.append({
            "name": _first_text(it, "ItemName", "ItemNm", "ItemDescription", "Description"),
            "barcode": barcode,
            "date": _first_text(it, "PriceUpdateDate", "UpdateDate"),
            "price": price,
            "company": company,
            "store_id": store_id,
            "is_on_sale": False, # Flag as REGULAR price
            "brand": _first_text(it, "ManufacturerName", "BrandName"),
            "unit": _first_text(it, "UnitQty", "UnitOfMeasure"),
            "quantity": qty,
            "is_weighted": is_weighted
        })
    return rows


async def parse_from_blob(data: bytes, filename_hint: str, retailer_id: str, run_id: str) -> int:
    kind = sniff_kind(data)
    logger.info("file.downloaded retailer=%s file=%s kind=%s bytes=%d", retailer_id, filename_hint, kind, len(data))
    
    xml_count = 0
    bucket = get_bucket()
    
    # Store files have "Store" in name but NOT "Price" (to distinguish from StorePrice files if any)
    is_store_file = "Store" in filename_hint and "Price" not in filename_hint
    store_ext_id = extract_store_id(filename_hint) if not is_store_file else None

    for inner_name, xml_bytes in iter_xml_entries(data, filename_hint=filename_hint):
        xml_count += 1
        try:
            rows = []
            if is_store_file:
                rows = parse_stores_xml(xml_bytes)
                if rows:
                    await save_parsed_stores(rows, retailer_id)
            else:
                rows = parse_prices_xml(xml_bytes, company=retailer_id, store_id=store_ext_id)
                if rows:
                    retailer_name = retailer_id 
                    await save_parsed_prices(rows, retailer_id, retailer_name)
            
            if rows and bucket:
                jsonl_data = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
                blob_path = f"json/{retailer_id}/{os.path.splitext(inner_name)[0]}.jsonl"
                await upload_to_gcs(bucket, blob_path, jsonl_data.encode('utf-8'), "application/json")
                
        except Exception as e:
            logger.warning("xml.parse_failed retailer=%s file=%s err=%s", retailer_id, filename_hint, e)
    
    return xml_count

