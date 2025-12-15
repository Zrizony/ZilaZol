# crawler/parsers.py
from __future__ import annotations
import json
import os
import re
from typing import List, Optional

from lxml import etree

from . import logger
from .archive_utils import iter_xml_entries, sniff_kind, md5_hex
from .gcs import get_bucket, upload_to_gcs
from .db import save_parsed_prices


def _first_text(elem, *paths) -> Optional[str]:
    """Extract first non-empty text from element using multiple XPath paths."""
    for p in paths:
        r = elem.find(p)
        if r is not None and (t := (r.text or "").strip()):
            return t
    return None


def extract_store_id(filename: str) -> Optional[str]:
    """
    Extract Store ID from standard Israeli filename format.
    Format is typically: [Type]_[ChainID]-[StoreID]-[Date].gz
    """
    # Pattern: Matches "7290..." (Chain ID) followed by "-" and "digits" (Store ID)
    match = re.search(r"(\d+)-(\d+)-\d+", filename)
    if match:
        return match.group(2)
    return None


def parse_prices_xml(xml_bytes: bytes, company: str, store_id: str = None) -> List[dict]:
    """Parse XML bytes into price item rows with extended metadata."""
    rows: List[dict] = []
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return rows

    items = root.findall(".//Item")
    if not items:
        items = list(root)

    for it in items:
        # Basic Info
        name = _first_text(
            it,
            "ItemName",
            "ManufacturerItemDescription",
            "Description",
            "itemname",
            "name",
        )
        barcode = _first_text(
            it, "ItemCode", "Barcode", "ItemBarcode", "itemcode", "barcode", "Code"
        )
        price = _first_text(it, "ItemPrice", "Price", "price")
        date = _first_text(
            it, "PriceUpdateDate", "UpdateDate", "LastUpdateDate", "date"
        )
        
        # Extended Metadata (New)
        brand = _first_text(it, "ManufacturerName", "ManufactureName", "BrandName")
        unit = _first_text(it, "UnitQty", "UnitOfMeasure", "UnitName")
        qty_str = _first_text(it, "Quantity", "Content", "QtyInPackage")
        weighted_str = _first_text(it, "bIsWeighted", "BisWeighted", "IsWeighted")

        if not (barcode or name or price):
            continue
            
        # Normalize fields
        is_weighted = False
        if weighted_str and weighted_str.lower() in ("1", "true", "y"):
            is_weighted = True
            
        qty = None
        if qty_str:
            try:
                qty = float(qty_str)
            except ValueError:
                pass

        rows.append({
            "name": name,
            "barcode": barcode,
            "date": date,
            "price": price,
            "company": company,
            "store_id": store_id,  # Add store ID to the row
            "brand": brand,
            "unit": unit,
            "quantity": qty,  # Changed from "size" to "quantity" for normalization
            "is_weighted": is_weighted
        })
    return rows


async def parse_from_blob(data: bytes, filename_hint: str, retailer_id: str, run_id: str) -> int:
    """
    Unified parse function for all blob types (PriceFull, PromoFull, StoresFull, generic).
    Logs file.downloaded with sniffed kind, extracts XMLs, parses, and logs file.processed.
    Returns count of XML entries processed.
    """
    kind = sniff_kind(data)
    logger.info("file.downloaded retailer=%s file=%s kind=%s bytes=%d", retailer_id, filename_hint, kind, len(data))
    
    # Try to extract store ID from the filename
    store_ext_id = extract_store_id(filename_hint)
    
    xml_count = 0
    bucket = get_bucket()
    
    for inner_name, xml_bytes in iter_xml_entries(data, filename_hint=filename_hint):
        xml_count += 1
        try:
            # Optional: store normalized XML
            if os.getenv("STORE_NORMALIZED_XML", "0") in ("1", "true", "True"):
                xml_md5 = md5_hex(xml_bytes)
                xml_key = f"raw/{retailer_id}/{run_id}/xml/{xml_md5[:2]}/{xml_md5}_{os.path.basename(inner_name)}"
                if bucket:
                    await upload_to_gcs(bucket, xml_key, xml_bytes, content_type="application/xml", metadata={"md5_hex": xml_md5, "source_filename": inner_name})
            
            # Parse XML to JSONL with the extracted Store ID
            rows = parse_prices_xml(xml_bytes, company=retailer_id, store_id=store_ext_id)
            
            if rows:
                # Get retailer name from config
                retailer_name = retailer_id
                try:
                    from .config import load_retailers_config
                    cfg = load_retailers_config()
                    for r in cfg.get("retailers", []):
                        if r.get("id") == retailer_id:
                            retailer_name = r.get("name", retailer_id)
                            break
                except Exception:
                    pass
                
                # Save to DB
                db_saved = await save_parsed_prices(rows, retailer_id, retailer_name)
                logger.debug("db.save_parsed_prices retailer=%s saved=%d", retailer_id, db_saved)
            
            # GCS Upload
            if rows and bucket:
                jsonl_data = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
                blob_path = f"json/{retailer_id}/{os.path.splitext(inner_name)[0]}.jsonl"
                await upload_to_gcs(bucket, blob_path, jsonl_data.encode('utf-8'), "application/json")
                
        except Exception as e:
            logger.warning("xml.parse_failed retailer=%s file=%s inner=%s err=%s", retailer_id, filename_hint, inner_name, e)
    
    logger.info("file.processed retailer=%s file=%s xml_entries=%d", retailer_id, filename_hint, xml_count)
    return xml_count

