# crawler/parsers.py
from __future__ import annotations
import re
from typing import List, Optional, Tuple, Dict
from lxml import etree
from . import logger
from .archive_utils import iter_xml_entries, sniff_kind
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
                # Try multiple possible address field names (English and Hebrew)
                address = (
                    _first_text(store, "Address", "Street", "StoreAddress", 
                               "AddressLine1", "FullAddress", "Location", 
                               "StreetAddress", "Addr", "StoreLocation",
                               "כתובת", "רחוב", "מיקום", "כתובת_סניף")  # Hebrew: address, street, location
                )
                city = _first_text(store, "City", "CityName", "StoreCity",
                                   "עיר", "יישוב")  # Hebrew: city, settlement
                name = _first_text(store, "StoreName", "StoreNm", "Name", "StoreName",
                                  "שם_סניף", "סניף", "שם")  # Hebrew: branch name, branch, name
                
                # Log if we found address data
                if address or city:
                    logger.info(f"parse_stores_xml found store ext_id={ext_id} name={name} city={city} address={address}")
                
                rows.append({
                    "external_id": ext_id,
                    "name": name,
                    "city": city,
                    "address": address
                })
    except Exception as e:
        logger.warning(f"Failed to parse stores XML: {e}")
    return rows


def parse_prices_xml(xml_bytes: bytes, company: str, store_id: str = None) -> Tuple[List[Dict], Dict]:
    """
    Parse price XML and return (price_rows, store_metadata).
    
    Returns:
        tuple: (list of price items, dict with store metadata: {store_id, name, city, address})
    """
    rows: List[dict] = []
    store_metadata = {}
    
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return rows, store_metadata

    # Extract store metadata from root level (if present in price files)
    # Some retailers embed store info in price XML files
    # First try to extract from root level tags
    store_metadata["store_id"] = store_id or _first_text(root, "StoreId", "StoreID", "storeid")
    store_metadata["name"] = _first_text(root, "StoreName", "StoreNm", "Name", "StoreName",
                                        "שם_סניף", "סניף", "שם")  # Hebrew: branch name, branch, name
    store_metadata["city"] = _first_text(root, "City", "CityName", "StoreCity",
                                        "עיר", "יישוב")  # Hebrew: city, settlement
    # Try multiple possible address field names (English and Hebrew)
    store_metadata["address"] = _first_text(root, "Address", "Street", "StoreAddress",
                                            "AddressLine1", "FullAddress", "Location",
                                            "StreetAddress", "Addr", "StoreLocation",
                                            "כתובת", "רחוב", "מיקום", "כתובת_סניף")  # Hebrew: address, street, location

    # Also check for store info in a Store element at root level (takes precedence)
    store_elem = root.find(".//Store")
    if store_elem is not None:
        # Override with Store element values if they exist
        extracted_store_id = _first_text(store_elem, "StoreId", "StoreID", "storeid")
        if extracted_store_id:
            store_metadata["store_id"] = extracted_store_id
        extracted_name = _first_text(store_elem, "StoreName", "StoreNm", "Name",
                                     "שם_סניף", "סניף", "שם")  # Hebrew: branch name, branch, name
        if extracted_name:
            store_metadata["name"] = extracted_name
        extracted_city = _first_text(store_elem, "City", "CityName", "StoreCity",
                                     "עיר", "יישוב")  # Hebrew: city, settlement
        if extracted_city:
            store_metadata["city"] = extracted_city
        # Try multiple address field names (English and Hebrew)
        extracted_address = _first_text(store_elem, "Address", "Street", "StoreAddress",
                                       "AddressLine1", "FullAddress", "Location",
                                       "StreetAddress", "Addr", "StoreLocation",
                                       "כתובת", "רחוב", "מיקום", "כתובת_סניף")  # Hebrew: address, street, location
        if extracted_address:
            store_metadata["address"] = extracted_address
    
    # Log if we found store metadata
    if store_metadata.get("address") or store_metadata.get("city"):
        logger.info(f"parse_prices_xml extracted store metadata: store_id={store_metadata.get('store_id')} "
                   f"city={store_metadata.get('city')} address={store_metadata.get('address')}")

    # Use store_id from metadata if we found it and it wasn't passed in
    effective_store_id = store_metadata.get("store_id") or store_id

    # 1. Handle PROMOS (Promo/PromoFull)
    # Price is in "DiscountedPrice", IS on sale
    for promo in root.findall(".//Promotion"):
        price = _first_text(promo, "DiscountedPrice", "DiscountRate")
        date = _first_text(promo, "PromotionUpdateDate", "UpdateDate", "PromotionStartDate")
        
        if not price: continue
        
        for item in promo.findall(".//Item"):
            barcode = _first_text(item, "ItemCode", "Barcode")
            if barcode:
                # Extract image URL if available
                image_url = _first_text(item, "ItemImage", "Image", "ImageUrl", "ImageURL",
                                       "Picture", "PictureUrl", "Photo", "PhotoUrl",
                                       "תמונה", "קישור_תמונה")  # Hebrew: image, image link
                rows.append({
                    "barcode": barcode,
                    "price": price,
                    "date": date,
                    "company": company,
                    "store_id": effective_store_id,
                    "is_on_sale": True,
                    "name": None, # Promos often lack names
                    "image_url": image_url
                })

    # 2. Handle PRICES (Price/PriceFull)
    # Some retailers have both regular price and promotion price in the same Item element
    # We need to compare them to determine if item is actually on sale
    items = root.findall(".//Item")
    if not items: items = list(root)
    
    for it in items:
        barcode = _first_text(it, "ItemCode", "Barcode")
        regular_price_str = _first_text(it, "ItemPrice", "Price", "RegularPrice", "ListPrice")
        promotion_price_str = _first_text(it, "PromotionPrice", "DiscountedPrice", "SalePrice", "DiscountPrice")
        
        # Determine which price to use and if on sale
        price_str = None
        is_on_sale = False
        
        if promotion_price_str and regular_price_str:
            # Both prices exist - compare them
            try:
                regular_price = float(regular_price_str)
                promotion_price = float(promotion_price_str)
                # Only mark as sale if promotion price is actually lower
                if promotion_price < regular_price:
                    price_str = promotion_price_str
                    is_on_sale = True
                else:
                    # Promotion price >= regular price, use regular price (not a real sale)
                    price_str = regular_price_str
                    is_on_sale = False
            except (ValueError, TypeError):
                # If parsing fails, fall back to regular price
                price_str = regular_price_str
                is_on_sale = False
        elif promotion_price_str:
            # Only promotion price exists - use it but mark as sale
            price_str = promotion_price_str
            is_on_sale = True
        elif regular_price_str:
            # Only regular price exists
            price_str = regular_price_str
            is_on_sale = False
        else:
            # No price found, skip this item
            continue
        
        if not (barcode and price_str): continue

        # Extract Raw Metadata
        qty_str = _first_text(it, "Quantity", "Content", "QtyInPackage")
        qty = None
        if qty_str:
            try: qty = float(qty_str)
            except: pass # Keep None if not a valid number
        
        weighted_str = _first_text(it, "bIsWeighted", "BisWeighted")
        is_weighted = (weighted_str and weighted_str.lower() in ("1", "true", "y"))

        # Extract image URL if available
        image_url = _first_text(it, "ItemImage", "Image", "ImageUrl", "ImageURL", 
                               "Picture", "PictureUrl", "Photo", "PhotoUrl",
                               "תמונה", "קישור_תמונה")  # Hebrew: image, image link
        
        rows.append({
            "name": _first_text(it, "ItemName", "ItemNm", "ItemDescription", "Description"),
            "barcode": barcode,
            "date": _first_text(it, "PriceUpdateDate", "UpdateDate"),
            "price": price_str,
            "company": company,
            "store_id": effective_store_id,
            "is_on_sale": is_on_sale,
            "brand": _first_text(it, "ManufacturerName", "BrandName"),
            "unit": _first_text(it, "UnitQty", "UnitOfMeasure"),
            "quantity": qty,
            "is_weighted": is_weighted,
            "image_url": image_url
        })
    
    return rows, store_metadata


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
                rows, store_metadata = parse_prices_xml(xml_bytes, company=retailer_id, store_id=store_ext_id)
                if rows: 
                    # Pass store metadata to save_parsed_prices so it can update store info
                    await save_parsed_prices(rows, retailer_id, retailer_id, store_metadata=store_metadata)
        except Exception as e:
            logger.warning(f"Parse error: {e}")
    return count
