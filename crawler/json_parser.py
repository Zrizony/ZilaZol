#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
json_parser.py - Unified JSON parser for price and promo data

This module consolidates all parsed data into a standardized JSON format
that can be easily consumed by the backend API.
"""

import json
import gzip
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class UnifiedDataParser:
    """Unified parser for all price and promo data"""
    
    def __init__(self, output_dir: str = "unified_json"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Standardized data structure
        self.products = {}  # barcode -> product_data
        self.promotions = {}  # barcode -> promotion_data
        self.stores = {}  # store_id -> store_data
        
        # Statistics
        self.stats = {
            "total_products": 0,
            "total_promotions": 0,
            "total_stores": 0,
            "files_processed": 0,
            "errors": 0
        }
    
    def parse_xml_file(self, file_path: Path, shop_name: str) -> bool:
        """Parse a single XML file and extract data"""
        try:
            # Read file content
            with open(file_path, 'rb') as f:
                content = f.read()
            
            # Try to determine file type and extract XML
            xml_content = self._extract_xml_content(content, file_path)
            
            if not xml_content:
                return False
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            # Determine file type and parse accordingly
            if self._is_price_file(root):
                self._parse_price_data(root, shop_name, file_path.name)
            elif self._is_promo_file(root):
                self._parse_promo_data(root, shop_name, file_path.name)
            elif self._is_store_file(root):
                self._parse_store_data(root, shop_name, file_path.name)
            else:
                log.warning(f"Unknown file type: {file_path.name}")
                return False
            
            self.stats["files_processed"] += 1
            return True
            
        except Exception as e:
            log.error(f"Error parsing {file_path}: {e}")
            self.stats["errors"] += 1
            return False
    
    def _extract_xml_content(self, content: bytes, file_path: Path) -> Optional[str]:
        """Extract XML content from various file formats"""
        try:
            # Check if it's a ZIP file (most common case)
            if content.startswith(b'PK'):
                return self._extract_from_zip(content)
            
            # Check if it's a gzipped file
            elif content.startswith(b'\x1f\x8b'):
                return self._extract_from_gzip(content)
            
            # Try to decode as plain text (XML)
            else:
                try:
                    return content.decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        return content.decode('latin-1')
                    except UnicodeDecodeError:
                        log.warning(f"Could not decode content from {file_path.name}")
                        return None
                        
        except Exception as e:
            log.error(f"Error extracting content from {file_path.name}: {e}")
            return None
    
    def _extract_from_zip(self, content: bytes) -> Optional[str]:
        """Extract XML content from ZIP file"""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zip_ref:
                # Look for XML files in the ZIP
                xml_files = [f for f in zip_ref.namelist() if f.endswith('.xml')]
                
                if not xml_files:
                    log.warning("No XML files found in ZIP")
                    return None
                
                # Use the first XML file found
                xml_file = xml_files[0]
                with zip_ref.open(xml_file) as f:
                    return f.read().decode('utf-8')
                    
        except Exception as e:
            log.error(f"Error extracting from ZIP: {e}")
            return None
    
    def _extract_from_gzip(self, content: bytes) -> Optional[str]:
        """Extract content from gzipped file"""
        try:
            with gzip.open(io.BytesIO(content), 'rt', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            log.error(f"Error extracting from GZIP: {e}")
            return None
    
    def _is_price_file(self, root: ET.Element) -> bool:
        """Check if XML contains price data"""
        return (
            root.find(".//Items") is not None or
            root.find(".//Products") is not None or
            root.find(".//Item") is not None or
            root.find(".//Product") is not None
        )
    
    def _is_promo_file(self, root: ET.Element) -> bool:
        """Check if XML contains promotion data"""
        return (
            root.find(".//Promotion") is not None or
            root.find(".//Promo") is not None
        )
    
    def _is_store_file(self, root: ET.Element) -> bool:
        """Check if XML contains store data"""
        return (
            root.find(".//Stores") is not None or
            root.find(".//Store") is not None
        )
    
    def _parse_price_data(self, root: ET.Element, shop_name: str, filename: str):
        """Parse price data from XML"""
        current_time = datetime.now().isoformat()
        
        # Handle different XML structures
        items = []
        
        # BinaProjects format
        if root.find(".//Items") is not None:
            items = root.findall(".//Item")
            for item in items:
                product_data = {
                    "barcode": item.findtext("ItemCode", "").strip(),
                    "name": item.findtext("ItemName", "").strip(),
                    "price": self._parse_float(item.findtext("ItemPrice", "0")),
                    "category": item.findtext("ItemCategory", "").strip(),
                    "unit": item.findtext("ItemUnit", "").strip(),
                    "update_date": item.findtext("PriceUpdateDate", "").split("T")[0],
                    "shop": shop_name,
                    "source_file": filename,
                    "parsed_at": current_time,
                    "data_type": "price"
                }
                self._add_product(product_data)
        
        # Cerberus format
        elif root.find(".//Products") is not None:
            items = root.findall(".//Product")
            for item in items:
                product_data = {
                    "barcode": item.findtext("Code", "").strip(),
                    "name": item.findtext("Name", "").strip(),
                    "price": self._parse_float(item.findtext("Price", "0")),
                    "category": item.findtext("Category", "").strip(),
                    "unit": item.findtext("Unit", "").strip(),
                    "update_date": item.findtext("UpdateDate", "").split("T")[0],
                    "shop": shop_name,
                    "source_file": filename,
                    "parsed_at": current_time,
                    "data_type": "price"
                }
                self._add_product(product_data)
    
    def _parse_promo_data(self, root: ET.Element, shop_name: str, filename: str):
        """Parse promotion data from XML"""
        current_time = datetime.now().isoformat()
        
        # Handle different XML structures
        promos = []
        
        # BinaProjects promo format
        if root.find(".//Promotion") is not None:
            promos = root.findall(".//Promotion")
            for promo in promos:
                promo_data = {
                    "barcode": promo.findtext("ItemCode", "").strip(),
                    "name": promo.findtext("ItemName", "").strip(),
                    "promo_price": self._parse_float(promo.findtext("PromoPrice", "0")),
                    "regular_price": self._parse_float(promo.findtext("ItemPrice", "0")),
                    "description": promo.findtext("PromoDesc", "").strip(),
                    "start_date": promo.findtext("PromoStartDate", "").split("T")[0],
                    "end_date": promo.findtext("PromoEndDate", "").split("T")[0],
                    "shop": shop_name,
                    "source_file": filename,
                    "parsed_at": current_time,
                    "data_type": "promotion"
                }
                self._add_promotion(promo_data)
        
        # Cerberus promo format
        elif root.find(".//Promo") is not None:
            promos = root.findall(".//Promo")
            for promo in promos:
                promo_data = {
                    "barcode": promo.findtext("Code", "").strip(),
                    "name": promo.findtext("Name", "").strip(),
                    "promo_price": self._parse_float(promo.findtext("PromoPrice", "0")),
                    "regular_price": self._parse_float(promo.findtext("Price", "0")),
                    "description": promo.findtext("PromoType", "").strip(),
                    "start_date": promo.findtext("StartDate", "").split("T")[0],
                    "end_date": promo.findtext("EndDate", "").split("T")[0],
                    "shop": shop_name,
                    "source_file": filename,
                    "parsed_at": current_time,
                    "data_type": "promotion"
                }
                self._add_promotion(promo_data)
    
    def _parse_store_data(self, root: ET.Element, shop_name: str, filename: str):
        """Parse store data from XML"""
        current_time = datetime.now().isoformat()
        
        stores = root.findall(".//Store")
        for store in stores:
            store_data = {
                "store_id": store.findtext("StoreID", "").strip(),
                "name": store.findtext("StoreName", "").strip(),
                "address": store.findtext("Address", "").strip(),
                "city": store.findtext("City", "").strip(),
                "phone": store.findtext("Phone", "").strip(),
                "chain": shop_name,
                "source_file": filename,
                "parsed_at": current_time,
                "data_type": "store"
            }
            self._add_store(store_data)
    
    def _add_product(self, product_data: Dict):
        """Add product to unified data structure"""
        barcode = product_data["barcode"]
        
        if not barcode or len(barcode) < 5:
            return  # Skip invalid barcodes
        
        if barcode not in self.products:
            self.products[barcode] = {
                "barcode": barcode,
                "name": product_data["name"],
                "category": product_data.get("category", ""),
                "unit": product_data.get("unit", ""),
                "prices": [],
                "promotions": [],
                "shops": set(),
                "last_updated": product_data["parsed_at"]
            }
        
        # Add price entry
        price_entry = {
            "price": product_data["price"],
            "shop": product_data["shop"],
            "date": product_data["update_date"],
            "source_file": product_data["source_file"],
            "parsed_at": product_data["parsed_at"]
        }
        
        self.products[barcode]["prices"].append(price_entry)
        self.products[barcode]["shops"].add(product_data["shop"])
        self.products[barcode]["last_updated"] = product_data["parsed_at"]
        
        # Update name if we have a better one
        if product_data["name"] and len(product_data["name"]) > len(self.products[barcode]["name"]):
            self.products[barcode]["name"] = product_data["name"]
        
        self.stats["total_products"] += 1
    
    def _add_promotion(self, promo_data: Dict):
        """Add promotion to unified data structure"""
        barcode = promo_data["barcode"]
        
        if not barcode or len(barcode) < 5:
            return  # Skip invalid barcodes
        
        # Add to products if not exists
        if barcode not in self.products:
            self.products[barcode] = {
                "barcode": barcode,
                "name": promo_data["name"],
                "category": "",
                "unit": "",
                "prices": [],
                "promotions": [],
                "shops": set(),
                "last_updated": promo_data["parsed_at"]
            }
        
        # Add promotion entry
        promo_entry = {
            "promo_price": promo_data["promo_price"],
            "regular_price": promo_data["regular_price"],
            "description": promo_data["description"],
            "start_date": promo_data["start_date"],
            "end_date": promo_data["end_date"],
            "shop": promo_data["shop"],
            "source_file": promo_data["source_file"],
            "parsed_at": promo_data["parsed_at"]
        }
        
        self.products[barcode]["promotions"].append(promo_entry)
        self.products[barcode]["shops"].add(promo_data["shop"])
        self.products[barcode]["last_updated"] = promo_data["parsed_at"]
        
        # Update name if we have a better one
        if promo_data["name"] and len(promo_data["name"]) > len(self.products[barcode]["name"]):
            self.products[barcode]["name"] = promo_data["name"]
        
        self.stats["total_promotions"] += 1
    
    def _add_store(self, store_data: Dict):
        """Add store to unified data structure"""
        store_id = store_data["store_id"]
        
        if not store_id:
            return  # Skip invalid store IDs
        
        self.stores[store_id] = store_data
        self.stats["total_stores"] += 1
    
    def _parse_float(self, value: str) -> float:
        """Safely parse float value"""
        try:
            return float(value.replace(",", "."))
        except (ValueError, AttributeError):
            return 0.0
    
    def process_directory(self, directory_path: str):
        """Process all files in a directory"""
        directory = Path(directory_path)
        
        if not directory.exists():
            log.error(f"Directory not found: {directory_path}")
            return
        
        log.info(f"Processing directory: {directory_path}")
        
        # Process all .gz and .xml files
        for file_path in directory.rglob("*.gz"):
            shop_name = file_path.parent.name
            self.parse_xml_file(file_path, shop_name)
        
        for file_path in directory.rglob("*.xml"):
            shop_name = file_path.parent.name
            self.parse_xml_file(file_path, shop_name)
    
    def save_unified_json(self):
        """Save unified data to JSON files"""
        current_time = datetime.now().isoformat()
        
        # Convert sets to lists for JSON serialization
        for barcode, product in self.products.items():
            product["shops"] = list(product["shops"])
        
        # Save products
        products_file = self.output_dir / "products.json"
        with open(products_file, 'w', encoding='utf-8') as f:
            json.dump({
                "metadata": {
                    "generated_at": current_time,
                    "total_products": len(self.products),
                    "total_promotions": self.stats["total_promotions"],
                    "files_processed": self.stats["files_processed"],
                    "errors": self.stats["errors"]
                },
                "products": self.products
            }, f, ensure_ascii=False, indent=2)
        
        # Save stores
        stores_file = self.output_dir / "stores.json"
        with open(stores_file, 'w', encoding='utf-8') as f:
            json.dump({
                "metadata": {
                    "generated_at": current_time,
                    "total_stores": len(self.stores)
                },
                "stores": self.stores
            }, f, ensure_ascii=False, indent=2)
        
        # Save statistics
        stats_file = self.output_dir / "statistics.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump({
                "generated_at": current_time,
                "statistics": self.stats,
                "summary": {
                    "unique_products": len(self.products),
                    "unique_stores": len(self.stores),
                    "total_price_entries": sum(len(p["prices"]) for p in self.products.values()),
                    "total_promotion_entries": sum(len(p["promotions"]) for p in self.products.values())
                }
            }, f, ensure_ascii=False, indent=2)
        
        log.info(f"Saved unified JSON files to {self.output_dir}")
        log.info(f"Products: {len(self.products)}, Stores: {len(self.stores)}")
    
    def create_search_index(self):
        """Create search-friendly index files"""
        current_time = datetime.now().isoformat()
        
        # Create barcode index
        barcode_index = {}
        for barcode, product in self.products.items():
            barcode_index[barcode] = {
                "name": product["name"],
                "category": product["category"],
                "shops": product["shops"],
                "has_promotions": len(product["promotions"]) > 0,
                "last_updated": product["last_updated"]
            }
        
        barcode_file = self.output_dir / "barcode_index.json"
        with open(barcode_file, 'w', encoding='utf-8') as f:
            json.dump({
                "metadata": {
                    "generated_at": current_time,
                    "total_barcodes": len(barcode_index)
                },
                "barcodes": barcode_index
            }, f, ensure_ascii=False, indent=2)
        
        # Create shop index
        shop_index = {}
        for barcode, product in self.products.items():
            for shop in product["shops"]:
                if shop not in shop_index:
                    shop_index[shop] = []
                shop_index[shop].append(barcode)
        
        shop_file = self.output_dir / "shop_index.json"
        with open(shop_file, 'w', encoding='utf-8') as f:
            json.dump({
                "metadata": {
                    "generated_at": current_time,
                    "total_shops": len(shop_index)
                },
                "shops": shop_index
            }, f, ensure_ascii=False, indent=2)
        
        log.info(f"Created search indexes: {len(barcode_index)} barcodes, {len(shop_index)} shops")


def main():
    """Main function to process all downloaded data"""
    parser = UnifiedDataParser()
    
    # Process all downloaded directories
    download_dirs = [
        "enhanced_downloads",
        "downloads",
        "GPTcrawl/downloads"
    ]
    
    for directory in download_dirs:
        if Path(directory).exists():
            parser.process_directory(directory)
    
    # Save unified JSON files
    parser.save_unified_json()
    
    # Create search indexes
    parser.create_search_index()
    
    # Print summary
    print(f"\n=== UNIFIED JSON PARSER SUMMARY ===")
    print(f"Files processed: {parser.stats['files_processed']}")
    print(f"Total products: {parser.stats['total_products']}")
    print(f"Total promotions: {parser.stats['total_promotions']}")
    print(f"Total stores: {parser.stats['total_stores']}")
    print(f"Errors: {parser.stats['errors']}")
    print(f"Output directory: {parser.output_dir}")


if __name__ == "__main__":
    main() 