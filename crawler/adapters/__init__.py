"""
Adapters for different retailer website formats.

Each adapter contains parsing logic or transformations specific
to a retailer's XML feed or published prices structure.
"""

from .publishedprices import PublishedPricesAdapter

__all__ = ["PublishedPricesAdapter"]
