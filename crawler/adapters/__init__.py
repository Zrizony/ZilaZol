# crawler/adapters/__init__.py
from .publishedprices import crawl_publishedprices
from .bina import bina_adapter
from .generic import generic_adapter

__all__ = ["crawl_publishedprices", "bina_adapter", "generic_adapter"]

