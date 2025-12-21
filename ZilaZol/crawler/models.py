# crawler/models.py
from __future__ import annotations
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import List, Optional


@dataclass
class RetailerResult:
    retailer_id: str
    source_url: str
    errors: List[str]
    adapter: str
    links_found: int = 0
    files_downloaded: int = 0
    skipped_dupes: int = 0
    xml: int = 0
    gz: int = 0
    zips: int = 0
    subpath: Optional[str] = None
    reasons: List[str] = field(default_factory=list)  # e.g., ["no_dom_links", "used_click_fallback"]
    
    def as_dict(self):
        d = asdict(self)
        d["ts"] = datetime.now(timezone.utc).isoformat()
        return d

