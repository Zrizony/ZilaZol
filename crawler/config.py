# crawler/config.py
import json
from pathlib import Path
from typing import Any, Dict

def load_retailers_config(path: str = "data/retailers.json") -> Dict[str, Any]:
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    # Validate minimal structure but do NOT enforce a fixed schema
    assert isinstance(data, dict), "retailers.json must be an object"
    assert "retailers" in data and isinstance(data["retailers"], list), \
        "retailers.json must contain a 'retailers' list"
    return data