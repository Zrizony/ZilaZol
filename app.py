# app.py
from __future__ import annotations
import os
import asyncio
from flask import Flask, jsonify, request, current_app

from crawler.core import run_all
from crawler import logger
from crawler.config import load_retailers_config

app = Flask(__name__)


@app.get("/health")
def health():
    return jsonify({"ok": True}), 200


@app.get("/version")
def version():
    return jsonify({"revision": os.getenv("K_REVISION", "dev")}), 200




@app.get("/retailers")
def retailers_debug():
    cfg = load_retailers_config()
    retailers = cfg.get("retailers", [])
    return jsonify({
        "total_retailers": len(retailers),
        "sample": [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "enabled": r.get("enabled", True),
                "sources": [s.get("url") for s in r.get("sources", [])][:3]
            } for r in retailers[:5]
        ]
    }), 200

@app.post("/run")
def run():
    try:
        cfg = load_retailers_config()
        retailers = [r for r in cfg.get("retailers", []) if r.get("enabled", True)]
        results = asyncio.run(run_all(retailers))
        return jsonify({"status": "done", "count": len(results), "results": results}), 200
    except Exception as e:
        # Return 200 so Cloud Scheduler stops retrying forever
        current_app.logger.exception("run_failed")
        return jsonify({"status": "error", "error": str(e)}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
