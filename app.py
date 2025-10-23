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

@app.route("/run", methods=["POST"])
def run():
    try:
        # Parse JSON payload
        payload = request.get_json() or {}
        retailer_filter = payload.get("retailer")
        dry_run = payload.get("dry_run", False)
        
        # Load retailer configuration
        cfg = load_retailers_config()
        all_retailers = cfg.get("retailers", [])
        
        # Filter retailers if specific one requested
        if retailer_filter:
            retailers = [r for r in all_retailers if r.get("id") == retailer_filter or r.get("name") == retailer_filter]
            if not retailers:
                return jsonify({"status": "error", "error": f"Retailer '{retailer_filter}' not found"}), 404
        else:
            retailers = [r for r in all_retailers if r.get("enabled", True)]
        
        if dry_run:
            return jsonify({
                "status": "dry_run",
                "retailers_found": len(retailers),
                "retailer_names": [r.get("name") for r in retailers]
            }), 200
        
        # Run the crawler
        results = asyncio.run(run_all(retailers))
        
        # Calculate summary statistics
        total_files = sum(r.get("files_downloaded", 0) for r in results)
        total_links = sum(r.get("links_found", 0) for r in results)
        total_errors = sum(len(r.get("errors", [])) for r in results)
        
        return jsonify({
            "status": "done",
            "retailers_processed": len(retailers),
            "results_count": len(results),
            "total_links_found": total_links,
            "total_files_downloaded": total_files,
            "total_errors": total_errors,
            "results": results
        }), 200
        
    except Exception as e:
        # Return 200 so Cloud Scheduler stops retrying forever
        current_app.logger.exception("run_failed")
        return jsonify({"status": "error", "error": str(e)}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
