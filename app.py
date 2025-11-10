# app.py
from __future__ import annotations
import os
import asyncio
from flask import Flask, jsonify, request, current_app
from google.cloud import storage

from crawler.core import run_all
from crawler import logger
from version import VERSION
from crawler.env import get_bucket
from crawler.config import load_retailers_config

app = Flask(__name__)

logger.info("startup version=%s", VERSION)
logger.info("bucket.config=%s", get_bucket() or "NONE")

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
        logger.info("marker.run.enter retailer=%s", retailer_filter or "ALL")
        
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
            # Log disabled retailers
            disabled = [r for r in all_retailers if not r.get("enabled", True)]
            for d in disabled:
                reason = d.get("disabled_reason", "no_reason_specified")
                logger.info("retailer=%s disabled reason=%s", d.get("id", "unknown"), reason)

        logger.info("marker.discovery.summary retailers=%d", len(retailers))
        
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
        
        resp = {
            "status": "done",
            "retailers_processed": len(retailers),
            "results_count": len(results),
            "total_links_found": total_links,
            "total_files_downloaded": total_files,
            "total_errors": total_errors,
            "results": results
        }
        logger.info("marker.after_extract retailers=%d total_files=%d", len(retailers), total_files)
        return jsonify(resp), 200
        
    except Exception as e:
        # Return 200 so Cloud Scheduler stops retrying forever
        current_app.logger.exception("run_failed")
        return jsonify({"status": "error", "error": str(e)}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

# ---- Diagnostics Endpoints ----

@app.route("/__version", methods=["GET"])
def __version():
    return jsonify({"version": VERSION})


@app.route("/__env", methods=["GET"])
def __env():
    import os
    visible = [
        "GCS_BUCKET",
        "PRICES_BUCKET",
        "BUCKET_NAME",
        "LOG_LEVEL",
        "RELEASE",
        "COMMIT_SHA",
    ]
    return jsonify({k: os.getenv(k) for k in visible})


@app.route("/__smoke", methods=["POST"])
def __smoke():
    import time, hashlib
    bkt_name = get_bucket()
    if not bkt_name:
        return jsonify(ok=False, error="No bucket configured (GCS_BUCKET/PRICES_BUCKET/BUCKET_NAME)."), 500
    payload = f"ok:{time.time()}".encode()
    md5 = hashlib.md5(payload).hexdigest()
    key = f"smoke/{VERSION}/{md5}.txt"
    try:
        client = storage.Client()
        bucket = client.bucket(bkt_name)
        blob = bucket.blob(key)
        blob.metadata = {"md5_hex": md5}
        blob.upload_from_string(payload, content_type="text/plain")
        logger.info("smoke.uploaded bucket=%s key=%s", bkt_name, key)
        return jsonify(ok=True, bucket=bkt_name, key=key)
    except Exception as e:
        current_app.logger.exception("smoke_failed")
        return jsonify(ok=False, error=str(e)), 500
