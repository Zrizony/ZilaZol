# app.py
from __future__ import annotations
import os
import asyncio
import threading
from flask import Flask, jsonify, request, current_app
from google.cloud import storage

from crawler.core import run_all
from crawler import logger
from version import VERSION
from crawler.env import get_bucket
from crawler.config import load_retailers_config, get_retailers

app = Flask(__name__)

logger.info("startup version=%s", VERSION)
logger.info("bucket.config=%s", get_bucket() or "NONE")


def _run_crawler_background(retailers: list, group: str = "all"):
    """
    Run crawler in background thread. This function is called by /run endpoint
    to avoid blocking Cloud Scheduler requests.
    
    Args:
        retailers: List of retailer configurations to crawl
        group: Group name for logging (e.g. 'creds', 'public', 'all')
    """
    try:
        logger.info("background.crawler.start group=%s retailers=%d", group, len(retailers))
        
        # Run the crawler (asyncio.run creates a new event loop in this thread)
        results = asyncio.run(run_all(retailers))
        
        # Calculate summary statistics
        total_files = sum(r.get("files_downloaded", 0) for r in results)
        total_links = sum(r.get("links_found", 0) for r in results)
        total_errors = sum(len(r.get("errors", [])) for r in results)
        
        logger.info("background.crawler.done group=%s retailers=%d total_files=%d total_links=%d total_errors=%d",
                   group, len(retailers), total_files, total_links, total_errors)
        
    except Exception as e:
        logger.exception("background.crawler.failed group=%s error=%s", group, str(e))

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

@app.route("/run", methods=["POST", "GET"])
def run():
    """
    Trigger crawler run. Returns immediately (200 OK) and runs crawler in background.
    
    This endpoint is designed to work with Cloud Scheduler:
    - Returns quickly to avoid timeout/503 errors
    - Starts crawler in background thread
    - Cloud Scheduler gets immediate 200 response
    - Crawler continues running and uploads to GCS
    
    Query Parameters:
        group: 'creds', 'public', or None (all)
        slug: specific retailer ID to crawl
    
    Body (optional JSON):
        retailer: specific retailer ID (deprecated, use slug param)
        dry_run: if true, return config without running crawler
    """
    try:
        # Parse JSON payload and query parameters
        payload = request.get_json() or {}
        retailer_filter = payload.get("retailer")
        dry_run = payload.get("dry_run", False)
        
        # Get group and slug from query parameters
        # Examples: /run?group=creds, /run?slug=supercofix, /run?group=creds&slug=supercofix
        group = request.args.get("group")  # 'creds', 'public', or None
        slug = request.args.get("slug") or retailer_filter  # Allow slug in query param or body
        
        logger.info("marker.run.enter slug=%s group=%s dry_run=%s", slug or "ALL", group or "all", dry_run)
        
        # Load retailer configuration with group filter
        if slug:
            # If specific retailer/slug requested, load all and filter by ID/name
            cfg = load_retailers_config()
            all_retailers = cfg.get("retailers", [])
            retailers = [r for r in all_retailers if r.get("id") == slug or r.get("name") == slug]
            if not retailers:
                logger.error("run.error slug=%s error=not_found", slug)
                return jsonify({"status": "error", "error": f"Retailer '{slug}' not found"}), 404
        else:
            # Use get_retailers with group filter
            all_retailers_for_group = get_retailers(group=group)
            retailers = [r for r in all_retailers_for_group if r.get("enabled", True)]
            
            # Log disabled retailers in the current group
            disabled = [r for r in all_retailers_for_group if not r.get("enabled", True)]
            for d in disabled:
                reason = d.get("disabled_reason", "no_reason_specified")
                logger.info("retailer=%s disabled reason=%s", d.get("id", "unknown"), reason)

        logger.info("marker.discovery.summary group=%s retailers=%d", group or "all", len(retailers))
        
        # Dry run: return configuration without running crawler
        if dry_run:
            return jsonify({
                "status": "dry_run",
                "group": group or "all",
                "retailers_found": len(retailers),
                "retailer_names": [r.get("name") for r in retailers]
            }), 200
        
        # Start crawler in background thread and return immediately
        # This prevents Cloud Scheduler from timing out or getting 503 errors
        thread = threading.Thread(
            target=_run_crawler_background,
            args=(retailers, group or "all"),
            daemon=True,
            name=f"crawler-{group or 'all'}"
        )
        thread.start()
        
        logger.info("marker.run.accepted group=%s retailers=%d thread=%s", 
                   group or "all", len(retailers), thread.name)
        
        # Return immediately to Cloud Scheduler
        return jsonify({
            "status": "accepted",
            "message": "Crawler started in background",
            "group": group or "all",
            "retailers_count": len(retailers)
        }), 200
        
    except Exception as e:
        # CRITICAL: Always return 200 to prevent Cloud Scheduler retry loops
        # Log the exception but don't propagate 5xx errors
        logger.exception("run.endpoint.failed error=%s", str(e))
        return jsonify({
            "status": "error",
            "error": str(e),
            "message": "Failed to start crawler"
        }), 200


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
