# app.py
from __future__ import annotations
import os
import asyncio
from flask import Flask, jsonify, request, current_app
from google.cloud import storage

from crawler.core import run_all
from crawler import logger
from crawler.constants import BUCKET
from version import VERSION
from crawler.env import get_bucket
from crawler.config import load_retailers_config, get_retailers

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

@app.route("/run", methods=["POST", "GET"])
def run():
    """
    Trigger crawler run. Runs the crawler synchronously inside this request.

    This endpoint is designed to be called by Cloud Scheduler:
    - Scheduler calls /run?group=...
    - Endpoint runs the full crawl with run_all(...)
    - When the crawl finishes, /run returns a JSON summary and 200 OK

    This ensures Cloud Run keeps the container alive for the whole crawl and
    uploads to GCS are not interrupted by background thread shutdown.
    """
    group = None  # Initialize for error handler
    try:
        # Parse JSON payload and query parameters
        payload = request.get_json() or {}
        retailer_filter = payload.get("retailer")
        dry_run = payload.get("dry_run", False)
        
        # Get group and slug from query parameters
        # Examples: /run?group=creds, /run?slug=supercofix, /run?group=creds&slug=supercofix
        group = request.args.get("group")  # 'creds', 'public', or None
        slug = request.args.get("slug") or retailer_filter  # Allow slug in query param or body
        
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
        
        # Run the crawler synchronously inside this request.
        # This guarantees Cloud Run keeps the instance alive until the crawl is done.
        from time import perf_counter
        start = perf_counter()

        logger.info(
            "marker.run.enter slug=ALL group=%s dry_run=%s",
            group or "all",
            bool(dry_run),
        )

        # run_all is async; run it to completion in this process
        results = asyncio.run(run_all(retailers))

        duration = perf_counter() - start
        logger.info(
            "marker.run.completed group=%s retailers=%d duration_sec=%.2f",
            group or "all",
            len(retailers),
            duration,
        )

        # Basic summary for the caller (Cloud Scheduler)
        # Do not include the full result list to keep the response small.
        success_count = sum(1 for r in results if not r.get("errors"))
        error_count = len(results) - success_count

        return jsonify({
            "status": "success",
            "group": group or "all",
            "retailers_count": len(retailers),
            "success_count": success_count,
            "error_count": error_count,
            "duration_sec": round(duration, 2),
        }), 200
        
    except Exception as e:
        logger.exception("run.endpoint.failed error=%s", str(e))
        return jsonify({
            "status": "error",
            "group": group or "all",
            "error": str(e),
        }), 500


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
