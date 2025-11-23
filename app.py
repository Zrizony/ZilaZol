# app.py
from __future__ import annotations
import os
import asyncio
import threading
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


def _run_crawler_background(retailers, group_for_log):
    """
    Helper function to run the crawler in a background thread.
    
    This function is called by threading.Thread to execute the crawler
    asynchronously without blocking the HTTP request.
    """
    try:
        logger.info(
            "background.crawler.thread_start thread_id=%s group=%s retailers=%d",
            threading.current_thread().ident,
            group_for_log,
            len(retailers),
        )
        
        from time import perf_counter
        start = perf_counter()
        
        # Run the crawler to completion
        results = asyncio.run(run_all(retailers))
        
        duration = perf_counter() - start
        
        success_count = sum(1 for r in results if not r.get("errors"))
        error_count = len(results) - success_count
        
        logger.info(
            "background.crawler.thread_end thread_id=%s group=%s retailers=%d duration_sec=%.2f success=%d errors=%d",
            threading.current_thread().ident,
            group_for_log,
            len(retailers),
            duration,
            success_count,
            error_count,
        )
    except Exception as e:
        logger.exception(
            "background.crawler.thread_failed thread_id=%s group=%s error=%s",
            threading.current_thread().ident,
            group_for_log,
            str(e),
        )


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


@app.route("/trigger", methods=["GET", "POST"])
def trigger():
    """
    Lightweight endpoint intended for Cloud Scheduler.

    - Accepts ?group=creds or ?group=public (or no group → 'all').
    - Resolves the retailer list for that group.
    - Starts the crawler in a background thread via _run_crawler_background.
    - Returns 200 OK immediately so Cloud Scheduler never times out.
    """
    # Read group from query parameters; default to "all" for logging
    group = request.args.get("group")
    group_for_log = group or "all"

    try:
        logger.info("trigger.enter group=%s", group_for_log)

        # Use existing config helper; filter to enabled retailers only
        all_retailers_for_group = get_retailers(group=group)
        retailers = [r for r in all_retailers_for_group if r.get("enabled", True)]

        # Log disabled retailers so we can see why counts differ
        disabled = [r for r in all_retailers_for_group if not r.get("enabled", True)]
        for d in disabled:
            reason = d.get("disabled_reason", "no_reason_specified")
            logger.info(
                "trigger.retailer_disabled id=%s group=%s reason=%s",
                d.get("id", "unknown"),
                group_for_log,
                reason,
            )

        logger.info(
            "trigger.discovery.summary group=%s retailers=%d",
            group_for_log,
            len(retailers),
        )

        if not retailers:
            # Still return 200 so Scheduler sees success, but log clearly
            logger.warning("trigger.no_retailers group=%s", group_for_log)
            return jsonify(
                {
                    "status": "accepted",
                    "message": "No enabled retailers for group",
                    "group": group_for_log,
                    "retailers_count": 0,
                }
            ), 200

        # Start crawler in a **non-daemon** thread so the container stays alive
        thread = threading.Thread(
            target=_run_crawler_background,
            args=(retailers, group_for_log),
            daemon=False,
            name=f"trigger-crawler-{group_for_log}",
        )
        thread.start()

        logger.info(
            "trigger.accepted group=%s retailers=%d thread=%s daemon=%s",
            group_for_log,
            len(retailers),
            thread.name,
            thread.daemon,
        )

        return jsonify(
            {
                "status": "accepted",
                "message": "Crawler started in background",
                "group": group_for_log,
                "retailers_count": len(retailers),
            }
        ), 200

    except Exception as e:
        # IMPORTANT: We still return 200 to avoid Cloud Scheduler retry storms.
        logger.exception("trigger.failed group=%s error=%s", group_for_log, e)
        return jsonify(
            {
                "status": "error",
                "message": "Failed to start crawler; see logs for details",
                "group": group_for_log,
                "error": str(e),
            }
        ), 200


@app.route("/run", methods=["POST", "GET"])
def run():
    """
    Trigger crawler run.

    This endpoint performs full parameter parsing (group, slug, dry_run, etc.)
    and is primarily intended for manual / debugging usage.

    For Cloud Scheduler, prefer calling /trigger, which is a lightweight
    wrapper that only starts the crawler in a background thread and returns 200
    immediately.
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
