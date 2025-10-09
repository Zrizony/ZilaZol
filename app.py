# app.py
from __future__ import annotations
import os
import asyncio
from flask import Flask, jsonify, request

from crawler.discovery import discover_retailers
from crawler.core import run_all
from crawler import logger
from crawler.gov_il import fetch_retailers

app = Flask(__name__)


@app.get("/health")
def health():
    return jsonify({"ok": True}), 200


@app.get("/version")
def version():
    return jsonify({"revision": os.getenv("K_REVISION", "dev")}), 200


@app.get("/retailers")
def retailers():
    debug = request.args.get("debug") == "1"
    try:
        retailers = fetch_retailers()  # uses the patched gov_il.py
        app.logger.info(f"discovered_retailers_count={len(retailers)}")
        return (
            jsonify({"count": len(retailers), "sample": retailers[:5], "debug": debug}),
            200,
        )
    except Exception as e:
        app.logger.exception("retailers_discovery_failed")
        return jsonify({"count": 0, "error": str(e)}), 200


@app.route("/run", methods=["POST", "GET"])
def run():
    try:
        debug = request.args.get("debug") == "1"
        retailers = discover_retailers(debug=debug)
        app.logger.info(f"discovered_retailers_count={len(retailers)}")

        # Return 200 regardless, to avoid 5xx in Cloud Scheduler.
        if not retailers:
            return jsonify({"status": "no_retailers_found", "count": 0}), 200

        results = asyncio.run(run_all(retailers))
        return (
            jsonify({"status": "done", "count": len(results), "results": results}),
            200,
        )

    except Exception as e:
        app.logger.exception("Run failed")
        # Still return 200 to keep Scheduler happy; details are in logs.
        return jsonify({"status": "error", "message": str(e)}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
