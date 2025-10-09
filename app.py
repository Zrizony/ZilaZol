# app.py
from __future__ import annotations
import os
import asyncio
from flask import Flask, jsonify, request
from crawler.gov_il import fetch_retailers
from crawler.gov_il import discovery_diagnostics
from crawler.core import run_all

app = Flask(__name__)


@app.get("/health")
def health():
    return jsonify({"ok": True}), 200


@app.get("/version")
def version():
    # tiny helper to prove which revision is serving
    return (
        jsonify(
            {
                "version": os.getenv("K_REVISION", "unknown"),
                "commit": os.getenv("GIT_COMMIT_SHA", "unknown"),
            }
        ),
        200,
    )


@app.get("/retailers")
def retailers():
    try:
        if request.args.get("debug") == "1":
            info = discovery_diagnostics()
            return jsonify(info), 200
        items = fetch_retailers()
        return jsonify({"count": len(items), "retailers": items}), 200
    except Exception as e:
        app.logger.exception("Retailers discovery failed")
        return jsonify({"status": "error", "error": str(e)}), 200


@app.errorhandler(Exception)
def handle_any_exception(e):
    # GLOBAL safety net so Cloud Run never returns 500
    app.logger.exception("Unhandled exception")
    return jsonify({"status": "error", "error": str(e)}), 200


@app.route("/run", methods=["POST", "GET"])
def run_job():
    try:
        retailers = fetch_retailers()
        app.logger.info("discovered_retailers_count=%s", len(retailers))

        # Return 200 even if none found (prevents Scheduler retries)
        if not retailers:
            return jsonify({"status": "no_retailers_found", "count": 0}), 200

        # Run orchestrator
        results = asyncio.run(run_all(retailers))
        return (
            jsonify({"status": "done", "count": len(results), "results": results}),
            200,
        )

    except Exception as e:
        app.logger.exception("Run failed")
        # IMPORTANT: 200 so Cloud Scheduler doesn't retry forever
        return jsonify({"status": "error", "error": str(e)}), 200


if __name__ == "__main__":
    # Local run
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
