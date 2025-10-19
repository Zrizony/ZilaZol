# app.py
from __future__ import annotations
import os
import asyncio
from flask import Flask, jsonify, request

from crawler.discovery import attach_routes
from crawler.core import run_all
from crawler import logger
from crawler.gov_il import fetch_retailers

app = Flask(__name__)
attach_routes(app)   # <-- mount /retailers & /retailers_async


@app.get("/health")
def health():
    return jsonify({"ok": True}), 200


@app.get("/version")
def version():
    return jsonify({"revision": os.getenv("K_REVISION", "dev")}), 200




@app.route("/run", methods=["POST", "GET"])
def run():
    try:
        retailers = fetch_retailers()
        app.logger.info(f"discovered_retailers_count={len(retailers)}")
        if not retailers:
            return jsonify({"status": "no_retailers_found"}), 200
        results = asyncio.run(run_all(retailers))
        return jsonify({"status": "done", "count": len(results), "results": results}), 200
    except Exception as e:
        app.logger.exception("Run failed")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
