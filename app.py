# app.py
from __future__ import annotations
import os
import asyncio
from flask import Flask, jsonify
from crawler.discovery import discover_retailers
from crawler.core import run_all
from crawler import logger

GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"

app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify({"ok": True}), 200

@app.post("/run")
def trigger_run():
    try:
        retailers = asyncio.run(discover_retailers(GOV_URL))
        if not retailers:
            return jsonify({"error": "no_retailers_discovered"}), 500
        results = asyncio.run(run_all(retailers))
        return jsonify({"count": len(results), "results": results}), 200
    except Exception as e:
        logger.exception("run_failed")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Local dev only; Cloud Run uses Gunicorn CMD
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
