# app.py
from __future__ import annotations
import os
import asyncio
from flask import Flask, jsonify
from crawler.gov_il import fetch_retailers
from crawler.core import run_all
from crawler import logger

# Config
GOV_URL = os.getenv("GOV_URL", "https://www.gov.il/he/pages/cpfta_prices_regulations")
PORT = int(os.getenv("PORT", 8080))

app = Flask(__name__)


@app.get("/health")
def health():
    """Basic health check."""
    return jsonify({"ok": True}), 200


@app.post("/run")
@app.get("/run")
def run():
    """Trigger the full async crawling pipeline."""
    try:
        retailers = fetch_retailers(GOV_URL)
        count = len(retailers)
        app.logger.info(f"Discovered {count} retailers from gov.il")

        if not retailers:
            return jsonify({"status": "no_retailers_found"}), 200

        # Run crawler
        results = asyncio.run(run_all(retailers))
        return (
            jsonify({"status": "done", "retailers_count": count, "results": results}),
            200,
        )

    except Exception as e:
        app.logger.exception("Run failed")
        # Respond 200 to Cloud Scheduler to prevent retries
        return jsonify({"status": "error", "error": str(e)}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
