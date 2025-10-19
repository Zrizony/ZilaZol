# crawler/discovery.py
from __future__ import annotations
import os
import json
from flask import jsonify, request
from .gov_il import fetch_retailers, fetch_retailers_async
from . import logger

def attach_routes(app):
    @app.get("/retailers")
    def retailers():
        debug = request.args.get("debug")
        try:
            items = fetch_retailers()
            if debug:
                return jsonify({
                    "count": len(items),
                    "items": [{"name": n, "url": u} for n, u in items],
                }), 200
            return jsonify({"count": len(items)}), 200
        except Exception as e:
            logger.exception("retailers_discovery_failed")
            return jsonify({"error": str(e)}), 500

    # Optional async test endpoint (does not block main loop)
    @app.get("/retailers_async")
    def retailers_async():
        try:
            # run discovery async with a fresh loop to avoid "no event loop" errors
            items = fetch_retailers()
            return jsonify({
                "count": len(items),
                "items": [{"name": n, "url": u} for n, u in items],
            }), 200
        except Exception as e:
            logger.exception("retailers_async_failed")
            return jsonify({"error": str(e)}), 500