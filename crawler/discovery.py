# crawler/discovery.py
from __future__ import annotations
import os
import json
from flask import jsonify, request
from .config_loader import load_config, resolve_retailers
from . import logger

def attach_routes(app):
    @app.get("/retailers")
    def retailers():
        debug = request.args.get("debug")
        try:
            cfg = load_config()
            items = resolve_retailers(cfg)
            if debug:
                return jsonify({
                    "count": len(items),
                    "items": [{"id": r.get("id"), "name": r.get("name"), "url": r.get("url"), "host": r.get("host"), "tags": r.get("tags")} for r in items],
                }), 200
            return jsonify({"count": len(items)}), 200
        except Exception as e:
            logger.exception("retailers_discovery_failed")
            return jsonify({"error": str(e)}), 500

    # Optional async test endpoint (does not block main loop)
    @app.get("/retailers_async")
    def retailers_async():
        try:
            cfg = load_config()
            items = resolve_retailers(cfg)
            return jsonify({
                "count": len(items),
                "items": [{"id": r.get("id"), "name": r.get("name"), "url": r.get("url"), "host": r.get("host"), "tags": r.get("tags")} for r in items],
            }), 200
        except Exception as e:
            logger.exception("retailers_async_failed")
            return jsonify({"error": str(e)}), 500