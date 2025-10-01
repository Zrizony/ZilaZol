#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Flask app wrapper for the cloud crawler with Google Cloud Storage integration
"""

from flask import Flask, request, jsonify
import os
from datetime import datetime

app = Flask(__name__)

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "price-crawler-cloud",
        "timestamp": datetime.now().isoformat(),
        "features": [
            "google_cloud_storage",
            "cloud_logging", 
            "automatic_uploads",
            "json_outputs",
            "zip_downloads",
            "storage_analytics",
            "automated_cleanup",
            "batch_operations"
        ],
        "endpoints": {
            "health": "/",
            "ping": "/ping (lightweight keepalive)",
            "test": "/test",
            "crawl": "/crawl (POST - runs full crawler)",
            "detailed_health": "/health/detailed",
            "storage_cleanup": "/storage/cleanup (POST)",
            "storage_analytics": "/storage/analytics",
            "storage_status": "/storage/status"
        }
    })

@app.route('/ping', methods=['GET', 'POST'])
def ping():
    """Lightweight keepalive endpoint for Cloud Scheduler warmup
    
    Use this endpoint to keep the service warm without triggering the crawler.
    Cloud Scheduler can hit this periodically to prevent cold starts.
    """
    return jsonify({
        "status": "ok",
        "service": "price-crawler-cloud",
        "timestamp": datetime.now().isoformat(),
        "message": "Service is alive and ready"
    }), 200

@app.route('/crawl', methods=['POST'])
def run_crawler():
    """Run the cloud crawler. Optional: target a single shop via query/body.

    Examples:
    - POST /crawl                 -> crawl all shops
    - POST /crawl?shop=rami_levi  -> crawl one shop
    - POST /crawl  {"shop":"rami_levi"} -> crawl one shop
    """
    try:
        # Import and run the cloud crawler only when called
        from crawler_cloud import main as crawler_main
        # Read 'shop' from query or JSON body
        target_shop = request.args.get('shop')
        if not target_shop:
            try:
                payload = request.get_json(silent=True) or {}
                target_shop = payload.get('shop')
            except Exception:
                target_shop = None
        
        # Run the cloud crawler (all or single shop)
        crawler_main(target_shop)
        
        return jsonify({
            "status": "success",
            "message": "Cloud crawler completed successfully",
            "timestamp": datetime.now().isoformat(),
            "shop": target_shop or "all",
            "features_used": [
                "Google Cloud Storage uploads",
                "Cloud logging",
                "Automatic file processing",
                "JSON data extraction",
                "Zip file downloads"
            ]
        }), 200
        
    except ImportError as e:
        return jsonify({
            "status": "error",
            "message": f"Import error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Runtime error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/test', methods=['GET'])
def test_endpoint():
    """Simple test endpoint"""
    return jsonify({
        "status": "success",
        "message": "Test endpoint working",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/run', methods=['POST'])
def fanout_run():
    """Fan-out runner: triggers per-shop crawls and returns immediately (202).

    This endpoint discovers retailers and sends internal HTTP requests to /crawl?shop=...
    It does not wait for completion to keep Scheduler within a short deadline.
    """
    try:
        from crawler_cloud import retailer_links, slug
        import requests as http
        base_url = os.environ.get('BASE_URL') or request.host_url.rstrip('/')

        links = retailer_links()
        triggered = []
        for shop in links.keys():
            shop_slug = slug(shop)
            try:
                http.post(f"{base_url}/crawl", json={"shop": shop_slug}, timeout=5)
                triggered.append(shop_slug)
            except Exception:
                pass

        return jsonify({
            "status": "accepted",
            "message": "Fan-out started",
            "triggered": triggered,
            "timestamp": datetime.now().isoformat()
        }), 202
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Fan-out error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/health/detailed', methods=['GET'])
def detailed_health():
    """Detailed health check with imports"""
    try:
        # Test imports
        import requests
        import playwright
        import google.cloud.storage
        import google.cloud.logging
        
        return jsonify({
            "status": "healthy",
            "imports": "all successful",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "import_error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/storage/cleanup', methods=['POST'])
def trigger_cleanup():
    """Manually trigger storage cleanup"""
    try:
        from crawler_cloud import cleanup_old_screenshots, get_storage_analytics
        
        # Get analytics before cleanup
        initial_analytics = get_storage_analytics()
        
        # Run cleanup
        deleted_count = cleanup_old_screenshots()
        
        # Get analytics after cleanup
        final_analytics = get_storage_analytics()
        
        # Calculate results
        results = {
            "status": "success",
            "message": f"Storage cleanup completed successfully",
            "timestamp": datetime.now().isoformat(),
            "cleanup_results": {
                "screenshots_deleted": deleted_count,
                "space_saved_bytes": 0,
                "files_removed": 0
            }
        }
        
        if initial_analytics and final_analytics:
            results["cleanup_results"]["space_saved_bytes"] = initial_analytics['total_size'] - final_analytics['total_size']
            results["cleanup_results"]["files_removed"] = initial_analytics['total_files'] - final_analytics['total_files']
        
        return jsonify(results), 200
        
    except ImportError as e:
        return jsonify({
            "status": "error",
            "message": f"Import error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Cleanup error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/storage/analytics', methods=['GET'])
def get_storage_analytics():
    """Get storage analytics and usage statistics"""
    try:
        from crawler_cloud import get_storage_analytics as crawler_analytics
        
        analytics = crawler_analytics()
        
        if analytics:
            return jsonify({
                "status": "success",
                "analytics": analytics,
                "timestamp": datetime.now().isoformat()
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to collect storage analytics",
                "timestamp": datetime.now().isoformat()
            }), 500
        
    except ImportError as e:
        return jsonify({
            "status": "error",
            "message": f"Import error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Analytics error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/storage/status', methods=['GET'])
def storage_status():
    """Get storage status and health"""
    try:
        from crawler_cloud import get_storage_client, BUCKET_NAME
        
        storage_client = get_storage_client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        if bucket.exists():
            # Get basic bucket info
            bucket.reload()
            
            return jsonify({
                "status": "healthy",
                "bucket": {
                    "name": BUCKET_NAME,
                    "exists": True,
                    "location": bucket.location,
                    "storage_class": bucket.storage_class,
                    "versioning_enabled": bucket.versioning_enabled,
                    "labels": bucket.labels or {}
                },
                "timestamp": datetime.now().isoformat()
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": f"Bucket {BUCKET_NAME} does not exist",
                "timestamp": datetime.now().isoformat()
            }), 404
        
    except ImportError as e:
        return jsonify({
            "status": "error",
            "message": f"Import error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Storage status error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    # Get port from environment variable (Cloud Run sets this)
    port = int(os.environ.get('PORT', 8080))
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=port, debug=False) 