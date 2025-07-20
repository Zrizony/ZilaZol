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
            "zip_downloads"
        ]
    })

@app.route('/crawl', methods=['POST'])
def run_crawler():
    """Run the cloud crawler and return results"""
    try:
        # Import and run the cloud crawler only when called
        from crawler_cloud import main as crawler_main
        
        # Run the cloud crawler
        crawler_main()
        
        return jsonify({
            "status": "success",
            "message": "Cloud crawler completed successfully",
            "timestamp": datetime.now().isoformat(),
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

if __name__ == '__main__':
    # Get port from environment variable (Cloud Run sets this)
    port = int(os.environ.get('PORT', 8080))
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=port, debug=False) 