#!/usr/bin/env python3
"""
Test script to verify Flask installation and imports
"""

def test_flask_import():
    """Test if Flask can be imported successfully"""
    try:
        import flask
        print(f"✅ Flask imported successfully")
        print(f"   Version: {flask.__version__}")
        return True
    except ImportError as e:
        print(f"❌ Flask import failed: {e}")
        return False

def test_flask_app_creation():
    """Test if Flask app can be created"""
    try:
        from flask import Flask
        app = Flask(__name__)
        print(f"✅ Flask app created successfully")
        return True
    except Exception as e:
        print(f"❌ Flask app creation failed: {e}")
        return False

def test_other_imports():
    """Test other required imports"""
    imports_to_test = [
        ('requests', 'requests'),
        ('beautifulsoup4', 'bs4'),
        ('playwright', 'playwright'),
        ('google.cloud.storage', 'google.cloud.storage'),
        ('lxml', 'lxml')
    ]
    
    all_success = True
    for package_name, import_name in imports_to_test:
        try:
            __import__(import_name)
            print(f"✅ {package_name} imported successfully")
        except ImportError as e:
            print(f"❌ {package_name} import failed: {e}")
            all_success = False
    
    return all_success

if __name__ == "__main__":
    print("🧪 Testing Flask and dependencies installation...")
    print("=" * 50)
    
    flask_ok = test_flask_import()
    app_ok = test_flask_app_creation()
    other_ok = test_other_imports()
    
    print("=" * 50)
    if flask_ok and app_ok and other_ok:
        print("🎉 All tests passed! Flask installation is working correctly.")
        exit(0)
    else:
        print("❌ Some tests failed. Please check the installation.")
        exit(1)
