# Price Crawler - Cloud-Optimized Version 🚀

A robust, cloud-native price crawler for Israeli retailers with Google Cloud Storage integration and automated scheduling.

## 🎯 Features

- **🔄 Async Playwright**: Fixed async/sync conflicts for Cloud Run
- **☁️ Google Cloud Storage**: Automatic file uploads and management
- **📊 Cloud Logging**: Centralized logging with Google Cloud Logging
- **⏰ Automated Scheduling**: Daily crawls with service warmup
- **🛡️ Error Handling**: Robust error handling and retry logic
- **📈 Analytics**: Storage analytics and cleanup automation

## 📁 Project Structure

```
crawler/
├── 🐍 Core Files
│   ├── crawler_cloud.py          # Main crawler (sync utilities)
│   ├── crawler_async.py          # Async crawler for Flask
│   └── app.py                    # Flask web service
├── 🚀 Deployment
│   ├── deploy.ps1                # Deploy to Cloud Run
│   ├── Dockerfile                # Container configuration
│   └── requirements.txt          # Python dependencies
├── ⏰ Scheduling
│   └── setup_scheduler_complete.ps1  # Complete scheduler setup
├── 🧪 Testing
│   └── test_crawler.py           # Comprehensive test suite
├── 📊 Data
│   └── unified_json/             # Processed data files
├── 📚 Documentation
│   ├── README.md                 # Original documentation
│   ├── README_CLEAN.md           # This file
│   └── SCHEDULER_SETUP.md        # Scheduler documentation
└── 🛠️ Utilities
    ├── test_docker_build.ps1     # Docker testing
    └── test_flask_docker_build.ps1
```

## 🚀 Quick Start

### 1. Deploy to Cloud Run
```powershell
.\deploy.ps1
```

### 2. Setup Automated Scheduling
```powershell
.\setup_scheduler_complete.ps1
```

### 3. Test the System
```powershell
python test_crawler.py
```

## 🔧 Configuration

### Environment Variables
- `GCS_BUCKET_NAME`: Google Cloud Storage bucket (default: `civic-ripsaw-466109-e2-crawler-data`)
- `GOOGLE_CLOUD_PROJECT`: GCP project ID (default: `civic-ripsaw-466109-e2`)
- `PORT`: Flask port (default: 8080, set by Cloud Run)

### Cloud Storage Structure
```
bucket/
├── downloads/           # Original zip/gz files
│   └── {shop_name}/
├── json_outputs/        # Processed JSON data
│   └── {shop_name}/
└── screenshots/         # Login screenshots (auto-cleanup)
    └── login_{shop}_{timestamp}.png
```

## 📡 API Endpoints

### Health & Status
- `GET /` - Health check with service info
- `GET /ping` - Lightweight keepalive (for warmup)
- `GET /test` - Simple test endpoint
- `GET /health/detailed` - Detailed health with imports

### Crawler Operations
- `POST /crawl` - Run full crawler (all shops)
- `POST /crawl?shop={name}` - Run crawler for specific shop
- `POST /run` - Async crawler (recommended for scheduler)

### Storage Management
- `POST /storage/cleanup` - Manual storage cleanup
- `GET /storage/analytics` - Storage usage analytics
- `GET /storage/status` - Storage health status

## ⏰ Scheduler Configuration

### Warmup Job (Every 5 Minutes)
- **Purpose**: Keep service warm, prevent cold starts
- **Endpoint**: `GET /ping`
- **Timeout**: 60 seconds
- **Cost**: Minimal (<$1/month)

### Daily Crawler (7 AM Jerusalem Time)
- **Purpose**: Full price data collection
- **Endpoint**: `POST /run`
- **Timeout**: 30 minutes
- **Features**: Async processing, error handling

## 🧪 Testing

### Comprehensive Test Suite
```powershell
python test_crawler.py
```

**Test Coverage:**
- ✅ Import tests
- ✅ Basic function tests
- ✅ Flask endpoint tests
- ✅ Cloud storage tests
- ✅ Async crawler tests
- ✅ Integration tests

### Manual Testing
```powershell
# Test health endpoint
curl https://your-service-url.run.app/ping

# Test crawler endpoint
curl -X POST https://your-service-url.run.app/run
```

## 🔍 Monitoring

### Cloud Logging
```bash
# View crawler logs
gcloud logging read "resource.type=cloud_run_revision" --limit 50

# View scheduler logs
gcloud logging read "resource.type=cloud_scheduler_job" --limit 50
```

### Scheduler Management
```bash
# List scheduler jobs
gcloud scheduler jobs list --location=europe-west1

# Pause crawler
gcloud scheduler jobs pause daily-crawler --location=europe-west1

# Resume crawler
gcloud scheduler jobs resume daily-crawler --location=europe-west1
```

## 🛠️ Troubleshooting

### Common Issues

**1. Playwright Async Errors**
- ✅ **Fixed**: Use `crawler_async.py` instead of sync version
- ✅ **Solution**: Flask now uses `asyncio.run()` properly

**2. Scheduler Timeouts**
- ✅ **Fixed**: 30-minute timeout for crawler job
- ✅ **Solution**: Use `/run` endpoint with async processing

**3. Storage Issues**
- ✅ **Fixed**: Retry logic and batch operations
- ✅ **Solution**: Automatic cleanup and error handling

**4. Cold Starts**
- ✅ **Fixed**: Warmup job every 5 minutes
- ✅ **Solution**: Service stays responsive

### Debug Commands
```powershell
# Test locally
python test_crawler.py

# Check service status
gcloud run services describe price-crawler --region=me-west1

# View recent logs
gcloud logging read "resource.type=cloud_run_revision" --limit 10
```

## 📊 Performance

### Optimizations
- **Async Processing**: No more sync/async conflicts
- **Batch Operations**: Efficient GCS uploads/deletes
- **Smart Cleanup**: Automatic old file removal
- **Connection Pooling**: Reused HTTP connections
- **Error Recovery**: Graceful failure handling

### Expected Performance
- **Cold Start**: ~10-15 seconds (with warmup: <1 second)
- **Daily Crawl**: 10-30 minutes (depending on data volume)
- **Storage**: Automatic cleanup keeps costs low
- **Reliability**: 99%+ uptime with proper error handling

## 🔄 Updates & Maintenance

### Regular Maintenance
1. **Monitor logs** for errors or performance issues
2. **Check storage analytics** to ensure cleanup is working
3. **Update credentials** if retailer logins change
4. **Review scheduler jobs** for proper execution

### Scaling
- **Increase timeout** if crawler takes longer than 30 minutes
- **Add more warmup frequency** if cold starts are problematic
- **Implement parallel processing** for faster crawls (future enhancement)

## 📚 Additional Resources

- [Google Cloud Run Documentation](https://cloud.google.com/run/docs)
- [Cloud Scheduler Documentation](https://cloud.google.com/scheduler/docs)
- [Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [Playwright Async API](https://playwright.dev/python/docs/async)

## 🎉 Success Indicators

Your crawler is working correctly when you see:
- ✅ Scheduler jobs running successfully
- ✅ Files appearing in Cloud Storage
- ✅ No async/sync errors in logs
- ✅ Regular data updates in `json_outputs/`
- ✅ Automatic screenshot cleanup

---

**Ready to crawl! 🕷️** Your price crawler is now optimized, reliable, and ready for production use.
