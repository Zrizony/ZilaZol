# Price Crawler - Israeli Retail Price Comparison

A cloud-based web crawler for Israeli retail price comparison, built with Python, Flask, and Google Cloud Platform.

## 🚀 Features

- **Automated Price Crawling**: Scrapes prices from Israeli retailer portals
- **Cloud-Native**: Built for Google Cloud Run with automatic scaling
- **Data Processing**: Extracts and processes XML/JSON price data from .zip and .gz files
- **Cloud Storage**: Automatically uploads data to Google Cloud Storage
- **REST API**: Flask-based API for triggering crawls and health checks
- **Scheduled Execution**: Automated daily runs via Cloud Scheduler
- **Storage Optimization**: Enhanced GCS operations with retry logic and batch processing
- **Automated Cleanup**: 2-day screenshot retention with intelligent cleanup
- **Storage Analytics**: Comprehensive storage usage monitoring and reporting

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Cloud Run     │    │  Cloud Storage  │    │ Cloud Scheduler │
│   (Flask App)   │◄──►│   (Data Store)  │    │  (Daily Trig.)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌─────────────────┐
│   Playwright    │    │   Cloud Logging │
│  (Web Scraping) │    │   (Monitoring)  │
└─────────────────┘    └─────────────────┘
```

## 🛠️ Tech Stack

- **Backend**: Python 3.11, Flask
- **Web Scraping**: Playwright, BeautifulSoup4
- **Cloud Platform**: Google Cloud Run, Cloud Storage, Cloud Scheduler
- **Data Processing**: XML parsing, JSON processing
- **Monitoring**: Cloud Logging

## 📁 Project Structure

```
crawler/
├── app.py                 # Flask application
├── crawler_cloud.py       # Main crawler logic
├── requirements.txt       # Python dependencies
├── Dockerfile            # Container configuration
├── deploy.ps1            # Deployment script
├── test_local.py         # Local testing script
├── .gitignore            # Git ignore rules
├── README.md             # This file
└── old/                  # Legacy files
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Google Cloud Platform account
- Git

### Local Development

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd crawler
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Test locally**
   ```bash
   python test_local.py
   ```

4. **Run Flask app**
   ```bash
   python app.py
   ```

### Cloud Deployment

#### Option 1: Manual Deployment
```bash
# Run the deployment script
.\deploy.ps1
```

#### Option 2: Google Cloud Console
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to Cloud Run → Create Service
3. Connect to GitHub repository
4. Configure automatic deployment

#### Option 3: GitHub Actions (Recommended)
1. Push to GitHub
2. Cloud Build automatically deploys to Cloud Run
3. Updates on every push to main branch

## 🔧 Configuration

### Environment Variables

- `GCS_BUCKET_NAME`: Google Cloud Storage bucket name
- `GOOGLE_CLOUD_PROJECT`: Google Cloud project ID
- `PORT`: Flask app port (default: 8080)

### Cloud Run Settings

- **Memory**: 2Gi
- **CPU**: 2 vCPU
- **Timeout**: 3600 seconds
- **Max Instances**: 1
- **Region**: me-west1 (Tel Aviv)

## 📡 API Endpoints

### Health Check
```http
GET /
```
Returns service status and features.

### Test Endpoint
```http
GET /test
```
Simple test endpoint for connectivity.

### Ping (Keepalive)
```http
GET|POST /ping
```
Lightweight keepalive endpoint for service warmup. Use this with Cloud Scheduler to prevent cold starts.
- Response time: < 50ms
- No heavy operations
- Returns: `{"status": "ok", "message": "Service is alive and ready"}`

**Use case**: Configure Cloud Scheduler to hit this endpoint every 5-10 minutes to keep the service warm.

### Trigger Crawler
```http
POST /crawl
```
Triggers the complete price crawling process (5-30 minutes).
- Scrapes all retailer websites
- Downloads and processes ZIP files
- Uploads data to Cloud Storage
- Performs cleanup operations

#### Per-shop crawl
You can target a single retailer to keep runtime short:

```http
POST /crawl?shop=<slug>
POST /crawl
{ "shop": "<slug>" }
```

Example: `POST /crawl?shop=RamiLevi` or `POST /crawl?shop=rami_levi` (slug/name both accepted).

### Fan-out Runner
```http
POST /run
```
Starts one request per retailer and returns 202 immediately (does not wait for completion). Use this when you want a single Scheduler job and per-shop crawling behind the scenes.

### Detailed Health Check
```http
GET /health/detailed
```
Tests all imports and dependencies.

### Storage Management

#### Storage Status
```http
GET /storage/status
```
Returns storage bucket health and configuration information.

#### Storage Analytics
```http
GET /storage/analytics
```
Returns comprehensive storage usage statistics including:
- Total file count and size
- File type distribution
- Folder structure analysis
- Creation time ranges

#### Storage Cleanup
```http
POST /storage/cleanup
```
Manually triggers the 2-day screenshot cleanup process.
Returns cleanup results including:
- Number of screenshots deleted
- Space saved in bytes
- Total files removed

## 🔄 Continuous Deployment

### GitHub Integration

1. **Connect Repository**
   - Go to Google Cloud Console
   - Navigate to Cloud Run
   - Click "Create Service"
   - Choose "Continuously deploy from a source repository"
   - Connect your GitHub repository

2. **Automatic Deployment**
   - Every push to `main` branch triggers deployment
   - Cloud Build automatically builds and deploys
   - No manual intervention required

### Manual Trigger

```bash
# Trigger crawler manually
curl -X POST https://price-crawler-947639158495.me-west1.run.app/crawl

# Ping service (warmup)
curl https://price-crawler-947639158495.me-west1.run.app/ping
```

## ⏰ Cloud Scheduler Setup

Configure automated scheduling for the crawler:

### Option 1: Daily Crawler Only
```bash
.\setup_scheduler.ps1
```
Sets up daily crawler at 7 AM (Jerusalem time).

### Option 2: Warmup + Crawler (Recommended)
```bash
# Setup warmup (every 5 minutes to prevent cold starts)
.\setup_scheduler_warmup.ps1

# Setup daily crawler
.\setup_scheduler.ps1
```
Keeps service warm and runs daily crawler.

**📚 See [SCHEDULER_SETUP.md](SCHEDULER_SETUP.md) for detailed configuration options.**

## 📊 Monitoring

### Cloud Logging
- All logs are automatically sent to Cloud Logging
- Search and filter logs in Google Cloud Console
- Set up alerts for errors

### Health Monitoring
- Regular health checks via `/` endpoint
- Detailed dependency checks via `/health/detailed`
- Automatic restart on failures

## 🔒 Security

- **Authentication**: Cloud Run handles authentication
- **Secrets**: Use Google Secret Manager for sensitive data
- **Network**: Runs in isolated Google Cloud environment
- **Updates**: Automatic security updates via container rebuilds

## 🗄️ Storage Optimization & Cleanup

### Automated Screenshot Cleanup
- **Retention Policy**: Screenshots are automatically deleted after 2 days
- **Intelligent Parsing**: Multiple date format support for accurate age detection
- **Batch Operations**: Efficient batch deletion with configurable batch sizes
- **Error Handling**: Comprehensive error handling with retry logic

### Enhanced GCS Operations
- **Retry Logic**: Automatic retry with exponential backoff for failed operations
- **Connection Pooling**: Optimized client connections with timeout management
- **Batch Processing**: Efficient batch operations for large file sets
- **Metadata Management**: Enhanced file metadata for better organization

### Storage Analytics
- **Usage Monitoring**: Real-time storage usage statistics
- **Performance Metrics**: Upload/download success rates and timing
- **File Distribution**: Analysis of file types and folder structures
- **Space Optimization**: Tracking of space saved through cleanup operations

### Configuration Options
```python
# GCS operation settings
GCS_BATCH_SIZE = 100          # Batch size for bulk operations
GCS_RETRY_ATTEMPTS = 3        # Number of retry attempts
GCS_TIMEOUT = 30              # Timeout for operations (seconds)
SCREENSHOT_RETENTION_DAYS = 2 # Screenshot retention period
```

## 🧪 Testing

### Local Testing
```bash
python test_local.py
```

### Storage Optimization Testing
```bash
# Test all storage features (including cleanup)
.\test_storage_optimization.ps1

# Test without cleanup (for development)
.\test_storage_optimization.ps1 -TestCleanup:$false

# Test against deployed service
.\test_storage_optimization.ps1 -BaseUrl "https://price-crawler-947639158495.me-west1.run.app"
```

### API Testing
```bash
# Health check
curl https://price-crawler-947639158495.me-west1.run.app/

# Test endpoint
curl https://price-crawler-947639158495.me-west1.run.app/test

# Trigger crawler
curl -X POST https://price-crawler-947639158495.me-west1.run.app/crawl

# Storage status
curl https://price-crawler-947639158495.me-west1.run.app/storage/status

# Storage analytics
curl https://price-crawler-947639158495.me-west1.run.app/storage/analytics

# Trigger cleanup
curl -X POST https://price-crawler-947639158495.me-west1.run.app/storage/cleanup
```

## 📈 Performance

- **Response Time**: < 2 seconds for health checks
- **Crawling Speed**: ~1000 products/minute
- **Uptime**: 99.9% (Cloud Run SLA)
- **Scalability**: Automatic scaling based on demand

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License.

## 🆘 Support

For issues and questions:
1. Check the logs in Google Cloud Console
2. Review the API documentation
3. Create an issue in the GitHub repository

## 🔄 Changelog

### v1.0.0
- Initial release
- Cloud Run deployment
- Automated crawling
- REST API endpoints
- Cloud Storage integration 