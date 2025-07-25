# Zilazol - Israeli Retail Price Crawler

A cloud-based web crawler for Israeli retail price comparison, built with Python, Flask, and Google Cloud Platform.

## 📁 Project Structure

```
Zilazol/
├── crawler/                    # Main application directory
│   ├── app.py                 # Flask web application
│   ├── crawler_cloud.py       # Main crawler logic
│   ├── requirements.txt       # Python dependencies
│   ├── Dockerfile            # Container configuration
│   ├── .dockerignore         # Docker ignore rules
│   ├── deploy.ps1            # Deployment script
│   ├── setup_scheduler.ps1   # Cloud Scheduler setup
│   ├── README.md             # Detailed documentation
│   ├── .gitignore            # Git ignore rules
│   ├── json_parser.py        # JSON data processing
│   ├── nextjs_integration.py # Next.js frontend integration
│   ├── api_structure.py      # API structure definitions
│   ├── view_json_sample.py   # JSON viewing utilities
│   ├── nextjs_setup_guide.md # Next.js setup instructions
│   ├── unified_json/         # Processed data files
│   └── old/                  # Legacy files
└── .git/                     # Git repository
```

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Google Cloud Platform account
- Git

### Development
1. Navigate to the crawler directory:
   ```bash
   cd crawler
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run locally:
   ```bash
   python app.py
   ```

### Deployment
1. Navigate to the crawler directory:
   ```bash
   cd crawler
   ```

2. Deploy to Google Cloud Run:
   ```bash
   .\deploy.ps1
   ```

3. Set up automated scheduling:
   ```bash
   .\setup_scheduler.ps1
   ```

## 🌐 Live Service

- **Service URL**: https://price-crawler-947639158495.me-west1.run.app
- **Health Check**: https://price-crawler-947639158495.me-west1.run.app/
- **API Documentation**: See `crawler/README.md`

## 📊 Current Status

✅ **Fully Operational**
- Cloud crawler deployed and working
- All API endpoints functional
- Data processing complete (50MB+ processed)
- Automated scheduling ready (daily at 7 AM)

## 📈 Features

- **Automated Price Crawling**: Scrapes prices from Israeli retailer portals
- **Cloud-Native**: Built for Google Cloud Run with automatic scaling
- **Data Processing**: Extracts and processes XML/JSON price data
- **Cloud Storage**: Automatically uploads data to Google Cloud Storage
- **REST API**: Flask-based API for triggering crawls and health checks
- **Scheduled Execution**: Automated daily runs via Cloud Scheduler

## 📚 Documentation

For detailed technical documentation, deployment guides, and API references, see:
- **`crawler/README.md`** - Complete technical documentation
- **`crawler/nextjs_setup_guide.md`** - Next.js integration guide
- **`crawler/api_structure.py`** - API structure definitions

## 🛠️ Tech Stack

- **Backend**: Python 3.11, Flask
- **Web Scraping**: Playwright, BeautifulSoup4
- **Cloud Platform**: Google Cloud Run, Cloud Storage, Cloud Scheduler
- **Data Processing**: XML parsing, JSON processing
- **Monitoring**: Cloud Logging
- **Frontend**: Next.js (planned)

## 🎯 Next Steps

1. **Data Collection**: ✅ Automated daily crawler running
2. **Backend API**: ✅ Cloud-based API ready
3. **Frontend Development**: 🚧 Next.js full-stack app (in progress)
4. **Analytics Dashboard**: 📋 Price comparison features 