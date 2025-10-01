# Cloud Scheduler Setup Guide

This guide explains how to configure Cloud Scheduler to work with the price crawler service.

## 🎯 Available Endpoints

### `/ping` - Service Warmup (Keepalive)
- **Method**: GET or POST
- **Purpose**: Lightweight endpoint to keep service warm and prevent cold starts
- **Response Time**: < 50ms
- **Use Case**: Periodic pings to maintain instance readiness

**Response:**
```json
{
  "status": "ok",
  "service": "price-crawler-cloud",
  "timestamp": "2025-10-01T19:58:00.730Z",
  "message": "Service is alive and ready"
}
```

### `/crawl` - Full Crawler Execution
- **Method**: POST
- **Purpose**: Runs the complete price scraping and data upload process
- **Response Time**: 5-30 minutes (depending on data volume)
- **Use Case**: Scheduled daily price collection

**Response:**
```json
{
  "status": "success",
  "message": "Cloud crawler completed successfully",
  "timestamp": "2025-10-01T07:00:00.000Z",
  "features_used": [...]
}
```

---

## 📅 Recommended Scheduler Configuration

### Option 1: Crawler Only (Simple Setup)

Run the crawler once per day:

```bash
# Setup daily crawler at 7 AM
.\setup_scheduler.ps1
```

**Job Details:**
- **Name**: `daily-crawler`
- **Schedule**: `0 7 * * *` (7 AM daily)
- **Endpoint**: `POST /crawl`
- **Timeout**: 1800s (30 minutes)

**Pros:**
- ✅ Simple setup
- ✅ One job to manage

**Cons:**
- ❌ First run each day may have cold start delay (10-30s)

---

### Option 2: Warmup + Crawler (Recommended)

Keep service warm AND run daily crawler:

```bash
# Setup warmup job (every 5 minutes)
.\setup_scheduler_warmup.ps1

# Setup daily crawler at 7 AM
.\setup_scheduler.ps1
```

**Job Details:**

**Warmup Job:**
- **Name**: `price-crawler-warmup`
- **Schedule**: `*/5 * * * *` (every 5 minutes)
- **Endpoint**: `GET /ping`
- **Timeout**: 60s

**Crawler Job:**
- **Name**: `daily-crawler`
- **Schedule**: `0 7 * * *` (7 AM daily)
- **Endpoint**: `POST /crawl`
- **Timeout**: 1800s (30 minutes)

**Pros:**
- ✅ No cold starts
- ✅ Faster response times
- ✅ Better reliability

**Cons:**
- ❌ Slightly higher costs (minimal - warmup uses <1MB RAM)
- ❌ Two jobs to manage

---

## 🔧 Manual Commands

### Create Warmup Job (Every 5 Minutes)

```bash
gcloud scheduler jobs create http price-crawler-warmup \
    --location=europe-west1 \
    --schedule="*/5 * * * *" \
    --uri="https://price-crawler-947639158495.me-west1.run.app/ping" \
    --http-method=GET \
    --description="Keep price crawler service warm" \
    --project=civic-ripsaw-466109-e2
```

### Create Daily Crawler Job (7 AM)

```bash
gcloud scheduler jobs create http daily-crawler \
    --location=europe-west1 \
    --schedule="0 7 * * *" \
    --uri="https://price-crawler-947639158495.me-west1.run.app/crawl" \
    --http-method=POST \
    --description="Daily price crawler run" \
    --time-zone="Asia/Jerusalem" \
    --attempt-deadline="1800s" \
    --project=civic-ripsaw-466109-e2
```

---

## 📊 Schedule Examples

| Use Case | Schedule | Cron | Endpoint |
|----------|----------|------|----------|
| Keep service warm | Every 5 min | `*/5 * * * *` | GET /ping |
| Keep service warm | Every 10 min | `*/10 * * * *` | GET /ping |
| Daily at 7 AM | Once daily | `0 7 * * *` | POST /crawl |
| Twice daily | 7 AM & 7 PM | `0 7,19 * * *` | POST /crawl |
| Every 6 hours | 4x daily | `0 */6 * * *` | POST /crawl |
| Business hours only | Mon-Fri 9 AM | `0 9 * * 1-5` | POST /crawl |

---

## 🧪 Testing Endpoints

### Test Ping (Warmup)
```bash
curl https://price-crawler-947639158495.me-west1.run.app/ping
```

### Test Crawler (Manual Run)
```bash
curl -X POST https://price-crawler-947639158495.me-west1.run.app/crawl
```

### Test from PowerShell
```powershell
# Ping
Invoke-RestMethod -Uri "https://price-crawler-947639158495.me-west1.run.app/ping"

# Crawl
Invoke-RestMethod -Uri "https://price-crawler-947639158495.me-west1.run.app/crawl" -Method POST
```

---

## 📈 Cost Optimization

### Strategy 1: Minimal Cost
- **Setup**: Crawler only, no warmup
- **Cold starts**: Yes (10-30s delay)
- **Monthly requests**: ~30 (daily)
- **Estimated cost**: ~$0.50/month

### Strategy 2: Balanced (Recommended)
- **Setup**: Warmup every 10 minutes + daily crawler
- **Cold starts**: No
- **Monthly requests**: ~4,380 (ping) + 30 (crawl)
- **Estimated cost**: ~$2-3/month

### Strategy 3: Always Warm
- **Setup**: Warmup every 5 minutes + daily crawler
- **Cold starts**: Never
- **Monthly requests**: ~8,760 (ping) + 30 (crawl)
- **Estimated cost**: ~$3-5/month

---

## 🔍 Monitoring

### View Scheduler Logs
```bash
gcloud scheduler jobs describe daily-crawler --location=europe-west1
```

### View Service Logs
```bash
gcloud logging read "resource.type=cloud_run_revision" --limit 50 --project=civic-ripsaw-466109-e2
```

### Check Last Run Status
```bash
gcloud scheduler jobs list --location=europe-west1
```

---

## ❓ FAQ

**Q: Why use /ping instead of /crawl for warmup?**
A: `/ping` is extremely lightweight (<50ms, minimal memory), while `/crawl` runs for 5-30 minutes and uses significant resources.

**Q: What happens if I don't use warmup?**
A: The first request each day will have a cold start (10-30s delay). Subsequent requests within ~15 minutes will be fast.

**Q: Can I use both warmup and crawler?**
A: Yes! This is the recommended approach for production use.

**Q: How do I pause the scheduler?**
```bash
gcloud scheduler jobs pause daily-crawler --location=europe-west1
```

**Q: How do I resume?**
```bash
gcloud scheduler jobs resume daily-crawler --location=europe-west1
```

**Q: How do I delete a job?**
```bash
gcloud scheduler jobs delete daily-crawler --location=europe-west1
```

---

## 📚 Additional Resources

- [Cloud Scheduler Documentation](https://cloud.google.com/scheduler/docs)
- [Cron Expression Guide](https://crontab.guru/)
- [Cloud Run Pricing](https://cloud.google.com/run/pricing)

