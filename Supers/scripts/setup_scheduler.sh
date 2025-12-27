#!/bin/bash
# scripts/setup_scheduler.sh
#
# Create Cloud Scheduler jobs for Supers price crawler in me-west1 region.
#
# This script:
# - Creates scheduler jobs in me-west1 (same region as Cloud Run)
# - Sets up two jobs: one for credentialed retailers, one for public retailers
# - Configures OIDC authentication for secure Cloud Run invocation
# - Warns about any existing jobs in the wrong region (europe-west1)
#
# Usage:
#   ./scripts/setup_scheduler.sh
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - Cloud Scheduler API enabled
#   - Cloud Run service deployed to me-west1
#   - Service account with appropriate permissions

set -euo pipefail

# Configuration
PROJECT_ID="${PROJECT_ID:-civic-ripsaw-466109-e2}"
REGION="me-west1"
SERVICE_NAME="price-crawler"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-${PROJECT_ID}@appspot.gserviceaccount.com}"
TIMEZONE="Asia/Jerusalem"

# Get the Cloud Run service URL
echo "🔍 Fetching Cloud Run service URL..."
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} \
  --region ${REGION} \
  --project ${PROJECT_ID} \
  --format='value(status.url)' 2>/dev/null || echo "")

if [ -z "$SERVICE_URL" ]; then
  echo "❌ ERROR: Could not find Cloud Run service '${SERVICE_NAME}' in region '${REGION}'"
  echo "   Please deploy the service first with: ./scripts/deploy.sh"
  exit 1
fi

echo "✅ Found Cloud Run service: ${SERVICE_URL}"

# Check for existing jobs in wrong region
echo ""
echo "🔍 Checking for existing jobs in europe-west1 (wrong region)..."
WRONG_REGION_JOBS=$(gcloud scheduler jobs list \
  --location=europe-west1 \
  --project=${PROJECT_ID} \
  --format='value(name)' 2>/dev/null || echo "")

if [ -n "$WRONG_REGION_JOBS" ]; then
  echo "⚠️  WARNING: Found scheduler jobs in europe-west1 (wrong region):"
  echo "$WRONG_REGION_JOBS"
  echo ""
  echo "   These jobs will cause 503 errors due to region mismatch!"
  echo "   To delete them, run:"
  echo "     gcloud scheduler jobs delete JOB_NAME --location=europe-west1 --project=${PROJECT_ID}"
  echo ""
  read -p "   Do you want to continue creating jobs in ${REGION}? (y/n) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Aborted"
    exit 1
  fi
fi

# Function to create or update a scheduler job
create_or_update_job() {
  local JOB_NAME=$1
  local SCHEDULE=$2
  local URI=$3
  local DESCRIPTION=$4

  echo ""
  echo "📅 Creating/updating job: ${JOB_NAME}"
  echo "   Schedule: ${SCHEDULE}"
  echo "   URI: ${URI}"
  
  # Check if job already exists
  JOB_EXISTS=$(gcloud scheduler jobs describe ${JOB_NAME} \
    --location=${REGION} \
    --project=${PROJECT_ID} \
    --format='value(name)' 2>/dev/null || echo "")
  
  if [ -n "$JOB_EXISTS" ]; then
    echo "   Job already exists, updating..."
    gcloud scheduler jobs update http ${JOB_NAME} \
      --location=${REGION} \
      --project=${PROJECT_ID} \
      --schedule="${SCHEDULE}" \
      --uri="${URI}" \
      --http-method=POST \
      --oidc-service-account-email=${SERVICE_ACCOUNT} \
      --oidc-token-audience="${SERVICE_URL}" \
      --time-zone="${TIMEZONE}" \
      --description="${DESCRIPTION}" \
      --quiet
    echo "✅ Job updated: ${JOB_NAME}"
  else
    echo "   Creating new job..."
    gcloud scheduler jobs create http ${JOB_NAME} \
      --location=${REGION} \
      --project=${PROJECT_ID} \
      --schedule="${SCHEDULE}" \
      --uri="${URI}" \
      --http-method=POST \
      --oidc-service-account-email=${SERVICE_ACCOUNT} \
      --oidc-token-audience="${SERVICE_URL}" \
      --time-zone="${TIMEZONE}" \
      --description="${DESCRIPTION}" \
      --quiet
    echo "✅ Job created: ${JOB_NAME}"
  fi
}

# Create jobs
echo ""
echo "🚀 Setting up Cloud Scheduler jobs in ${REGION}..."
echo ""

# Job 1: Public retailers (every 3 hours)
create_or_update_job \
  "supers-crawler-public" \
  "0 */3 * * *" \
  "${SERVICE_URL}/run?group=public" \
  "Crawl public retailers (Bina, generic sites, Wolt) every 3 hours"

# Job 2: Credentialed retailers (every 6 hours)
create_or_update_job \
  "supers-crawler-creds" \
  "0 */6 * * *" \
  "${SERVICE_URL}/run?group=creds" \
  "Crawl credentialed retailers (PublishedPrices) every 6 hours"

# Job 3: All retailers (daily at 3 AM)
create_or_update_job \
  "supers-crawler-daily" \
  "0 3 * * *" \
  "${SERVICE_URL}/run" \
  "Crawl all retailers daily at 3 AM Jerusalem time"

# Summary
echo ""
echo "✨ Setup complete!"
echo ""
echo "📋 Summary:"
echo "   Region: ${REGION}"
echo "   Service: ${SERVICE_URL}"
echo "   Service Account: ${SERVICE_ACCOUNT}"
echo ""
echo "📅 Created jobs:"
echo "   - supers-crawler-public  → /run?group=public (every 3 hours)"
echo "   - supers-crawler-creds   → /run?group=creds (every 6 hours)"
echo "   - supers-crawler-daily   → /run (daily at 3 AM)"
echo ""
echo "🔧 To view jobs:"
echo "   gcloud scheduler jobs list --location=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🧪 To test a job manually:"
echo "   gcloud scheduler jobs run supers-crawler-public --location=${REGION} --project=${PROJECT_ID}"
echo ""
echo "📊 To view logs:"
echo "   gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=${SERVICE_NAME}' --limit=50 --format=json"
echo ""

