#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-civic-ripsaw-466109-e2}"
SERVICE="${SERVICE:-price-crawler}"
REGION="${REGION:-me-west1}"
BUCKET="${GCS_BUCKET:-civic-ripsaw-466109-e2-crawler-data}"
RELEASE="${RELEASE:-$(date -u +%Y%m%dT%H%MZ)-force}"
IMAGE="gcr.io/${PROJECT_ID}/supers:${RELEASE}"

echo "Building and pushing ${IMAGE}..."
gcloud builds submit --tag "${IMAGE}" --project "${PROJECT_ID}"

echo "Deploying to Cloud Run..."
gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform managed \
  --memory 16Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars "RELEASE=${RELEASE},GCS_BUCKET=${BUCKET}" \
  --allow-unauthenticated \
  --project "${PROJECT_ID}"

echo "Deployment complete. Current revision and traffic:"
gcloud run services describe "${SERVICE}" \
  --region "${REGION}" \
  --project "${PROJECT_ID}" \
  --format='value(status.latestReadyRevisionName,status.traffic)'

