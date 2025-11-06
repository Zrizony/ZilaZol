#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-civic-ripsaw-466109-e2}"
REGION="${REGION:-me-west1}"
SERVICE="${SERVICE:-price-crawler}"

URL="$(gcloud run services describe "${SERVICE}" --region "${REGION}" --project "${PROJECT_ID}" --format='value(status.url)')"

echo "Service URL: ${URL}"
echo ""
echo "# /__version"
curl -s "${URL}/__version" | jq '.' || curl -s "${URL}/__version"
echo ""
echo "# /__env"
curl -s "${URL}/__env" | jq '.' || curl -s "${URL}/__env"
echo ""
echo "# /__smoke"
curl -s -X POST "${URL}/__smoke" | jq '.' || curl -s -X POST "${URL}/__smoke"
echo ""

