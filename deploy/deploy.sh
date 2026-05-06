#!/usr/bin/env bash
#
# Cloud Run deploy: build a container image, push to Artifact Registry,
# deploy a revision. Idempotent — re-running with the same args
# replaces the existing service revision.
#
# Required environment:
#   GCP_PROJECT       — target Google Cloud project id.
#   GCP_REGION        — e.g. us-central1.
#   GCS_BUCKET        — bucket the engine will read/write SSTables from.
#   AR_REPO           — Artifact Registry repo (must exist; create with
#                       `gcloud artifacts repositories create`).
#   SERVICE_NAME      — Cloud Run service name (default: zero-db).
#
# IAM the runtime service account must hold on $GCS_BUCKET:
#   roles/storage.objectAdmin
# (Cloud Run injects credentials via the metadata service; the auth
# module in src/storage/auth.zig fetches them automatically.)
#
# Run from the repo root:
#   GCP_PROJECT=foo GCP_REGION=us-central1 GCS_BUCKET=foo-zero-db \
#   AR_REPO=zero-db ./deploy/deploy.sh

set -euo pipefail

: "${GCP_PROJECT:?GCP_PROJECT is required}"
: "${GCP_REGION:?GCP_REGION is required}"
: "${GCS_BUCKET:?GCS_BUCKET is required}"
: "${AR_REPO:?AR_REPO is required}"
SERVICE_NAME="${SERVICE_NAME:-zero-db}"
# Optional dedicated runtime SA. When set, baked into the revision spec
# so Cloud Run uses it as the request-time identity. When unset, Cloud
# Run falls back to the project's default compute SA.
RUNTIME_SA="${RUNTIME_SA:-}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

GIT_SHA="$(git rev-parse --short=12 HEAD 2>/dev/null || echo "untagged")"
IMAGE="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/${AR_REPO}/${SERVICE_NAME}:${GIT_SHA}"

echo "==> Building image: ${IMAGE}"
# Cloud Run runs amd64/linux. Apple Silicon (and other arm64) hosts must
# cross-build or Cloud Run rejects the manifest.
docker build --platform linux/amd64 -f deploy/Dockerfile -t "${IMAGE}" .

echo "==> Pushing to Artifact Registry"
docker push "${IMAGE}"

echo "==> Deploying to Cloud Run"
SA_FLAG=()
if [[ -n "${RUNTIME_SA}" ]]; then
  echo "    runtime SA: ${RUNTIME_SA}"
  SA_FLAG=(--service-account "${RUNTIME_SA}")
fi

gcloud run deploy "${SERVICE_NAME}" \
  --project "${GCP_PROJECT}" \
  --region "${GCP_REGION}" \
  --image "${IMAGE}" \
  --platform managed \
  --allow-unauthenticated \
  --port 8080 \
  --set-env-vars "GCS_BUCKET=${GCS_BUCKET}" \
  --memory 512Mi \
  --cpu 1 \
  --concurrency 80 \
  --timeout 60s \
  --max-instances 1 \
  --no-cpu-throttling \
  "${SA_FLAG[@]}"

echo "==> Live: ${SERVICE_NAME} (${GIT_SHA})"
gcloud run services describe "${SERVICE_NAME}" \
  --project "${GCP_PROJECT}" \
  --region "${GCP_REGION}" \
  --format 'value(status.url)'
