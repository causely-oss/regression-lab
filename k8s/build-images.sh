#!/usr/bin/env bash

# build-images.sh
#
# Builds all service images directly for production cluster.
# Must be run before applying the Kubernetes manifests.
#
# Usage:
#   bash k8s/build-images.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICES_DIR="$SCRIPT_DIR/../environment/services"

REGISTRY="${REGISTRY:-docker.io/moyle123}"
PLATFORM="${PLATFORM:-linux/amd64}"
TAG="${TAG:-v1}"

SERVICES=(
  analytics-service
  api-gateway
  audit-service
  auth-service
  billing-service
  cache-service
  cart-service
  catalog-service
  checkout
  delivery-service
  discount-service
  email-service
  external-payment-api
  fraud-detection
  frontend
  ingest-service
  inventory-service
  loyalty-service
  media-service
  notification-service
  orders-service
  payment-adapter
  payments-api
  pricing-service
  processing-service
  profile-service
  ranking-service
  recommendation-service
  reporting-service
  review-service
  search-service
  session-service
  shipping-service
  tax-service
  user-service
  warehouse-service
)

for svc in "${SERVICES[@]}"; do
  echo ""
  echo "=== Building ${REGISTRY}/${svc}:${TAG} for ${PLATFORM} ==="
  docker buildx build \
    --platform "${PLATFORM}" \
    -t "${REGISTRY}/${svc}:${TAG}" \
    --push \
    "${SERVICES_DIR}/${svc}"
done

echo ""
echo "Built and pushed ${#SERVICES[@]} images with tag ${TAG}"
