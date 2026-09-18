#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# build-images.sh
#
# Builds all custom service images directly into minikube's Docker daemon.
# Must be run before applying the Kubernetes manifests.
#
# Usage:
#   bash k8s/build-images.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICES_DIR="$SCRIPT_DIR/../environment/services"

echo "=== Pointing Docker CLI at minikube's daemon ==="
eval "$(minikube docker-env)"

SERVICES=(analytics-service api-gateway audit-service auth-service billing-service cache-service cart-service catalog-service checkout delivery-service discount-service email-service external-payment-api fraud-detection frontend ingest-service inventory-service loyalty-service media-service notification-service orders-service payment-adapter payments-api pricing-service processing-service profile-service ranking-service recommendation-service reporting-service review-service search-service session-service shipping-service tax-service user-service warehouse-service)

for svc in "${SERVICES[@]}"; do
  echo ""
  echo "=== Building scenario01/$svc:latest ==="
  docker build -t "scenario01/$svc:latest" "$SERVICES_DIR/$svc"
done

echo ""
echo "=== All ${#SERVICES[@]} images built ==="
docker images | grep scenario01
