#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# rebuild-updated-services.sh
#
# Rebuilds and redeploys all services that were modified for injection scenario fixes.
# Run this after code changes to pick up the updates.
#
# Services updated:
#   - frontend: 500 status propagation on /search, /auth, /orders, /catalog
#   - api-gateway: 500 status propagation on /auth, /catalog, /orders
#   - payment-adapter: retry logic (3 attempts) for external-payment-api
#   - ranking-service: Redis cache read + expensive recomputation on miss (~150ms)
#   - recommendation-service: cache-miss ML scoring delay (~120ms) + write-back
#   - loyalty-service: cache-miss aggregation delay (~100ms)
#   - orders-service: inventory call added to list_orders
#
# Usage:
#   bash rebuild-updated-services.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"
REGISTRY="${REGISTRY:-causely-oss}"

SERVICES=(
  frontend
  api-gateway
  payment-adapter
  ranking-service
  recommendation-service
  loyalty-service
  orders-service
)

echo "=== Rebuilding ${#SERVICES[@]} updated services ==="
echo "  Registry  : $REGISTRY"
echo "  Namespace : $NAMESPACE"
echo ""

cd environment/services

for svc in "${SERVICES[@]}"; do
  echo "--- Building $svc ---"
  docker build --no-cache -t "${REGISTRY}/${svc}:latest" "./$svc"
  docker push "${REGISTRY}/${svc}:latest"
  echo ""
done

cd ../..

echo "=== Restarting deployments ==="
for svc in "${SERVICES[@]}"; do
  kubectl rollout restart deploy/"$svc" -n "$NAMESPACE"
done

echo ""
echo "=== Waiting for rollouts ==="
for svc in "${SERVICES[@]}"; do
  kubectl rollout status deploy/"$svc" -n "$NAMESPACE" --timeout=120s
done

echo ""
echo "=== All services rebuilt and deployed ==="
echo ""
echo "Wait ~60s for metrics to stabilize, then test injections:"
echo "  bash inject/inject_cpu_throttle.sh          # Scenario 2"
echo "  bash inject/inject_external_api_latency.sh  # Scenario 3"
echo "  bash inject/inject_redis_pressure.sh        # Scenario 4"
echo "  bash inject/inject_pod_errors.sh            # Scenario 5"
echo "  bash inject/inject_orders_retry_loop.sh     # Scenario 6"
echo "  bash inject/inject_discount_latency.sh      # Scenario 7"
echo "  bash inject/inject_deep_chain_latency.sh    # Scenario 8"
echo ""
echo "Restore all: bash inject/restore_all.sh"
