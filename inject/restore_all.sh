#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# restore_all.sh
#
# Restores ALL services to baseline. Run this to clean up after any injection.
#
# Usage:
#   bash inject/restore_all.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring ALL services to baseline ==="
echo "  Namespace : $NAMESPACE"
echo ""

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  # Reset all pods in the deployment — configs are stored in-memory per-pod
  kubectl get pods -n "$NAMESPACE" -l app="$svc" -o name 2>/dev/null | while read pod; do
    kubectl exec -n "$NAMESPACE" "$pod" -- \
      wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
      2>&1 || echo "    (skipped — $pod not reachable)"
  done
}

# Reset all services with admin endpoints
inject checkout                8082 "latency_ms=0&error_rate=0.0"
inject inventory-service       8086 "latency_ms=0&error_rate=0.0&pause_consumer=false"
inject orders-service          8085 "latency_ms=0&error_rate=0.0"
inject search-service          8088 "latency_ms=0&error_rate=0.0"
inject ranking-service         8089 "latency_ms=0&error_rate=0.0"
inject external-payment-api    8115 "latency_ms=0&error_rate=0.0"
inject discount-service        8098 "latency_ms=0&error_rate=0.0"
inject recommendation-service  8095 "latency_ms=0&error_rate=0.0"

# Restore DB latency
echo ""
echo "  → payments-db: query_delay_ms=20"
kubectl exec -n "$NAMESPACE" deploy/payments-db -- \
  psql -U postgres -d payments \
  -c "UPDATE system_config SET value='20' WHERE key='query_delay_ms';" 2>/dev/null || \
  echo "    (skipped — payments-db not reachable)"

# Restore Redis
echo ""
echo "  → redis: removing pressure keys, maxmemory=0, policy=noeviction"
kubectl exec -n "$NAMESPACE" deploy/redis -- sh -c '
for db in 0 1 2 3 4 5 6 7 8 9; do
  for pattern in "pressure:*" "fraud:rate:*"; do
    keys=$(redis-cli -n $db KEYS "$pattern" 2>/dev/null)
    if [ -n "$keys" ]; then
      echo "$keys" | xargs redis-cli -n $db DEL >/dev/null 2>&1
    fi
  done
done
' 2>/dev/null || true
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli CONFIG SET maxmemory 0 2>/dev/null || true
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli CONFIG SET maxmemory-policy noeviction 2>/dev/null || true

# Restart all injectable services to reset Prometheus histogram counters.
# Without this, stale slow-request data from the inject period skews p95
# metrics in Grafana even after latency is cleared.
echo ""
echo "  → restarting services to reset Prometheus histogram counters..."
INJECTABLE_SERVICES="checkout inventory-service orders-service search-service ranking-service external-payment-api discount-service recommendation-service billing-service payment-adapter payments-api"
for svc in $INJECTABLE_SERVICES; do
  kubectl rollout restart -n "$NAMESPACE" deployment/"$svc" 2>/dev/null && echo "    restarted $svc" || echo "    (skipped — $svc not found)"
done
echo "  → waiting for rollouts to complete..."
for svc in $INJECTABLE_SERVICES; do
  kubectl rollout status -n "$NAMESPACE" deployment/"$svc" --timeout=120s 2>/dev/null || true
done

echo ""
echo "=== All services restored ==="
echo "Metrics should return to baseline within ~60 seconds."
