#!/usr/bin/env bash
# restore_orders_retry_loop.sh
#
# Restores orders-service and inventory-service after inject_orders_retry_loop.sh.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring orders + inventory services ==="

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

inject inventory-service 8086 "error_rate=0.0&latency_ms=0&pause_consumer=false"
inject orders-service    8085 "latency_ms=0&error_rate=0.0"

echo ""
echo "=== Restore complete ==="
echo "Orders and inventory back to baseline within ~60s."
