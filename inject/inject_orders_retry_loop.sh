#!/usr/bin/env bash
# inject_orders_retry_loop.sh
#
# Scenario: Orders retry loop overloads inventory — inventory logs errors,
#           metrics show orders latency first.
#
# What happens:
#   - orders-service gets injected latency (simulates retry backoff eating time)
#   - inventory-service gets high error rate (simulates overload from retries)
#   - orders-service latency spikes because each call to inventory errors/retries
#   - inventory logs fill with error messages
#   - The misleading signal: orders-service latency appears first in metrics,
#     but inventory-service is the one actually erroring
#
# Root cause: inventory-service errors causing orders-service retry amplification
# Red herrings: orders-service latency spike appears first, Kafka looks healthy
#
# Usage:
#   bash inject_orders_retry_loop.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Injecting orders retry loop → inventory overload ==="
echo "  Namespace : $NAMESPACE"
echo ""

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

# Inventory errors (the actual root cause)
inject inventory-service 8086 "error_rate=0.40&latency_ms=800"

# Orders latency (the visible symptom — retries pile up)
inject orders-service 8085 "latency_ms=2000&error_rate=0.10"

echo ""
echo "=== Injection complete ==="
echo ""
echo "Expected effects (allow ~60s for metrics to propagate):"
echo "  inventory-service error rate : ~0% → ~40%"
echo "  inventory-service latency    : ~10ms → ~800ms"
echo "  orders-service latency       : ~50ms → ~2s (retry amplification)"
echo "  orders-service error rate    : ~0% → ~10%"
echo "  inventory pod logs           : INJECTED ERROR messages"
echo "  orders pod logs              : inventory call failures"
echo "  Kafka                        : healthy (misleading)"
echo ""
echo "Key signal: orders latency rises FIRST in metrics, but root cause"
echo "is inventory errors causing the retry amplification."
echo ""
echo "To restore: bash inject/restore_orders_retry_loop.sh"
