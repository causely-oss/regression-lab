#!/usr/bin/env bash
# restore_discount_latency.sh
#
# Restores discount-service after inject_discount_latency.sh.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring discount-service ==="

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

inject discount-service 8098 "latency_ms=0&error_rate=0.0"

echo ""
echo "=== Restore complete ==="
echo "Discount-service latency back to baseline (~5ms)."
