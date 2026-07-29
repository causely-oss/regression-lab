#!/usr/bin/env bash
# restore_pod_errors.sh
#
# Restores checkout after inject_pod_errors.sh.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring checkout error rate ==="

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

inject checkout 8082 "error_rate=0.0&latency_ms=0"

echo ""
echo "=== Restore complete ==="
echo "Checkout error rate back to baseline (~0.5%)."
