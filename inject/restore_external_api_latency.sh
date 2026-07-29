#!/usr/bin/env bash
# restore_external_api_latency.sh
#
# Restores external-payment-api after inject_external_api_latency.sh.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring external-payment-api ==="

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

inject external-payment-api 8115 "latency_ms=0&error_rate=0.0"

echo ""
echo "=== Restore complete ==="
echo "External payment API back to baseline (~150ms, ~2% error rate)."
