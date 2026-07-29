#!/usr/bin/env bash
# restore_inventory_stall.sh
#
# Restores inventory-service to normal after inject_inventory_stall.sh.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring inventory-service ==="

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

inject inventory-service 8086 "pause_consumer=false&latency_ms=0&error_rate=0.0"

echo ""
echo "=== Restore complete ==="
echo "Kafka consumer will resume. Lag will drain over ~60s."
echo "inventory-service HTTP latency and error rate back to baseline."
