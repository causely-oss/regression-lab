#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# inject_inventory_stall.sh
#
# Scenario: Inventory consumer stalls — Kafka lag grows, orders-service times out.
#
# What happens:
#   - inventory-service Kafka consumer is paused (stops consuming regression-lab-inventory-updates)
#   - Kafka consumer lag grows rapidly (→ ~80k with sustained load)
#   - orders-service calls to inventory-service start timing out
#   - Shipping pipeline stalls (no regression-lab-shipping-events produced)
#
# Root cause: inventory-service consumer pause
# Red herrings: Kafka itself is healthy, orders-service metrics show latency first
#
# Usage:
#   bash inject_inventory_stall.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Injecting inventory consumer stall ==="
echo "  Namespace : $NAMESPACE"
echo ""

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

# Pause the Kafka consumer in inventory-service
inject inventory-service 8086 "pause_consumer=true"

# Add latency to inventory HTTP endpoints (simulates backpressure)
inject inventory-service 8086 "latency_ms=4000&error_rate=0.25"

echo ""
echo "=== Injection complete ==="
echo ""
echo "Expected effects (allow ~60s for metrics to propagate):"
echo "  inventory-service Kafka consumer : running → paused"
echo "  Kafka consumer lag (inventory)   : 0 → growing (80k+ over time)"
echo "  inventory-service HTTP latency   : ~10ms → ~4s"
echo "  inventory-service error rate     : ~0% → ~25%"
echo "  orders-service latency           : ~50ms → ~5s+ (waiting on inventory)"
echo "  shipping pipeline                : active → stalled"
echo ""
echo "To restore: bash inject/restore_inventory_stall.sh"
