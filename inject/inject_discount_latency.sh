#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# inject_discount_latency.sh
#
# Scenario: Discount-service latency spike (no traces) — downstream slowdown.
#
# What happens:
#   - discount-service latency: ~5ms → ~800ms
#   - pricing-service latency increases (calls discount-service)
#   - checkout latency increases (calls pricing-service in step 2)
#   - The tricky part: discount-service has no tracing, so distributed traces
#     show a gap between pricing-service call and response
#   - loyalty-service (downstream of discount) appears slow from pricing's
#     perspective but is actually healthy
#
# Root cause: discount-service internal latency spike
# Red herrings: pricing-service looks slow, loyalty-service appears affected,
#   checkout latency rises but payments path is clean
#
# Usage:
#   bash inject_discount_latency.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Injecting discount-service latency spike ==="
echo "  Namespace : $NAMESPACE"
echo ""

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

# Inject latency into discount-service (no error rate — pure latency)
inject discount-service 8098 "latency_ms=800&error_rate=0.0"

echo ""
echo "=== Injection complete ==="
echo ""
echo "Expected effects (allow ~60s for metrics to propagate):"
echo "  discount-service latency     : ~5ms → ~800ms"
echo "  pricing-service latency      : ~15ms → ~850ms+ (waits on discount)"
echo "  checkout latency             : moderate increase (~800ms added to step 2)"
echo "  checkout error rate          : unchanged (no timeouts, just slow)"
echo "  loyalty-service              : healthy (but appears slow from pricing)"
echo "  payments path                : unaffected"
echo ""
echo "To restore: bash inject/restore_discount_latency.sh"
