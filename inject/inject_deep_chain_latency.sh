#!/usr/bin/env bash
# inject_deep_chain_latency.sh
#
# Scenario: Deep call chain latency — recommendation-service slowdown cascades
#           through 6-hop chain.
#
# Call chain:
#   frontend → api-gateway → catalog-service → review-service →
#   recommendation-service → analytics-service → reporting-service
#
# What happens:
#   - recommendation-service latency: ~50ms → ~600ms
#   - Each upstream service adds the delay to its own response time
#   - review-service latency: ~60ms → ~700ms (waiting on recommendation)
#   - catalog-service latency: ~80ms → ~800ms (waiting on review)
#   - api-gateway catalog latency: ~90ms → ~900ms
#   - frontend /catalog latency: ~100ms → ~1s+
#   - analytics-service and reporting-service are downstream of recommendation
#     but their latency doesn't change (they're just called by recommendation)
#
# Root cause: recommendation-service internal latency spike
# Red herrings: every service in the chain shows latency increase,
#   analytics/reporting look slow in chain but are actually healthy
#
# Usage:
#   bash inject_deep_chain_latency.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Injecting deep call chain latency (recommendation-service) ==="
echo "  Namespace : $NAMESPACE"
echo ""

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

# Inject latency into recommendation-service (the bottleneck in the chain)
inject recommendation-service 8095 "latency_ms=600&error_rate=0.0"

echo ""
echo "=== Injection complete ==="
echo ""
echo "Expected effects (allow ~60s for metrics to propagate):"
echo "  recommendation-service p95   : ~50ms → ~600ms"
echo "  review-service p95           : ~60ms → ~700ms (cascade)"
echo "  catalog-service p95          : ~80ms → ~800ms (cascade)"
echo "  api-gateway /catalog p95     : ~90ms → ~900ms (cascade)"
echo "  frontend /catalog p95        : ~100ms → ~1s+ (cascade)"
echo "  analytics-service            : healthy (called by recommendation)"
echo "  reporting-service            : healthy (called by analytics)"
echo ""
echo "Key pattern: every service in the chain shows a moderate latency"
echo "increase, but the root cause is recommendation-service adding ~550ms."
echo ""
echo "To restore: bash inject/restore_deep_chain_latency.sh"
