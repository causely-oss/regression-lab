#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# inject_cpu_throttle.sh
#
# Scenario: Node-3 CPU throttling (90% sustained) — search + ranking pods affected.
#
# What happens:
#   - search-service and ranking-service both slow down (simulating CPU throttle)
#   - Search latency spikes: p95 ~15ms → ~800ms
#   - Ranking latency spikes: p95 ~10ms → ~500ms
#   - Profile-service and user-service (downstream of ranking) appear slow from
#     the caller's perspective but are actually healthy
#   - search error rate rises slightly due to timeouts
#
# Root cause: CPU throttling on the node hosting search + ranking
# Red herrings: profile-service and user-service look slow in traces
#
# Usage:
#   bash inject_cpu_throttle.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Injecting CPU throttle simulation (search + ranking) ==="
echo "  Namespace : $NAMESPACE"
echo ""

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

# Simulate CPU-throttled services with high latency
inject search-service  8088 "latency_ms=800&error_rate=0.08"
inject ranking-service 8089 "latency_ms=500&error_rate=0.05"

echo ""
echo "=== Injection complete ==="
echo ""
echo "Expected effects (allow ~60s for metrics to propagate):"
echo "  search-service p95 latency   : ~15ms → ~800ms"
echo "  ranking-service p95 latency  : ~10ms → ~500ms"
echo "  search error rate            : ~0% → ~8%"
echo "  ranking error rate           : ~0% → ~5%"
echo "  frontend /search latency     : ~30ms → ~1.5s+"
echo ""
echo "To restore: bash inject/restore_cpu_throttle.sh"
