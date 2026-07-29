#!/usr/bin/env bash
# inject_redis_pressure.sh
#
# Scenario: Redis memory pressure → eviction spike, cache hit rate drops,
#           cache-dependent services see latency increase.
#
# What happens:
#   - Redis maxmemory is reduced to 2MB (from default), forcing evictions
#   - Cache hit rates plummet across all Redis-dependent services
#   - Services that cache-miss fall back to expensive computation paths
#   - Affected: ranking (search), recommendation (catalog), loyalty (checkout)
#   - Each cache miss adds ~100-200ms recomputation latency
#
# Root cause: Redis memory pressure causing mass evictions
# Red herrings: many services show latency increase simultaneously
#
# Usage:
#   bash inject_redis_pressure.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Injecting Redis memory pressure ==="
echo "  Namespace : $NAMESPACE"
echo ""

# Get current maxmemory for reference
echo "  Current Redis config:"
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli CONFIG GET maxmemory 2>/dev/null || true
echo ""

# Set maxmemory to 2MB with allkeys-lru eviction
echo "  → Setting maxmemory=2mb, maxmemory-policy=allkeys-lru"
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli CONFIG SET maxmemory 2mb 2>/dev/null
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli CONFIG SET maxmemory-policy allkeys-lru 2>/dev/null

# Fill Redis with junk data using a single pipeline command to trigger evictions
echo "  → Filling Redis with pressure data across all DBs..."
kubectl exec -n "$NAMESPACE" deploy/redis -- sh -c '
for db in 0 1 2 3 4 5 6 7 8 9; do
  for i in $(seq 1 50); do
    redis-cli -n $db SET "pressure:${db}:${i}" "$(dd if=/dev/urandom bs=4096 count=1 2>/dev/null | base64)" EX 600 >/dev/null 2>&1
  done
done
echo "Done filling 500 keys across 10 DBs"
' 2>/dev/null

echo ""
echo "  Redis memory info after injection:"
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli INFO memory 2>/dev/null | grep -E "used_memory_human|maxmemory_human|evicted_keys" || true

echo ""
echo "=== Injection complete ==="
echo ""
echo "Expected effects (allow ~60-120s for metrics to propagate):"
echo "  Redis evicted_keys           : 0 → rapidly growing"
echo "  ranking-service latency      : ~15ms → ~150ms+ (cache miss recomputation)"
echo "  recommendation-service       : ~50ms → ~170ms+ (ML scoring on miss)"
echo "  loyalty-service latency      : ~5ms → ~100ms+ (aggregation on miss)"
echo "  search p95 (load gen)        : increase (via ranking miss)"
echo "  catalog p95 (load gen)       : increase (via recommendation miss)"
echo "  checkout p95 (load gen)      : moderate increase (via loyalty miss)"
echo ""
echo "To restore: bash inject/restore_redis_pressure.sh"
