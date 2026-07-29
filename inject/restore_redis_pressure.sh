#!/usr/bin/env bash
# restore_redis_pressure.sh
#
# Restores Redis to normal after inject_redis_pressure.sh.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring Redis to normal ==="

# Remove pressure keys from all DBs
echo "  → Removing pressure keys from all DBs..."
kubectl exec -n "$NAMESPACE" deploy/redis -- sh -c '
for db in 0 1 2 3 4 5 6 7 8 9; do
  keys=$(redis-cli -n $db KEYS "pressure:*" 2>/dev/null)
  if [ -n "$keys" ]; then
    echo "$keys" | xargs redis-cli -n $db DEL >/dev/null 2>&1
  fi
done
echo "Done cleaning pressure keys"
' 2>/dev/null

# Restore maxmemory to default (0 = unlimited)
echo "  → Restoring maxmemory=0 (unlimited), maxmemory-policy=noeviction"
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli CONFIG SET maxmemory 0 2>/dev/null
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli CONFIG SET maxmemory-policy noeviction 2>/dev/null

echo ""
echo "  Redis memory info after restore:"
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli INFO memory 2>/dev/null | grep -E "used_memory_human|maxmemory_human|evicted_keys" || true

echo ""
echo "=== Restore complete ==="
echo "Redis back to unlimited memory. Caches will repopulate over ~60s."
