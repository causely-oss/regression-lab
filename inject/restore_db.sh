#!/usr/bin/env bash
# restore_db.sh
#
# Restores payments-db to normal baseline query latency (~20ms).
# Run this after inject_db_latency.sh to end the simulated incident.
#
# Usage:
#   bash restore_db.sh

set -euo pipefail

NAMESPACE="${PAYMENTS_DB_NAMESPACE:-scenario-01}"

echo "=== Restoring DB to normal latency ==="
echo "  Namespace : $NAMESPACE"
echo "  Delay     : 20ms (baseline)"
echo ""

if ! kubectl get deploy payments-db -n "$NAMESPACE" &>/dev/null; then
  echo "ERROR: Deployment 'payments-db' not found in namespace '$NAMESPACE'."
  exit 1
fi

kubectl exec -n "$NAMESPACE" deploy/payments-db -- \
  psql -U postgres -d payments \
  -c "UPDATE system_config SET value='20' WHERE key='query_delay_ms';" \
  -c "SELECT key, value FROM system_config WHERE key='query_delay_ms';"

echo ""
echo "=== Restore complete ==="
echo "Metrics should return to baseline within ~60 seconds."
echo ""
echo "  payments-db query latency : ~20ms"
echo "  payments-api error rate   : ~0%"
echo "  checkout success rate     : ~99.5%"
