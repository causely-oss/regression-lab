#!/usr/bin/env bash
# inject_db_latency.sh
#
# Simulates a payments-db latency spike:
#   - Query delay: 20ms → 2,800ms
#   - This causes DB CPU to climb as connections pile up on slow queries
#   - payments-api error rate → ~18%  (3s timeout in checkout hits at ~2.8s DB latency)
#   - checkout error rate    → ~14%
#
# Requires: kubectl, payments-db pod running in the scenario-01 namespace
#
# Usage:
#   bash inject_db_latency.sh [--delay-ms N]
#
# Options:
#   --delay-ms N   Target query delay in milliseconds (default: 2800)

set -euo pipefail

DELAY_MS="${1:-2800}"
NAMESPACE="${PAYMENTS_DB_NAMESPACE:-scenario-01}"

echo "=== Injecting DB latency spike ==="
echo "  Namespace : $NAMESPACE"
echo "  Delay     : ${DELAY_MS}ms  (was ~20ms)"
echo ""

# Check the deployment is available
if ! kubectl get deploy payments-db -n "$NAMESPACE" &>/dev/null; then
  echo "ERROR: Deployment 'payments-db' not found in namespace '$NAMESPACE'."
  echo "       Make sure the stack is up: kubectl apply -f k8s/"
  exit 1
fi

# Apply the delay via the system_config table
kubectl exec -n "$NAMESPACE" deploy/payments-db -- \
  psql -U postgres -d payments \
  -c "UPDATE system_config SET value='${DELAY_MS}' WHERE key='query_delay_ms';" \
  -c "SELECT key, value FROM system_config WHERE key='query_delay_ms';"

echo ""
echo "=== Injection complete ==="
echo ""
echo "Expected effects (allow ~60s for metrics to propagate):"
echo "  payments-db query latency : ~20ms  → ~${DELAY_MS}ms"
echo "  DB CPU usage              : ~35%   → ~92%"
echo "  payments-api error rate   : ~0%    → ~18%"
echo "  checkout error rate       : ~0.5%  → ~14%"
echo ""
echo "Watch in Grafana: http://\$(minikube ip):30300  (admin / admin)"
echo "Or follow logs  : kubectl logs -n $NAMESPACE deploy/payments-api -f"
echo ""
echo "To restore normal behavior run: bash restore_db.sh"
