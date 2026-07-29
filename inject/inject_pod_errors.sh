#!/usr/bin/env bash
# inject_pod_errors.sh
#
# Scenario: Elevated errors in individual pods — stdout errors visible in logs.
#
# What happens:
#   - checkout service starts returning 500s at ~20% rate
#   - Errors are visible in pod logs (stdout)
#   - checkout success rate drops: ~99.5% → ~80%
#   - Upstream services (api-gateway, frontend) see elevated error rates
#   - Downstream services (payments-api, billing, pricing) are all healthy
#
# Root cause: checkout service application error (injected)
# Red herrings: payments-api metrics look normal, billing is fine,
#   the error originates in checkout itself not its dependencies
#
# Usage:
#   bash inject_pod_errors.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Injecting elevated pod errors (checkout) ==="
echo "  Namespace : $NAMESPACE"
echo ""

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

# Inject errors into checkout (20% error rate, no latency — pure error injection)
inject checkout 8082 "error_rate=0.20&latency_ms=0"

echo ""
echo "=== Injection complete ==="
echo ""
echo "Expected effects (allow ~60s for metrics to propagate):"
echo "  checkout error rate         : ~0.5% → ~20%"
echo "  checkout success rate       : ~99.5% → ~80%"
echo "  checkout pod logs           : INJECTED ERROR messages in stdout"
echo "  api-gateway error rate      : increases (passes through checkout errors)"
echo "  payments-api                : healthy (errors happen before payment call)"
echo "  billing-service             : healthy"
echo ""
echo "To investigate: kubectl logs -n $NAMESPACE deploy/checkout -f"
echo "To restore: bash inject/restore_pod_errors.sh"
