#!/usr/bin/env bash
# inject_external_api_latency.sh
#
# Scenario: External payment API latency → 3s, 6% failure rate, retry amplification.
#
# What happens:
#   - external-payment-api latency jumps from ~150ms to ~3s
#   - external-payment-api failure rate: ~2% → 6%
#   - payment-adapter retries amplify the latency (each retry = +3s)
#   - billing-service latency spikes (waits on payment-adapter)
#   - checkout overall latency increases but doesn't necessarily fail
#     (billing is non-blocking in checkout, but latency is visible)
#
# Root cause: external-payment-api degradation
# Red herrings: billing-service looks slow, payment-adapter shows errors,
#   checkout latency rises but payments-api/payments-db are healthy
#
# Usage:
#   bash inject_external_api_latency.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Injecting external payment API degradation ==="
echo "  Namespace : $NAMESPACE"
echo ""

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

# Slow down external payment API with elevated errors
inject external-payment-api 8115 "latency_ms=3000&error_rate=0.06"

echo ""
echo "=== Injection complete ==="
echo ""
echo "Expected effects (allow ~60s for metrics to propagate):"
echo "  external-payment-api latency  : ~150ms → ~3s"
echo "  external-payment-api errors   : ~2% → ~6%"
echo "  payment-adapter latency       : ~200ms → ~3s+ (retries amplify)"
echo "  billing-service latency       : ~300ms → ~6s+ (waits on adapter)"
echo "  checkout latency              : moderate increase (billing path)"
echo "  payments-api / payments-db    : healthy (not affected)"
echo ""
echo "To restore: bash inject/restore_external_api_latency.sh"
