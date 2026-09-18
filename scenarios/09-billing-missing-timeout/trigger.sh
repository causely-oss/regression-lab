#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# trigger.sh — simulate an ordinary transient slowdown in payment-adapter.
#
# This is NOT the bug. It represents normal dependency variance that a
# correctly-configured client should fail fast against. The regression on
# scenario-09-billing-timeout-bug is what turns this into a sustained
# incident instead of a shrugged-off blip.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Injecting transient payment-adapter latency ==="
kubectl exec -n "$NAMESPACE" deploy/payment-adapter -- \
  wget -q -O- --post-data '' "http://localhost:8092/admin/config?latency_ms=8000&error_rate=0.0"

echo ""
echo "Expected effects (allow ~60s for metrics to propagate):"
echo "  payment-adapter latency   : ~200ms -> ~8s"
echo "  billing-service latency   : should be bounded by its own client timeout"
echo "                              (regressed build: it isn't, and keeps climbing)"
echo "  checkout                  : billing leg times out at ~5s regardless"
echo ""
echo "To restore: bash scenarios/09-billing-missing-timeout/restore.sh"
