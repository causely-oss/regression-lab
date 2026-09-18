#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# restore_deep_chain_latency.sh
#
# Restores recommendation-service after inject_deep_chain_latency.sh.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring recommendation-service ==="

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

inject recommendation-service 8095 "latency_ms=0&error_rate=0.0"

echo ""
echo "=== Restore complete ==="
echo "Recommendation-service latency back to baseline (~50ms)."
echo "Upstream cascade will clear within ~60s."
