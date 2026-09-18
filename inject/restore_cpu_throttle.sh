#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# restore_cpu_throttle.sh
#
# Restores search-service and ranking-service after inject_cpu_throttle.sh.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring search + ranking services ==="

inject() {
  local svc="$1" port="$2" params="$3"
  echo "  → $svc: $params"
  kubectl exec -n "$NAMESPACE" deploy/"$svc" -- \
    wget -q -O- --post-data '' "http://localhost:${port}/admin/config?${params}" \
    2>&1
}

inject search-service  8088 "latency_ms=0&error_rate=0.0"
inject ranking-service 8089 "latency_ms=0&error_rate=0.0"

echo ""
echo "=== Restore complete ==="
echo "Search and ranking latency back to baseline within ~60s."
