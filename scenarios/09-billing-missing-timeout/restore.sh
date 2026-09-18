#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# restore.sh — remove the payment-adapter latency trigger.

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"

echo "=== Restoring payment-adapter ==="
kubectl exec -n "$NAMESPACE" deploy/payment-adapter -- \
  wget -q -O- --post-data '' "http://localhost:8092/admin/config?latency_ms=0&error_rate=0.0"

echo "Restore complete."
