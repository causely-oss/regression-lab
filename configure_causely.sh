#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# configure_causely.sh
#
# Applies Causely labels to all services in the scenario-01 namespace:
#   - latency-threshold=450.0 (450ms)
#   - error-rate-activation-delay=1 (1 minute)
#   - latency-activation-delay=1 (1 minute)
#
# Usage:
#   bash configure_causely.sh
#   bash configure_causely.sh <namespace>   # override namespace

set -euo pipefail

NAMESPACE="${1:-scenario-01}"

echo "=== Configuring Causely labels for all services in namespace: ${NAMESPACE} ==="
echo ""

# Get all services in the namespace (exclude kubernetes system services)
SERVICES=$(kubectl get svc -n "${NAMESPACE}" -o jsonpath='{.items[*].metadata.name}')

if [ -z "${SERVICES}" ]; then
  echo "ERROR: No services found in namespace '${NAMESPACE}'"
  exit 1
fi

COUNT=0
for SVC in ${SERVICES}; do
  echo "Labeling ${SVC}..."

  kubectl label svc -n "${NAMESPACE}" "${SVC}" \
    "causely.ai/latency-threshold=450.0" \
    "causely.ai/error-rate-activation-delay=1" \
    "causely.ai/latency-activation-delay=1" \
    --overwrite

  COUNT=$((COUNT + 1))
done

echo ""
echo "=== Done: ${COUNT} services labeled ==="

# Apply service-specific overrides
echo ""
echo "=== Applying service-specific label overrides ==="

echo "Labeling external-payment-api with error-rate-threshold=0.04..."
kubectl label svc -n "${NAMESPACE}" external-payment-api \
  "causely.ai/error-rate-threshold=0.04" \
  --overwrite

echo ""
echo "Labels applied to all services:"
echo "  causely.ai/latency-threshold=450.0          (450ms request duration threshold)"
echo "  causely.ai/error-rate-activation-delay=1    (1 minute delayed activation)"
echo "  causely.ai/latency-activation-delay=1       (1 minute delayed activation)"
echo ""
echo "Service-specific overrides:"
echo "  external-payment-api: causely.ai/error-rate-threshold=0.04  (4% — accounts for baseline 2% error rate)"
echo ""
echo "Verify with:"
echo "  kubectl get svc -n ${NAMESPACE} --show-labels"
