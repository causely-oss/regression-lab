#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# production-scale.sh
#
# Sets 3 replicas and HPA minReplicas=3 for the 12 high-traffic services.
# Run this after `kubectl apply -f k8s/` when deploying to a real cluster (EKS, GKE, etc.).
#
# Usage:
#   bash k8s/production-scale.sh
#   bash k8s/production-scale.sh <namespace>   # override namespace

set -euo pipefail

NAMESPACE="${1:-scenario-01}"

SCALED_SERVICES=(
  frontend
  api-gateway
  checkout
  payments-api
  search-service
  orders-service
  catalog-service
  user-service
  auth-service
  cart-service
  recommendation-service
  ranking-service
)

echo "=== Scaling deployments to 3 replicas in namespace: ${NAMESPACE} ==="
for svc in "${SCALED_SERVICES[@]}"; do
  echo "  kubectl scale deployment/${svc} --replicas=3"
  kubectl scale deployment/"${svc}" -n "${NAMESPACE}" --replicas=3
done

echo ""
echo "=== Setting HPA minReplicas=3 ==="
for svc in "${SCALED_SERVICES[@]}"; do
  echo "  hpa/${svc} minReplicas → 3"
  kubectl patch hpa "${svc}" -n "${NAMESPACE}" \
    --type='merge' \
    -p '{"spec":{"minReplicas":3}}'
done

echo ""
echo "=== Done. Current replica counts ==="
kubectl get deployments -n "${NAMESPACE}" \
  $(printf -- "-l app=%s " "${SCALED_SERVICES[@]}") \
  --no-headers \
  -o custom-columns="NAME:.metadata.name,DESIRED:.spec.replicas,READY:.status.readyReplicas" \
  2>/dev/null || kubectl get deployments -n "${NAMESPACE}" --no-headers \
  -o custom-columns="NAME:.metadata.name,DESIRED:.spec.replicas,READY:.status.readyReplicas" \
  | grep -E "$(IFS='|'; echo "${SCALED_SERVICES[*]}")"
