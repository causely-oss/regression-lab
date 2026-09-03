#!/usr/bin/env bash
# deploy.sh — build + push + roll out pricing-service from the currently
# checked-out branch (bug branch or fix branch, doesn't matter which).
#
# Usage:
#   git checkout scenario-11-pricing-n-plus-one-bug   # or your fix branch
#   bash scenarios/11-pricing-n-plus-one/deploy.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"
REGISTRY="${REGISTRY:-causely-oss}"
SVC="pricing-service"

echo "=== Building $SVC from branch $(git rev-parse --abbrev-ref HEAD) ==="
docker build --no-cache -t "${REGISTRY}/${SVC}:latest" "environment/services/${SVC}"
docker push "${REGISTRY}/${SVC}:latest"

echo "=== Rolling out $SVC in namespace $NAMESPACE ==="
kubectl rollout restart deploy/"$SVC" -n "$NAMESPACE"
kubectl rollout status deploy/"$SVC" -n "$NAMESPACE" --timeout=120s

echo ""
echo "Done. Wait ~60s for metrics to stabilize before observing."
