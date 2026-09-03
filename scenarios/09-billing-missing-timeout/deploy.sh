#!/usr/bin/env bash
# deploy.sh — build + push + roll out billing-service from the currently
# checked-out branch (bug branch or fix branch, doesn't matter which).
#
# The live deployment normally pins billing-service to an immutable tag
# (e.g. :v3) with imagePullPolicy: IfNotPresent, so a plain `docker push` +
# `rollout restart` to that same tag (or to :latest) will NOT pull your new
# build — the node already has something cached under that reference. This
# script points the deployment at a dedicated :scenario-09 tag and forces
# imagePullPolicy: Always so every run actually deploys what you just built,
# whether it's the bug branch or your fix branch.
#
# Usage:
#   git checkout scenario-09-billing-timeout-bug   # or your fix branch
#   bash scenarios/09-billing-missing-timeout/deploy.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"
REGISTRY="${REGISTRY:-causely-oss}"
SVC="billing-service"
TAG="scenario-09"

echo "=== Building $SVC from branch $(git rev-parse --abbrev-ref HEAD) ==="
docker build --no-cache -t "${REGISTRY}/${SVC}:${TAG}" "environment/services/${SVC}"
docker push "${REGISTRY}/${SVC}:${TAG}"

echo "=== Pointing $SVC at ${REGISTRY}/${SVC}:${TAG} (imagePullPolicy: Always) ==="
kubectl set image deploy/"$SVC" "$SVC"="${REGISTRY}/${SVC}:${TAG}" -n "$NAMESPACE"
kubectl patch deploy/"$SVC" -n "$NAMESPACE" --type=json \
  -p="[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"Always\"}]"

echo "=== Rolling out $SVC in namespace $NAMESPACE ==="
kubectl rollout restart deploy/"$SVC" -n "$NAMESPACE"
kubectl rollout status deploy/"$SVC" -n "$NAMESPACE" --timeout=120s

echo ""
echo "Done. Wait ~60s for metrics to stabilize before injecting/observing."
echo "To go back to the fleet baseline afterward:"
echo "  kubectl set image deploy/$SVC $SVC=${REGISTRY}/${SVC}:v3 -n $NAMESPACE   # check k8s/03-app.yaml for the current baseline tag"
