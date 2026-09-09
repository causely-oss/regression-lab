#!/usr/bin/env bash
# deploy.sh — build + push + roll out pricing-service from whatever is
# currently checked out (bug tree or fix tree, doesn't matter which).
#
# The live deployment normally pins pricing-service to an immutable tag
# (e.g. :v2), so a plain `docker push` + `rollout restart` to that same tag
# (or to :latest) risks the node not pulling your new build. This script
# tags the image with the current commit's short SHA (an ordinary-looking
# build identifier, not anything scenario-named — an agent under test
# should not be able to read "scenario-11" off `kubectl describe pod` or
# `docker images`) and forces imagePullPolicy: Always so every run actually
# deploys what you just built.
#
# Usage: this branch carries the scenarios/ tooling but not necessarily the
# source you want built (the neutral bug/fix branches fork from main and
# don't have scenarios/ at all). Overlay the source you want with a
# path-scoped checkout instead of switching branches wholesale — see
# "Deploy / verify" in README.md for the full pattern:
#   git checkout scenario-11-pricing-n-plus-one-bug
#   git checkout <bug-or-fix-branch> -- environment/services/pricing-service
#   bash scenarios/11-pricing-n-plus-one/deploy.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"
REGISTRY="${REGISTRY:-causely-oss}"
SVC="pricing-service"
TAG="build-$(git rev-parse --short=12 HEAD)"

echo "=== Building $SVC from commit $(git rev-parse --short HEAD) ==="
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
echo "Done. Wait ~60s for metrics to stabilize before observing."
echo "To go back to the fleet baseline afterward:"
echo "  kubectl set image deploy/$SVC $SVC=${REGISTRY}/${SVC}:v2 -n $NAMESPACE   # check k8s/03-app.yaml for the current baseline tag"
