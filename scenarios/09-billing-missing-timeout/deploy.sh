#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# deploy.sh — build + push + roll out billing-service from whatever is
# currently checked out (bug tree or fix tree, doesn't matter which).
#
# The live deployment normally pins billing-service to an immutable tag
# (e.g. :v3), so a plain `docker push` + `rollout restart` to that same tag
# (or to :latest) risks the node not pulling your new build. This script
# tags the image with the current commit's short SHA (an ordinary-looking
# build identifier, not anything scenario-named — an agent under test
# should not be able to read "scenario-09" off `kubectl describe pod` or
# `docker images`) and forces imagePullPolicy: Always so every run actually
# deploys what you just built.
#
# Usage: this branch carries the scenarios/ tooling but not necessarily the
# source you want built (the neutral bug/fix branches fork from main and
# don't have scenarios/ at all). Overlay the source you want with a
# path-scoped checkout instead of switching branches wholesale — see
# "Deploy / verify" in README.md for the full pattern:
#   git checkout scenario-09-billing-timeout-bug
#   git checkout <bug-or-fix-branch> -- environment/services/billing-service
#   bash scenarios/09-billing-missing-timeout/deploy.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-scenario-01}"
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/causely-oss/regression-lab}"
SVC="billing-service"
TAG="build-$(git rev-parse --short=12 HEAD)"

echo "=== Building $SVC from commit $(git rev-parse --short HEAD) ==="
docker build --no-cache -t "${IMAGE_REPO}/${SVC}:${TAG}" "environment/services/${SVC}"
docker push "${IMAGE_REPO}/${SVC}:${TAG}"

echo "=== Pointing $SVC at ${IMAGE_REPO}/${SVC}:${TAG} (imagePullPolicy: Always) ==="
kubectl set image deploy/"$SVC" "$SVC"="${IMAGE_REPO}/${SVC}:${TAG}" -n "$NAMESPACE"
kubectl patch deploy/"$SVC" -n "$NAMESPACE" --type=json \
  -p="[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"Always\"}]"

echo "=== Rolling out $SVC in namespace $NAMESPACE ==="
kubectl rollout restart deploy/"$SVC" -n "$NAMESPACE"
kubectl rollout status deploy/"$SVC" -n "$NAMESPACE" --timeout=120s

echo ""
echo "Done. Wait ~60s for metrics to stabilize before injecting/observing."
echo "To go back to the fleet baseline afterward:"
echo "  kubectl set image deploy/$SVC $SVC=${IMAGE_REPO}/${SVC}:v3 -n $NAMESPACE   # check k8s/03-app.yaml for the current baseline tag"
