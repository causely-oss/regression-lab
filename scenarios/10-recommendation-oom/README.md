# Scenario 10: recommendation-service memory limit set too low

**Fix type:** Kubernetes manifest (no app code touched)
**Branch with the regression:** `scenario-10-recommendation-oom-bug`
**Affected service:** `recommendation-service` (port 8095)

## Normal state

Every service in `k8s/03-app.yaml` runs with `requests: {cpu: 100m, memory:
128Mi}` / `limits: {cpu: 500m, memory: 256Mi}`, `recommendation-service`
included. `recommendation-service` sits on the search, deep-chain, and
streaming flows (consumes `regression-lab-recommendations` from Kafka, talks to
`analytics-service`, `delivery-service`, `user-service`, `cache-service`, and
Redis), so it holds more in-flight state than a typical leaf service.

## The regression

On `scenario-10-recommendation-oom-bug`, `recommendation-service`'s resources
block in `k8s/03-app.yaml` is dropped to `requests: {cpu: 100m, memory: 32Mi}`
/ `limits: {cpu: 500m, memory: 64Mi}` — framed as a commit "right-sizing"
resource requests across a few low-traffic-looking services, so it isn't a
single obviously-isolated diff (it touches more than one service's numbers;
only `recommendation-service`'s new limit is actually too low for its real
working set). A second, unrelated commit on the branch adds a harmless
annotation to a different deployment.

Under sustained load, `recommendation-service`'s working set exceeds 64Mi,
the kernel OOM-kills the container, and the pod cycles into
`CrashLoopBackOff`.

## Expected Causely signal

- OOMKilled / pod restart symptom on `recommendation-service` (this is exactly
  what `causely-k8s-investigation` is built to catch).
- Downstream impact on the search flow (`ranking-service` → `profile-service`)
  and the streaming flow (`processing-service` → `recommendation-service` →
  `delivery-service`) as calls to `recommendation-service` fail during the
  crash/restart cycle.
- Consumer lag on `regression-lab-recommendations` growing while the pod is down.

## Expected fix

Restore `recommendation-service`'s memory `limits`/`requests` in
`k8s/03-app.yaml` to a value that actually fits its working set (256Mi/128Mi,
matching the rest of the fleet, is the known-good baseline — but the point of
the fix is "big enough," not "byte-identical to before"). This is a pure
manifest change; no Go code should need to change for this scenario.

## Deploy / verify

```bash
git checkout scenario-10-recommendation-oom-bug
kubectl apply -f k8s/03-app.yaml -n scenario-01
kubectl rollout status deploy/recommendation-service -n scenario-01 --timeout=120s
# ... let load run for a few minutes, observe via Causely MCP tools ...
# agent fixes k8s/03-app.yaml, opens a PR onto main
git checkout <fix-branch>
kubectl apply -f k8s/03-app.yaml -n scenario-01
```

No image rebuild is needed for either direction — this scenario only ever
touches the manifest.
