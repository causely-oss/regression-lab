# Scenario 10: recommendation-service memory limit set too low

**Fix type:** Kubernetes manifest (no app code touched)
**Descriptive branch (operator reference only, do not deploy from this):** `scenario-10-recommendation-oom-bug`
**Branch to deploy for an agent test:** `chore/service-resource-rightsizing` (forks from `main`, no `scenarios/` directory or scenario-named history in its tree — see [Blind-test hygiene](../README.md#blind-test-hygiene--every-scenario-has-two-branches))
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

Operator steps, from this repo checkout (has `scenarios/`, fine for you):

```bash
git checkout chore/service-resource-rightsizing
kubectl apply -f k8s/03-app.yaml -n scenario-01
kubectl rollout status deploy/recommendation-service -n scenario-01 --timeout=120s
```

Then hand the agent under test a **separate** clone/worktree of
`chore/service-resource-rightsizing` only (not this checkout — see
[Blind-test hygiene](../README.md#blind-test-hygiene--every-scenario-has-two-branches)).
It should open its fix PR against `main`.

To verify a fix and reset:

```bash
git checkout <agent's fix branch or commit>
kubectl apply -f k8s/03-app.yaml -n scenario-01
```

No image rebuild is needed for either direction — this scenario only ever
touches the manifest.
