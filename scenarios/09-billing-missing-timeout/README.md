# Scenario 9: billing-service loses its downstream request timeout

**Fix type:** application code (Go)
**Descriptive branch (carries this `scenarios/` tooling — check it out to run the scripts below, but never hand this checkout to an agent under test):** `scenario-09-billing-timeout-bug`
**Branch to deploy for an agent test:** `chore/billing-http-client-pooling` (forks from `main`, no `scenarios/` directory or scenario-named history in its tree — see [Blind-test hygiene](../README.md#blind-test-hygiene--every-scenario-has-two-branches))
**Affected service:** `billing-service` (port 8091)

## Normal state

`billing-service.chargeHandler` calls `fraud-detection`, `tax-service`, and
`payment-adapter` synchronously on the request path (notification-service is
fire-and-forget). All three calls go through the package-level `otelHTTPClient`,
which is constructed with a 5s `Timeout`. `checkout` itself also bounds its
call into `billing-service` at 5s via its own client. Under normal load,
`payment-adapter` responds in well under a second, so the timeout is never the
limiting factor.

## The regression

On `scenario-09-billing-timeout-bug`, `billing-service`'s `otelHTTPClient` is
reconstructed without the `Timeout` field (see the commit touching
`environment/services/billing-service/main.go` — framed as a client-setup
cleanup, not a timeout removal, so it doesn't read as an obviously-reverted
diff). The client still has the OTel transport wrapper; it just no longer
bounds request duration. A second, unrelated commit on the same branch tweaks
an unrelated log line elsewhere in the file, so the branch isn't a single
isolated diff.

## Trigger (simulates real-world variance)

The regression alone doesn't cause a symptom under steady load — you need a
downstream blip for the missing timeout to matter, exactly like a real
incident. Use `payment-adapter`'s existing fault-injection admin endpoint
(the same mechanism `inject/*.sh` uses elsewhere in this repo) to add a
realistic slow patch:

```bash
bash scenarios/09-billing-missing-timeout/trigger.sh    # inject
bash scenarios/09-billing-missing-timeout/restore.sh     # remove the trigger
```

This is **not** the bug — it represents an ordinary transient dependency
slowdown that a correctly-configured client would fail fast against and that
`checkout`'s own 5s ceiling would absorb gracefully. The regression is what
turns it into a sustained incident:

- `billing-service` now waits out the full injected latency on every request
  instead of failing at 5s.
- `checkout`'s own client still times out at 5s waiting on `billing-service`,
  so checkout requests error out on the billing leg — but `billing-service`
  keeps the goroutine/connection alive for the full downstream duration
  regardless, because nothing ever cancels it early.
- Under sustained load this accumulates: `billing-service` runs a growing
  number of in-flight requests whose caller has already given up, degrading
  its own latency and throughput even for unrelated requests.

## Expected Causely signal

- Elevated latency / error rate symptom on `checkout` (billing leg).
- `billing-service` shows rising latency and possibly rising resource usage
  correlated with the `payment-adapter` edge, not with `payments-api`/`payments-db`
  (those stay healthy — this is a different call path than Scenario 3 in
  `inject/`).
- Topology should show `payment-adapter` as the introduced slowdown but
  `billing-service` as the entity whose behavior changed structurally (it no
  longer sheds load), which is the tell that this isn't just "a dependency is
  slow" but "the caller regressed."

## Expected fix

`billing-service`'s `otelHTTPClient` needs a bounded request timeout again
(a few seconds — consistent with the rest of the fleet's client timeouts).
The fix is a `billing-service` code change, not a `payment-adapter` change and
not a config toggle — `payment-adapter`'s injected latency is the environmental
trigger, not the root cause.

## Deploy / verify

Operator steps, from this repo checkout (has `scenarios/`, fine for you).

Note: `chore/billing-http-client-pooling` carries the regression but, by
design, has no `scenarios/` directory — so a plain `git checkout
chore/billing-http-client-pooling` would delete `deploy.sh`/`trigger.sh` from
your working tree along with it. Stay on the descriptive branch for the
scripts, and pull in just the neutral branch's `billing-service` source (the
one that actually matters for what gets built) with a path-scoped checkout
instead of switching branches wholesale:

```bash
git checkout scenario-09-billing-timeout-bug
git checkout chore/billing-http-client-pooling -- environment/services/billing-service
bash scenarios/09-billing-missing-timeout/deploy.sh   # builds+pushes billing-service, rolls it out
bash scenarios/09-billing-missing-timeout/trigger.sh
```

Then hand the agent under test a **separate** clone/worktree of
`chore/billing-http-client-pooling` only (not this checkout — see
[Blind-test hygiene](../README.md#blind-test-hygiene--every-scenario-has-two-branches)).
It should open its fix PR against `main`.

To verify a fix and reset (the agent's fix branch forks from `main`, so it
won't have `scenarios/` either — same path-scoped-checkout pattern applies):

```bash
git checkout scenario-09-billing-timeout-bug
git checkout <agent's fix branch or commit> -- environment/services/billing-service
bash scenarios/09-billing-missing-timeout/deploy.sh
bash scenarios/09-billing-missing-timeout/restore.sh
```
