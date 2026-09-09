# Scenario 11: pricing-service calls discount-service once per cart item

**Fix type:** application code (Go)
**Descriptive branch (operator reference only, do not deploy from this):** `scenario-11-pricing-n-plus-one-bug`
**Branch to deploy for an agent test:** `feat/per-sku-discount-pricing` (forks from `main`, no `scenarios/` directory or scenario-named history in its tree — see [Blind-test hygiene](../README.md#blind-test-hygiene--every-scenario-has-two-branches))
**Affected services:** `pricing-service` (port 8097) calling `discount-service` (port 8098)

## Normal state

`pricing-service.calculateHandler` splits the `items` query param into a list,
sums a per-item base price into a single `baseTotal`, and makes **one**
aggregate call to `discount-service` (`/discount/apply?...&amount=<baseTotal>`)
to get the final discounted total. Call volume on `discount-service` tracks
request volume on `pricing-service` 1:1, regardless of cart size.

## The regression

On `scenario-11-pricing-n-plus-one-bug`, the discount call is moved inside the
per-item loop: `pricing-service` now calls `discount-service` once per item in
the cart (accumulating a `finalTotal` across the per-item discounted amounts)
instead of once per request with the full total. The loop and variable names
are restructured as part of this change (not just cut-and-pasted), so it isn't
recoverable with a mechanical `git revert` of a single hunk — the fix has to
rebuild the aggregation logic against the current code, not undo a diff. A
second, unrelated commit on the branch adjusts an unrelated Prometheus metric
bucket elsewhere in the file.

Cart sizes in the load generator vary (multi-item carts are common), so
`discount-service`'s effective request rate now scales with average items-per-cart
instead of with `pricing-service`'s request rate — a multi-x increase in call
volume and downstream load with no corresponding increase in upstream traffic.

## Expected Causely signal

- `discount-service` call rate / CPU / latency symptom that doesn't correlate
  with `pricing-service`'s or `checkout`'s request-rate metric — the
  disproportion between the `pricing-service → discount-service` edge rate and
  `pricing-service`'s own inbound rate is the tell.
- Possible secondary effect on `loyalty-service`/`cache-service` (discount's
  own downstream) if the added load pushes them into elevated latency too.
- This should present differently from `inject/inject_discount_latency.sh`
  (Scenario 7 in `inject/`), which injects latency directly on
  `discount-service` itself — here `discount-service` may be perfectly
  healthy per-call, it's just getting called far more often than it should be.

## Expected fix

`pricing-service.calculateHandler` needs to go back to computing one aggregate
`baseTotal` across the cart and making a single call to `discount-service` per
request. The fix belongs in `pricing-service`, not `discount-service` — the
call pattern is wrong, not the callee.

## Deploy / verify

Operator steps, from this repo checkout (has `scenarios/`, fine for you):

```bash
git checkout feat/per-sku-discount-pricing
bash scenarios/11-pricing-n-plus-one/deploy.sh   # builds+pushes pricing-service, rolls it out
```

Then hand the agent under test a **separate** clone/worktree of
`feat/per-sku-discount-pricing` only (not this checkout — see
[Blind-test hygiene](../README.md#blind-test-hygiene--every-scenario-has-two-branches)).
It should open its fix PR against `main`.

To verify a fix and reset:

```bash
git checkout <agent's fix branch or commit>
bash scenarios/11-pricing-n-plus-one/deploy.sh
```
