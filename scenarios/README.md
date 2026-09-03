# Regression Scenarios (code/config fix required)

The scenarios under `inject/` are **runtime toggles**: they flip a fault-injection
flag on a live pod via its `/admin/config` endpoint, and `restore_*.sh` flips the
same flag back. There is no source diff involved, so they're good for testing
detection/diagnosis but don't exercise an agent's ability to actually produce a
fix — "restoring" them is a curl call, not a PR.

The scenarios in this directory are different: each one ships a real regression
to application code or a Kubernetes manifest — as if a bad PR had merged — and
the fix is a real diff an agent has to write and get merged, not a flag to flip.

## Lifecycle

1. **Baseline.** `main` (and this repo's default branch) is always the healthy
   state. Load generator running, Causely green.
2. **Regression lands.** Each scenario's code lives on a dedicated branch
   (`scenario-XX-<slug>-bug`) containing the regression commit(s), simulating a
   PR that already merged. Deploying a scenario means building just the one
   affected service's image from that branch and rolling it into the existing
   `scenario-01` namespace — the rest of the fleet is untouched.
3. **Symptom appears.** Within ~60s of load hitting the new code path, Causely
   should surface a symptom/diagnosis pointing at the right entity.
4. **Agent fixes it.** The agent should be pointed at the *symptom*, not the
   git history (e.g. "checkout latency is elevated, find and fix the root
   cause" — not "what changed recently"). It uses the Causely MCP tools to
   localize the faulty entity, reads the current source, and writes a fix —
   ideally as a PR back onto `main`.
5. **Verify.** Rebuild/redeploy from the fix, confirm the symptom clears.

## Why the regressions aren't single, cleanly-revertable commits

If a scenario were "one isolated commit on top of a clean baseline," the
"correct" fix degenerates to `git revert <sha>` — that tests git archaeology,
not engineering judgment. Each scenario branch here interleaves the regression
with at least one unrelated, realistic commit, and the regression itself is
never a mechanically-revertable formatting change (see each scenario's README
for specifics). The intent is that an agent solving this from Causely symptoms
alone — without being told to inspect git history — arrives at the fix by
reading and reasoning about the current code, not by finding and undoing a
commit.

## Grading

Grade the resulting code/config state and whether the symptom clears, not
whether the diff byte-matches the pre-regression version. A differently
written fix that is behaviorally correct should pass.

## Scenarios

| # | Slug | Fix type | Service | Symptom |
|---|------|----------|---------|---------|
| 9 | [billing-missing-timeout](09-billing-missing-timeout/README.md) | App code | `billing-service` | Rising latency / stuck in-flight requests during a downstream blip that used to be bounded |
| 10 | [recommendation-oom](10-recommendation-oom/README.md) | K8s config | `recommendation-service` | OOMKilled / CrashLoopBackOff under normal load |
| 11 | [pricing-n-plus-one](11-pricing-n-plus-one/README.md) | App code | `pricing-service` / `discount-service` | `discount-service` call rate & latency scale with cart size instead of request rate |
