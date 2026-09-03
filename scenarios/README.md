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

## Blind-test hygiene — every scenario has two branches

Every scenario exists as **two** branches, and they are not interchangeable:

- **`scenario-NN-<slug>-bug`** — the descriptive, documented branch (this is
  what these READMEs reference by name). It forks from `scenario-01-otel-request-rates`,
  which also carries this whole `scenarios/` directory in its tree. **Never
  check this branch out into a workspace an agent under test can read** — the
  branch name and the `scenarios/` directory sitting right there in the tree
  are the answer key.
- **A neutrally-named branch off `main`** (e.g. `chore/billing-http-client-pooling`,
  `chore/service-resource-rightsizing`, `feat/per-sku-discount-pricing`) —
  the same regression + filler commits, forked from `main` before any of this
  documentation existed, so its tree and history contain nothing scenario-related.
  **This is the branch to give the agent under test.**

This split exists because of a real failure: an early run against scenario 9
had an agent (with full local repo + kubectl + shell access) diagnose the
issue correctly but explicitly call it out as "the scenario-09 billing-timeout
regression" and refer to "the trigger" — vocabulary lifted straight from this
README, which was sitting in its working tree. A second leak in the same run:
the deployed image was tagged `causely-oss/billing-service:scenario-09`, visible
via a plain `kubectl describe pod`. Both are structural risks any time the
investigating agent has shell/git/kubectl access (not just when it's scoped to
Causely MCP tools) — the environment itself must not be able to say its own
scenario number out loud.

When handing off to an agent under test:

1. **Deploy from the neutral branch**, not the descriptive one — `git checkout
   <neutral-branch>` before running `deploy.sh`, so the running image and the
   working tree it was built from are both clean.
2. **Give the agent a genuinely separate `git clone`, not this checkout, and
   not a `git worktree`.** Checking out the neutral branch in this same repo
   (or `git worktree add`) does *not* isolate anything — a worktree shares
   this repo's `.git` object database and refs, so `git branch -a` /
   `git log --all` / `git show <other-branch>:<path>` still see every
   `scenario-NN-*-bug` branch and this `scenarios/` directory no matter what's
   checked out. Only a fresh clone, restricted to one branch, has none of that
   in its object database to find. This repo's git root is one level up from
   `regression-lab` (a shared monorepo with many unrelated branches too), so
   clone from there:
   ```bash
   git clone --single-branch --branch <neutral-branch> --no-tags \
     /path/to/your/regression-lab /path/for/agent
   ```
   Start the agent in `/path/for/agent/regression-lab`. Use a fresh destination
   path per run — don't reuse one an earlier run may have touched.
3. **Verify the deployed image tag has no scenario-identifying string** —
   `deploy.sh` tags builds as `build-<short-sha>` for exactly this reason;
   don't override `TAG` with something scenario-named.
4. The agent's fix PR should target `main` (or wherever the neutral branch's
   fix should land), not the descriptive `scenario-NN-*-bug` branch. If the
   fix needs to land as a real GitHub PR, the neutral branch has to be pushed
   to `origin` first — as of this writing none of the scenario branches
   (neutral or descriptive) have been pushed; they're local-only.

## Grading

Grade the resulting code/config state and whether the symptom clears, not
whether the diff byte-matches the pre-regression version. A differently
written fix that is behaviorally correct should pass.

## Scenarios

| # | Slug | Fix type | Service | Deploy branch (agent-facing) |
|---|------|----------|---------|-------------------------------|
| 9 | [billing-missing-timeout](09-billing-missing-timeout/README.md) | App code | `billing-service` | `chore/billing-http-client-pooling` |
| 10 | [recommendation-oom](10-recommendation-oom/README.md) | K8s config | `recommendation-service` | `chore/service-resource-rightsizing` |
| 11 | [pricing-n-plus-one](11-pricing-n-plus-one/README.md) | App code | `pricing-service` / `discount-service` | `feat/per-sku-discount-pricing` |
