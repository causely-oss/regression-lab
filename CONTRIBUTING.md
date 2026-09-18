# Contributing

Thanks for looking. This repo's job is to ship **real, mergeable regressions** — a service or
manifest that looks like it passed review — so an SRE agent's fix has to be a genuine diff, not a
`git revert`. Most of the conventions below exist to protect that.

## Dev loop

```bash
kind create cluster --name scenario-01     # or point kubectl at any cluster
bash k8s/build-images.sh                   # build + push all 36 service images
kubectl apply -f k8s/                       # deploy the stack
bash load/generate_load.py                  # start traffic (see README.md for details)
```

See `README.md` for the full build/deploy/load walkthrough, including the kind-specific image
loading step and the optional Causely configuration pass.

## Regenerating services

`generate_services.py` is a one-time scaffold generator — it wrote the initial 36
`environment/services/*/main.go`, `go.mod`, `Dockerfile`, and the k8s/docker-compose/prometheus
config that reference them. **Do not re-run it** against a service that already carries a
scenario regression or a hand-written fix; it will overwrite that file with the generic template
and silently erase the regression.

## Adding a regression scenario

Each scenario needs:

1. The regression itself, as a real commit (or interleaved commits — see below) on a
   `scenario-NN-<slug>-bug` branch, plus a neutrally-named branch off `main` carrying the same
   change with no scenario-identifying name, path, or string anywhere in its tree or history.
2. A `scenarios/NN-<slug>/` directory: a `README.md` describing the symptom and the fix, a
   `deploy.sh` that builds and rolls out just the affected service (tagged `build-<short-sha>`,
   never a scenario name), and `trigger.sh` / `restore.sh` if the regression needs a runtime nudge
   to manifest.
3. A row in `scenarios/README.md`'s table.

### Why the regression isn't a single, cleanly-revertable commit

If a scenario were "one isolated commit on a clean baseline," the correct fix degenerates to
`git revert <sha>` — that tests git archaeology, not engineering judgment. Interleave the
regression with at least one unrelated, realistic commit, and make sure the regression itself
isn't a mechanically-revertable formatting change. The goal is that an agent working from the
Causely symptom alone — without being told to inspect git history — has to read and reason about
the current code to find the fix.

### Blind-test hygiene

The neutral branch must never leak the scenario's identity: no `scenario-NN` string in a path,
branch name, commit message, log line, image tag, or Kafka topic. See `scenarios/README.md` for
the full rationale and the two-branch split it's built around.

## No environment-specific values

This repo was extracted from one author's cluster, and the extraction included a pass to strip
personal paths, credentials, and internal hostnames from history. Don't reintroduce that class of
value — local filesystem paths, personal registry namespaces, internal cluster/mediator hostnames,
or real tokens. Use placeholders (`/path/to/your/regression-lab`, `<your-username>`,
`mediator.<namespace>:4317`) instead, the same way the existing docs do.

## License

By contributing you agree that your contributions are licensed under [Apache 2.0](LICENSE).
