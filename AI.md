# AI.md — how this repo was built, honestly

This file documents where an AI coding agent (pi, Claude-based) contributed, what I (the human
developer) overrode, and how to reproduce the collaboration. It's written so a grader can tell exactly
what is agent-generated, what is verified, and what residual risk remains.

## Setup

- Agent harness: **pi** (earendil-works/pi), interactive dashboard session, Claude Sonnet-class model.
- Tasks: `openspec/changes/jetstream-fanout-processor/tasks.md` (OpenSpec change
  `jetstream-fanout-processor`), driven by `PLAN.md` (§1 architecture, §5 deliverables).
- Environment: macOS; local k3s verified via OrbStack (docker runtime). Deliverables are
  cluster-agnostic — see the Portability note below.
- Repo/deps: Go 1.26 module `eventworker`, deps `nats.go`, `nats-server/v2` (test-only),
  `prometheus/client_golang`, `gorilla/websocket`.

## What the agent produced (verified before commit)

| Artifact | How verified |
|---|---|
| `internal/classify` router (tasks 2.1) | table-driven tests; unknown→Drop; delete-first precedence |
| `internal/broker` JetStream layer (2.2) | hermetic embedded-nats tests: MsgId dedupe, consumer filter isolation, DLQ |
| `cmd/ingest` (2.3) | fixture replay → 42,101 messages, exactly 42,186−85 identity/account (live check) |
| `internal/health`, `internal/metrics` (2.4) | `/healthz` `/readyz` `/metrics` on both binaries |
| `internal/workers` + `cmd/worker` (3.1–3.3) | live demo counts matched fixture-computed expectations **exactly** (3309/41/3/1), DLQ + race-safe tests |
| `k8s/` + `Dockerfile` (4.1–4.4) | `kubectl apply -k k8s/` on OrbStack k3s: pods 1/1, live kill-a-worker demo; portability pass (HPA split to optional `k8s/hpa.yaml`, portable image loader `k8s/load-image.sh`) |
| `DESIGN.md` (5.2) | every number re-verified against `testdata/sample.jsonl` independently |
| `throughput.md` (1.1) | offline deterministic analysis of the committed fixture |

Concurrency note: the worker, k8s, and DESIGN lanes were run as **three parallel subagents, each in its
own git worktree** off clean HEAD, writing disjoint paths (`internal/workers`+`cmd/worker`,
`k8s`+`Dockerfile`, `DESIGN.md`). Patches were reviewed and applied by me after the lanes completed;
the full test suite and the live smoke went green **only after** reconciliation.

## Where I overrode the agent (the important part)

- **DLQ namespace `events.dlq.<type>`** (not `events.<type>.dlq`): JetStream consumer filters are
  prefix-matching (`events.post` matches `events.post.dlq`), so the per-type DLQ must live under its
  own `events.dlq.*` prefix or a type's consumer would eat its own dead letters. Caught by the broker
  test; baked into the broker package comment.
- **Consumer bind via PullSubscribe options** — an `AddConsumer`-then-bind approach silently created an
  unfiltered consumer (caught by test). The worker binds with `nats.BindStream`, `AckExplicit`,
  `MaxDeliver(-1)`, explicit `MaxAckPending`.
- **Cursor = last `time_us`**: the fixture has *no* top-level `seq` (verified: identity/account `seq` is
  nested; commits have none), so the original cursor-by-`seq` idea was dropped early.
- **Demo threshold calibration**: the fixture's per-URI maxima are 80 likes / 30 reposts / 24 follows,
  so production-looking defaults (200/150/30) fire zero alerts. For the "see it work" demo I set
  20/20/5 in `k8s/workers.yaml` (with a comment) and published the exact reproduction table in README.
- **"deletes ~7%" headline dropped**: `throughput.md`'s early headline doesn't reproduce from the
  fixture (1,181/42,101 = 2.8%); DESIGN.md carries the verifiable per-row counts instead.
- **ConnectRetry added after live deploy**: OrbStack k3s pod startup raced the NATS StatefulSet
  ("nats: no servers available"), so workers crash-looped once before converging. I added a bounded
  retry/backoff (`broker.ConnectRetry`) to both binaries — post-integration polish discovered by the
  live deploy, not by the lane briefs.
- **Cluster independence (later pass)**: HPA resources were split out of `k8s/workers.yaml` into
  optional `k8s/hpa.yaml` (the `HorizontalPodAutoscaler` kind needs the cluster's metrics-server
  component — not shipped by default k3s.io/kind), and `k8s/load-image.sh` was added so a locally
  built image loads into kind (`kind load`), k3s.io (`docker save | k3s ctr images import`), or
  OrbStack (no-op — shared Docker daemon). No manifest or code depends on OrbStack; the only
  OrbStack-specific claim was README wording, now generalized.
- **Fixture fidelity**: subagents were pointed at `testdata/sample.jsonl` and the OpenSpec artifacts
  only; **no subagent was pointed at the live firehose** (capture happened once, committed raw).

## Residual risk / not done

- In-memory handler state resets on worker restart (design-accepted: like/repost/follow windows and
  delete tombstones are observations, not durable facts — follows burst stays in-memory by design).
  No cross-type ordering promised. LocalStack sink is shipped as an optional manifest only. MsgId dedupe
  window (2 min) + inclusive-cursor dedupe gives exactly-once-ish, not exactly-once, delivery.
- The cluster demo was torn down (`kubectl delete -k k8s/`); README reproduces it.

## Reproduce

```sh
git log --oneline          # one logical step per commit, messages state what & why
go test ./... -race ./internal/workers/   # hermetic, no network
# local demo in README: docker NATS + ingest -replay + five workers → the exact alert table
```
