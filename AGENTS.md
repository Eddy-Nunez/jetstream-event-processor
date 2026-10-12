# AGENTS.md — eventworker

Repo contract for AI agent sessions working in this repository.

## What this is

`eventworker` is the take-home service for **Zenity Senior SWE**: a Jetstream fanout processor.
Core (graded) artifact = the Go routing kernel: **ingest → classify → per-type fanout → independent workers**.

Read, in order:
1. `../misc/PLAN.md` — the working implementation plan (phases, scope control, deliverable mapping).
2. `openspec/changes/jetstream-fanout-processor/` — proposal.md, specs/, design.md, tasks.md (the authoritative task list).

## Repo layout

```
cmd/capture      Phase-1 bounded capture → testdata/sample.jsonl (done — stable fixture, do NOT re-run casually)
cmd/ingest       WS → decode-once → classify → NATS publish, cursor resume, -replay (done)
cmd/worker       generic per-type pull-consumer (done)
Dockerfile       multi-stage; one image (eventworker:dev), k8s picks the binary (done)
internal/ingest  Jetstream WS client (exists; reused, not rewritten)
internal/classify routing table — THE graded artifact (done)
internal/broker  NATS JetStream client: stream EVENTS, MsgId dedupe, pull consumers, DLQ (done)
internal/workers per-type handlers: posts matcher, like/repost counters, follow burst, delete TTL (done)
internal/health  /healthz, /readyz, /metrics (done)
metrics          eventworker_* Prometheus metrics (done)
k8s/             manifests: NATS StatefulSet, ingest, 5 workers, optional hpa.yaml (metrics-server) + localstack; k8s/load-image.sh (portable image load into kind/k3s.io/OrbStack) (done)
DESIGN.md        architecture + decisions (graded centerpiece)
throughput.md    measured fixture numbers
README.md        build / deploy / see-it-work
AI.md            honest provenance (what the agent did, what I overrode)
testdata/sample.jsonl  committed 60 s live capture; DO NOT regenerate casually
```

## Discipline (non-negotiable)

- **Build gate**: every change must leave `go build ./...` and `go test ./...` green.
- **Do not rewrite working code**: `internal/ingest` WS client + frame splitting are committed and
  shared by capture/live pipeline — reuse, and keep fixture fidelity.
- **Do not point subagents at the live firehose**: capture once to the fixture; analysis is offline.
- **Routing stays in Go**: NATS/LocalStack are fanout fabric/sinks, never the routing brain.
- **OpenSpec**: task tracking lives in the open change. Update tasks.md when a task is done;
  use `/opsx-apply`-style workflow to apply, archive when complete.
- **PLAN.md scope control**: identity/account drop-at-publish, ingest replica=1, LocalStack optional.
- **Commit convention**: one logical step per commit, message states what and why. Verify with `git status` before committing.
- **Fixture is as-captured**: never re-marshal/rewrite `testdata/sample.jsonl` lines.

## Verify before finishing a task

- `go build ./... && go test ./...` — green.
- The task's stated verification in `openspec/changes/jetstream-fanout-processor/tasks.md`.
- `git status` — only intended files.
