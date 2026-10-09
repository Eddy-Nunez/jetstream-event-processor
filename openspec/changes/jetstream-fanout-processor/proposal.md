## Why

Jetstream is a continuous, high-volume firehose; routing it fan-out to per-type downstream workers is the core engineering problem this take-home must demonstrate. Without a Go-owned classifier and a durable per-type fabric, either every worker re-parses the full stream (wasteful, tightly coupled) or routing is outsourced to infrastructure (nothing gradeable, no isolation story). We need a small, real, deployable system: ingest → classify → per-type fanout → independent workers, with backpressure, DLQ, and fail isolation — sized for a ~2–3 h build.

## What Changes

- **New service repo `eventworker/`**: Go binaries `cmd/ingest` and `cmd/worker`, sharing internal packages.
- **Router in Go** (the graded artifact): one pure routing table `(kind, collection, operation) → Type`, unit-tested, with unknown intersections dropped but observable via metrics — never silently lost.
- **NATS JetStream as fanout fabric**: stream `EVENTS` over subjects `events.*`, server-side `Nats-Msg-Id` dedupe, per-type DLQ after `MaxDeliver`.
- **Ingest pipeline**: single WS connection to `jetstream2.us-east.bsky.network`, decode-once, classify, drop identity/account per policy, publish with cursor persistence; on resume the inclusive-cursor duplicate is deduped.
- **Per-type workers** (posts, likes, reposts, follows, deletes): independent pull-consumers with real per-type logic (keyword/language match, rolling counters + threshold alert, burst detection, tombstone TTL set), bounded concurrency, graceful shutdown.
- **k8s manifests**: ingest `replica=1` Deployment, per-type worker Deployments each with own HPA/limits/DLQ, NATS StatefulSet + PVC, optional LocalStack (one-type AWS sink) — flagged optional.
- **Deployable=true**: builds clean, deploys to local k3s, `go test ./...` green, 1–3 meaningful tests incl. routing.

Not changing: the existing Phase-1 bounded capture (`cmd/capture`, committed `testdata/sample.jsonl` fixture) — it stays as the fidelity fixture and resume-test input.

## Capabilities

### New Capabilities
- `ingest`: Jetstream WS ingestion — single ordered connection, decode-once, cursor resume with inclusive-cursor dedupe, identity/account drop policy, fixture-fidelity framing.
- `router`: the routing kernel — `(kind, collection, operation) → Type` table, delete-first-class routing, unknown → observable drop, subject contract shared with the WS wantedCollections.
- `broker`: NATS JetStream fanout fabric — stream/subject topology, MsgId dedupe, retention limits, per-type DLQ, backpressure visibility.
- `workers`: per-type independent consumers — real per-type downstream logic, bounded concurrency, ack/nak, graceful shutdown, TLS-free local operation.
- `deploy`: cluster topology — ingest single-replica deployment, per-type worker deployments with own HPA/limits/DLQ, NATS stateful storage, optional LocalStack sink, fail-isolation story.

### Modified Capabilities
<!-- none: this change introduces the first specs in the repo -->

## Impact

- **Code**: new Go module `eventworker` (`cmd/ingest`, `cmd/worker`, `internal/{ingest,classify,broker,workers,health}`); existing `cmd/capture` + `internal/ingest` WS client are reused, not rewritten.
- **Deps**: `github.com/nats-io/nats.go` (JetStream), `github.com/gorilla/websocket` (present), prometheus client (metrics).
- **Infra**: local k3s (OrbStack); NATS StatefulSet + PVC; optional LocalStack. No cloud resources.
- **Docs**: DESIGN.md (architecture + routing decision & alternatives), README.md (build/deploy/see-it-work), AI.md (setup + agent provenance/honesty).
