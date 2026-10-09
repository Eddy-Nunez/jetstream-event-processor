## Context

See proposal.md — Why. Current repo state: Phase 1 is done and committed — `cmd/capture` produced a bounded 60 s `testdata/sample.jsonl` fixture (42k+ events) and `internal/ingest` has a working, tested-by-use Gorilla WS client with shared raw-frame splitting. There are zero specs/deployments yet. The environment is a Mac with OrbStack k3s (`orb start k8s`, namespace `sample` precedent in sibling repo). Go 1.24 module baseline; NATS JetStream is the intended broker; LocalStack strictly optional and additive.

## Goals / Non-Goals

**Goals:**
- One Go-owned routing decision per event (the graded artifact), executed before anything touches the broker.
- NATS JetStream as a durable fanout fabric with per-type isolation, DLQ, and backpressure visibility.
- Single ordered ingest connection with a resume cursor; workers independently scalable and killable without cascading failure.
- Everything demonstrable on local k3s with only NATS required.

**Non-Goals:**
- Horizontal scaling of ingest (one WS cursor, one replica — the firehose trim keeps one connection comfortable).
- A full read-model in LevelDB or similar (no stateful worker features scope it).
- Kafka/Redpanda or any broker that needs >100 MB/pod — NATS JetStream covers fanout + durability at ~20 MB.
- LocalStack as a router — the routing brain stays in Go; AWS is a documented parity story plus one optional sink.

## Decisions

### D1. Router is a pure Go table `(kind, collection, operation) → Type`
Why: it is small, unit-testable, and the single source of truth for both the NATS subject a worker consumes and the `wantedCollections` handed to the WS layer — a drift between those two would silently starve fanout. Delete commits are matched delete-first (they carry no record/CID and must never reach a content matcher); unknown intersections resolve to an observable Drop with a metric, never a panic or silent loss.
Alternatives considered: routing inside NATS via subject wildcard re-publish (moves the brain into config), route-by-prefix on collection only (loses the delete/identity/account distinction). Rejected: the graded artifact must be code, and one axis is not enough.

### D2. NATS JetStream over Kafka/Redpanda/SQS
Why: JetStream gives durable per-subject streams, server-side `Nats-Msg-Id` dedupe, per-subject DLQ via `MaxDeliver`, and queue-depth stats for HPA — all in a ~20 MB process that runs fine on a laptop k3s node. Kafka would cost ~1+ GB RAM per node plus operator ceremony for a take-home; SQS would outsource the demonstration and need AWS creds.
Alternatives considered: plain NATS core (no durability semantics for replay/DLQ), in-process channels with worker goroutines (no isolation story — one type's stall blocks the process). Rejected.

### D3. Generic worker binary, per-type by env; real per-type logic
Why: one `cmd/worker` with `EVENTWORKER_TYPE=posts|likes|reposts|follows|deletes` keeps the per-type surface honest while sharing pull/ack/retry/shutdown plumbing. Each deployment is its own HPA unit scaling on its own queue depth, and owns its DLQ subject. Processing is real: posts keyword/language match, likes/reposts rolling counters + threshold alert, follows sliding-window burst detection (the only stateful one), deletes TTL tombstone set.
Ordering contract: posts/likes/reposts/deletes are commutative (a matcher/counter/set); follows is window-based, not strict-order — so no per-type ordering guarantee is promised beyond JetStream's per-subject order.

### D4. Ingest = one ordering owner; cursor written on shutdown + periodically
Why: Jetstream's inclusive cursor re-delivers the last event on resume, so the pipeline persists the last processed cursor and dedupes the first redelivered event. **Fixture-verified cursor facts (throughput.md §4):** every frame carries `time_us` (strictly monotonic in the 42,186-event sample) → the resume cursor is last `time_us`; there is **no top-level `seq` anywhere** — `seq` is nested (`identity.seq` / `account.seq`) and unused for commits. DedupeKey = `did|collection|rkey|operation|rev` for commits (`did|collection|rkey|operation` was already unique in-window; `rev` covers update-bumps), `idn:<nested seq>` for identity/account. A time clamp guards against Jetstream/NTP clock skew making the cursor roll back.
Alternatives considered: multiple ingest replicas with sharded cursors — rejected, the single WS connection is not the bottleneck and one replica makes the cursor story simple and correct.

### D5. Health/metrics per component, depth-driven HPA
Why: readiness for ingest means "WS connected and publishing"; worker readiness means "consumer alive with empty in-flight"; `/metrics` exposes per-type drop/publish/ack counts; queue depth comes from JetStream stream stats, feeding each worker's HPA.

### D6. Optional stores are isolated, additive, and documented
LocalStack (the posts→SQS notification sink) is gated behind config so the core demo needs neither it
nor any external state: follows-burst detection stays in-memory by design.

## Risks / Trade-offs

- [Jetstream throughput spikes to tens of thousands of events/sec] → wantedCollections trims commit volume; identity/account floor is dropped-at-publish with a counter; a one-event-per-decode pipeline with a single WS reader is not the bottleneck — fanout/backpressure is, which is exactly what JetStream's durable streams + per-type DLQ absorb.
- [Inclusive-cursor duplicate on every reconnect] → persisted cursor + first-event dedupe + `Nats-Msg-Id`; integration test replays the fixture to prove exactly-once-per-key publish.
- [Workers with real logic could misbehave in the demo] → each type is bounded (concurrency limit, MaxDeliver, DLQ); killing one worker must provably not affect the others (Phase-4 demo).
- [k3s single node: stateful NATS + PVC is simple but not HA] → documented as the take-home topology; production path (JetStream 3+ cluster, S3-backed) is a DESIGN.md story, matching the AWS-parity requirement.
- [Clock skew on `time_us`] → monotonic clamp in the cursor path only; fixture stays as-captured for honest analysis.

## Migration Plan

Greenfield service repo; nothing to migrate. Rollback = revert commits / `kubectl delete -f k8s/`. Deploy order: NATS StatefulSet+Service → ingest → workers → (optional localstack).

## Open Questions

Now answered by fixture analysis (see throughput.md §4–§5): schema nuances, the cursor key (`time_us`), dedupe-key uniqueness, and identity/account volume (≈1 ev/s floor). Remaining deferrable:** none that change the specs, the approach, or the task breakdown — per-type retention (`MaxAge` = 1 h demo window, ≈2.5M messages at the measured upper bound) is a tunable constant set in Task 2.2, not a design fork.
