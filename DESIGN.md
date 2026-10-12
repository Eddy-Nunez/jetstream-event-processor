# DESIGN — eventworker: a Jetstream fan-out processor

**Status:** architecture as committed (Tasks 2.1–2.4) and as specced for the worker tier (Task 3).
**Sources:** `internal/classify`, `internal/broker`, `internal/ingest`, `cmd/ingest`, `internal/health`, `internal/metrics`, `throughput.md`, and the OpenSpec change (`design.md` decisions D1–D6, `specs/*`). Every measured number below comes from `throughput.md`, the offline analysis of the committed `testdata/sample.jsonl` fixture (42,186 events; independent re-count reproduces every figure).

## 1. Architecture

```
                                    local k3s (demo topology)
  ┌──────────────────────────────────────────────────────────────────┐
  │                                                                  │
  │  Jetstream WS firehose                                           │
  │       │  single ordered connection, wantedCollections filtered   │
  │       ▼                                                          │
  │  cmd/ingest (replica=1)                                          │
  │    decode-once → internal/classify (ROUTER, the graded artifact) │
  │    Nats-Msg-Id publish → events.<type>   cursor file (atomic)    │
  │       │                                                          │
  │       ▼                                                          │
  │  NATS JetStream stream EVENTS (subjects events.>)                │
  │    durable file storage, server-side MsgId dedupe, MaxAge window │
  │       │                                                          │
  │       ▼                                                          │
  │  per-type pull consumers — cmd/worker × 5                        │
  │    posts | likes | reposts | follows | deletes                   │
  │    each: own Deployment, HPA (queue depth), limits,              │
  │          DLQ events.dlq.<type>                                   │
  │                                                                  │
  │  /healthz, /readyz, /metrics served per pod                      │
  └──────────────────────────────────────────────────────────────────┘
```

Ingest holds one ordered Jetstream WebSocket connection, decodes each frame once, and routes each event once (`cmd/ingest` `publishEvent` → `classify.Classify`); the resolved type selects the subject `events.<type>` and `Nats-Msg-Id = DedupeKey` (`internal/broker` `Publish`). JetStream streams all of `events.>` into one durable `EVENTS` stream (file storage, `MaxAge` replay window, discard-oldest, unlimited per-subject history) so slow workers never drop messages. Per-type workers are independent durable pull consumers with their own scaling, limits, and DLQ; `internal/health` serves `/healthz`, `/readyz`, `/metrics` per pod with the shared `eventworker_*` Prometheus metrics.

## 2. The router decision + alternatives

**Chosen: a pure Go routing table.** `Type = Classify(kind, collection, operation)` collapses three axes to one key held in a unit-tested map (`internal/classify/classify.go`). Design properties of the table, each backed by either the code or the fixture:

- **Delete-first:** a delete commit routes to the `deletes` type for *any* collection, because fixture-verified deletes carry no `record`/`cid` (all 1,181, `throughput.md` §3) and must never reach a content matcher (`classify.go` precedence, router spec).
- **Identity/account first-class:** those frame kinds always arrive regardless of `wantedCollections` (~1/s steady floor) and are classified as their own types so ingest can count and drop them by policy instead of silently losing them (`classify.go`, ingest spec).
- **Unknown → observable Drop:** any uncovered `(kind, collection, operation)` resolves to `Type.Drop` — counted via `eventworker_drop_total`, never published, never panicked (`classify.go`, `cmd/ingest` `publishEvent`, metrics spec).
- **Subject set mirrors `wantedCollections`:** `TypeColSet` (the four demo collections) is the same constant handed to the WS dial, so the routing table and server-side trimming cannot drift — drift would silently starve fanout (`classify.go`, `TypeColSet` used at dial time in `cmd/ingest`).

Alternatives weighed and rejected:

| Alternative | Rejected because |
|---|---|
| Routing inside NATS (subject-wildcard re-publish) | Moves the routing brain into config; the graded artifact must be code, testable and versioned (`design.md` D1). |
| Route by collection prefix only | One axis loses the delete and identity/account distinctions that the routing contract depends on (`design.md` D1). |
| Kafka / Redpanda | ~1 GB+ RAM per node plus operator ceremony is overkill for a k3s take-home; JetStream gives durability, MsgId dedupe, and DLQ in a ~20 MB process (`proposal.md` Non-Goals, `design.md` D2). |
| SQS / EventBridge as the router | Outsources the graded artifact and requires AWS; retained only as a *documented* production parity path plus one optional sink (`design.md` D2/D6, deploy spec). |

## 3. Exactly-once-ish ingest story

- **Cursor = last processed `time_us`.** Fixture-verified: `time_us` is present on all 42,186 events and strictly monotonic (0 regressions); there is **no top-level `seq`** — `seq` is nested inside `identity`/`account` only, and commits carry no `seq` at all (`throughput.md` §4, `internal/ingest/event.go`).
- **Inclusive-cursor dedupe.** Jetstream re-delivers the last event on resume, so the first event after every dial is skipped when its `DedupeKey` equals the persisted cursor (`cmd/ingest` `runLive` "first event" guard).
- **Server-side MsgId dedupe.** Every publish sets `Nats-Msg-Id = DedupeKey` (`internal/broker` `Publish`), with a 2-minute dedupe window that covers crash windows where the cursor was not yet persisted.
- **DedupeKey contract:** commits key on `did|collection|rkey|operation|rev` (fixture-verified: the four-field prefix was already unique in-window — 42,101 keys, 0 dups; `rev` covers update-bumps); identity/account key on their nested `seq` (`idn:`/`acct:` prefixes) (`internal/ingest/event.go`).
- **Atomic cursor file:** temp-file + rename, saved every 250 events and flushed on graceful shutdown; cursor progress is strictly monotonic (`if ev.TimeUS > cur.Time`), so Jetstream/NTP clock skew cannot roll the cursor back (`cmd/ingest` `saveCursor`/`runLive`). `-replay` feeds the fixture through the identical classify→publish path for a deterministic offline demo.

Net effect: no loss across reconnect and no duplicate per dedupe key at the stream — exactly-once per-key publish, “exactly-once-ish” end to end.

## 4. Ordering contract per type

JetStream preserves publish order per subject, and each worker pulls only its own subject in order — so ordering holds within a type. The contract is deliberately relaxed beyond that (`design.md` D3, workers spec):

- **posts / likes / reposts / deletes are commutative** — a keyword/language matcher, a rolling counter, and a TTL tombstone set have no strict-order requirement; a worker may process its subject's messages in any order inside its bounded concurrency window.
- **follows is window-based, not strict-order** — burst detection over a sliding time window tolerates reordering within the window.
- **No cross-type ordering is promised** — each type is an independent stream of a different fact; nothing requires a like to be observed before the post it targets.

Publish-side ordering is stream order from the single ingest connection, with redelivery collapsed by dedupe (Section 3).

## 5. Backpressure & DLQ

- **The durable stream absorbs spikes.** Measured ingest is ~470→700 ev/s (60 s capture upper bound; 89.8 s first-to-last `time_us` span is the conservative denominator), likes alone ~68% of commit volume. The stream keeps unlimited per-subject history (`MaxMsgsPerSubject: -1`) so spike bursts queue behind workers instead of being dropped; a 1 h `MaxAge` replay window ≈ 2.5M messages (~1.7M likes) at the upper bound (`throughput.md` §1–2, §5; `internal/broker` `EnsureStream`).
- **Bounded pull concurrency.** Workers pull with explicit-ack, bounded `MaxAckPending` consumers, ack only after per-type processing commits, and nak-with-deferral on transient failure (workers spec; `internal/broker` `PullConsumer`).
- **MaxDeliver → DLQ.** A message that exhausts delivery attempts lands on the per-type dead-letter subject `events.dlq.<type>`, inside the same stream. The DLQ namespace lives under `events.dlq.*` deliberately: JetStream consumer filters behave as prefixes, so a consumer filtered on `events.post` would otherwise also match `events.post.dlq`; the `events.dlq.*` prefix guarantees a type's consumer never sees its own dead letters (`internal/broker` package comment).
- **Queue depth → HPA.** Per-subject pending counts (stream stats) are the HPA input and the operator's stuck-type signal (broker spec, `design.md` D5).

## 6. AWS-parity production path

Documented, never the router. Routing stays in Go in every environment; AWS is a parity story plus one optional demo sink (`design.md` D2/D6, deploy spec):

- **Sink parity:** the posts worker's keyword/language matcher emits an on-hit notification — the one output that naturally maps to SQS/EventBridge. In production that outbox is replaced by the managed service; the ingest/router pipeline is unchanged.
- **Fabric parity:** the `EVENTS` JetStream stream maps to managed Kafka (MSK) or EventBridge streams with durable retention; `Nats-Msg-Id` dedupe maps to idempotent producers plus consumer offsets; `events.dlq.<type>` maps to per-type dead-letter queues.
- **Demo vs. prod:** local k3s (NATS StatefulSet + PVC, optional LocalStack gated behind config, HPA optional per `k8s/hpa.yaml`) is the demo topology — a smaller, unsecured, single-node instance of the same artifact, with an S3-backed JetStream cluster swapped in for production. Nothing about the routing path differs.

### From prototype to production (the hardening path)

The prototype is a smaller, unsecured, single-node instance of the same artifact — each gap below is the step to production:

- **Durability:** file-store NATS + PVC → JetStream raft cluster (3+) with S3-backed storage; raise `MaxAge` past the demo replay window.
- **Exactly-once-ish → exactly-once:** the 2-min `MsgId` dedupe window + inclusive-cursor guard cover crash windows; close the rest with an idempotent sink keyed on `DedupeKey` (SQS/EventBridge `MessageDeduplicationId`, or a SQL upsert), making post-window redelivery a no-op.
- **Security:** pod-local plaintext NATS → TLS + operator/client auth, network isolation, cursor-file permissions, secrets via k8s Secrets.
- **Observability:** add the fanout SLO (WS→publish→worker-ack latency) to the existing `/metrics`, alert on DLQ ingress (`events.dlq.*`), and switch worker HPA from CPU to a NATS queue-depth adapter (documented in `k8s/hpa.yaml`).
- **Drift control:** refresh `sample.jsonl` on a schedule and re-run the deterministic demo as a CI golden test, so schema drift surfaces as a diff.
- **Rollout:** CI per merge (build, `go test ./... -count=1`, replay golden, kill-a-worker recovery job); blue/green per worker Deployment (consumers drain in-flight and resume durably).

## 7. Scale & fail isolation

- **Ingest is single-replica and not the bottleneck.** One ordered WS cursor cannot be sharded cleanly across pods, and at a measured ~470–700 ev/s of decode-once-plus-classify work ingest is one small process — the consumption tier is what backpressure is for (`design.md` D4, deploy spec).
- **Per-type isolation.** Each worker type is its own Deployment with its own HPA (scaling on its own queue depth), resource limits, and DLQ subject. A stuck or saturated type accumulates to its own DLQ and cannot block another type's subject (`deploy spec` scenario "One type fails, others keep flowing"). The HPA manifests ship as an *optional* `k8s/hpa.yaml` (they need the cluster's metrics-server component, e.g. `k3s enable metrics-server` — not available on default kind/plain k3s); the core demo is Deployment + probes only and runs on any k8s.
- **Kill-a-worker guarantee.** Consumers ack after commit; on SIGTERM a worker stops pulling, drains and acks in-flight work inside its grace period, then exits — a killed worker causes redelivery, not loss, and other types keep flowing (workers spec; durable consumers retained across restart).
- **Measured load summary** (`throughput.md` §1–2; all counts re-verified against the fixture):

| Type / op | Count | Rate (ts-span → 60 s) | Share of commits |
|---|---|---|---|
| like · create | 28,720 | 320→479 /s | 68.2 % |
| post · create | 4,889 | 54→81 /s | 11.6 % |
| repost · create | 4,825 | 54→80 /s | 11.5 % |
| follow · create | 2,476 | 28→41 /s | 5.9 % |
| follow · delete | 457 | 5→7.6 /s | 1.1 % |
| like · delete | 418 | 5→7.0 /s | 1.0 % |
| post · delete | 163 | 2→2.7 /s | 0.4 % |
| repost · delete | 143 | 2→2.4 /s | 0.3 % |
| post · update | 10 | 0.1→0.2 /s | <0.1 % |

Totals: 42,186 events; 42,101 commits (99.8 %); 37 identity + 48 account frames (~1/s floor); 16,344 unique DIDs; 1,181 record-less deletes.
