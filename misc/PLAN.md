# Implementation Plan — Jetstream Fanout Processor

**Status:** working plan (feeds `DESIGN.md` / `README.md` / `AI.md`)
**Budget:** ~2–3 h total. Sized to be *done*, not sprawling.
**Scope discipline:** the assignment grades your *routing/fanout design*. Core = ingest → classify → per-type fanout → independent workers. Everything else is optional and explicitly flagged.

---
## 0. Grounding facts (verified against live Jetstream docs)

| Fact | Consequence for design | VERIFIED |
|---|---|---|
| `wantedCollections` filters **commit** events only; **identity/account/sync always delivered** | Cannot drop identity/account at WS layer — must classify-and-drop in Go ingest; they are a guaranteed steady volume floor | ✓ |
| Cursor is **inclusive** (re-receive event N on resume) | Persist cursor = last processed `seq`/`time_us`; dedupe first event after each resume | ✓ |
| Delete commits carry **no record/cid** — only `{operation, collection, rkey}` | Deletes worker gets the key only; "retraction/cleanup" of previously-stored state, not content capture | ✓ |
| Throughput: single firehose subject, all collections — **low thousands of events/sec typical, spikes to tens of thousands**; wantedCollections trims it | One Go WS consumer + classifier is not the bottleneck; fanout/backpressure design is | ✓ (order-of-magnitude) |
| Two consumer endpoints: `jetstream2.us-east.bsky.network` (global) and regional `jetstream.us-east.bsky.network` | Pick one; `jetstream2` is the shared/global public endpoint | ✓ |

> ⚠️ Identity/account derive from the **ability to confirm exact event schemas + per-type throughput on real data**. Treat the table as "documented from a bounded sample" — see Phase 慣2.

---
## 1. Architecture (decided)

```
                     ┌─────────────────────────────────────────────┐
  wss://jetstream2   │  ingest Deployment (replicas: 1)            │
  ───────────────►   │  WS → decode once → classify               │
                     │  (kind + collection + operation)           │
                     │  → drop identity/account/sync per policy   │
                     │  → publish to per-type NATS subject        │
                     └───────────────┬─────────────────────────────┘
                                     │  NATS JetStream (broker; PVC-backed)
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
             events.posts      events.likes     events.follows   (…deletes)
                    │                │                │
                    ▼                ▼                ▼
             worker-posts    worker-likes    worker-follows   worker-deletes
             Deployment      Deployment      Deployment       Deployment
             (own HPA,       (own HPA,       (own HPA,        (own HPA,
              limits, DLQ)    limits, DLQ)    limits, DLQ)     limits, DLQ)
```

- **Router owns routing in Go** (this is the graded artifact). NATS is the *fanout fabric*, not the brain.
- **AWS parity** is a documented story in DESIGN.md + optional LocalStack sink for ONE event type (notifications — posts keyword match → SQS/EventBridge out), so you show AWS-native integration without outsourcing the router.
- **Redis** is scoped down (see §4) — keep it to ONE demonstration worker, optional.

---
## 2. Repo layout (new service repo, sibling to `sample-k8s`)

```
eventworker/
├── cmd/
│   ├── ingest/            # WS → classify → NATS publisher (+ cursor persistence)
│   └── worker/            # generic worker binary; type configured via env/flag
├── internal/
│   ├── ingest/            # jetstream WS client, cursor, reconnect, classify
│   ├── classify/          # kind+collection+operation → type router table
│   ├── broker/            # NATS JetStream client (streams, subjects, ack, dlq)
│   ├── workers/           # per-type worker implementations (posts/likes/follows/deletes)
│   ├── store/             # Redis KV upsert client (only used by stateful worker)
│   └── health/            # liveness/readiness HTTP handlers (+ /metrics)
├── testdata/
│   └── sample.jsonl       # bounded captured live sample (resume test fixture; committed)
├── k8s/                   # manifests: natss, redis, ingest, worker-* , optional localstack
├── Dockerfile
├── README.md
├── DESIGN.md
├── AI.md
├── AGENTS.md
└── openspec/              # feature targets + tasks tracked with openspec
```

---
## 3. Phases (tuned to ~2–3 h)

### Phase 1 — Capture bounded sample (~20 min)
- **Task 1.1** Connect a **single** WS connection, drain **60 s** with wantedCollections on the collections you demo, write to `testdata/sample.jsonl`. Exit. (Bluesky throttles per-IP WS; one bounded capture, not N live subagents.)
- **Task 1.2 (subagent, offline against sample)** Produce: per-collection event schemas, dedupe-key heuristic confirmation, `seq` monotonicity check, and a **throughput table (per type, src/sec, high→low)**. Commit findings → feed DESIGN.md.
- **Deliverable:** `testdata/sample.jsonl` (doubles as integration-test fixture) + `throughput.md`.

### Phase 2 — Skeleton + broker (~45 min)
- **Task 2.1** `go mod init`; `internal/classify`: router table keyed by `(kind, collection, operation)` → `Type`; unknown → dropped with metric. Pure, unit-testable (first test).
- **Task 2.2** `internal/broker`: NATS JetStream — create stream `EVENTS` (storage file, retention limits, `MaxAge` ~= replay window, `MaxMsgsPerSubject` 0, discard-oldest, `MsgId` dedupe server-side via `Nats-Msg-Id`), subjects `events.*`.
- **Task 2.3** `cmd/ingest`: connect WS, decode once, classify, publish with `MsgId`; on reconnect resume with persisted cursor.
- **Task 2.4** `internal/health`: `/healthz`, `/readyz`, `/metrics` (prometheus).
- **Deliverable:** ingest pod publishes classified events to JetStream; README captures "ingest → NATS GUI shows `events.*` subjects moving".

### Phase 3 — Workers (~45 min)
- **Task 3.1** `cmd/worker` generic pull-consumer (bounded concurrency, ack/nak, retry→`events.<type>.dlq` after `MaxDeliver`).
- **Task 3.2** Worker implementations (downstream "work" simulated but *real logic*):
  - `posts`: keyword/language matcher → on-hit notification (log line + optional LocalStack SQS out). **Ordering: irrelevant (commutative matcher)**
  - `likes`/`reposts`: in-memory rolling counters → threshold alert. **Ordering: irrelevant (commutative)**
  - `follows`: sliding-window **burst detection** per target account (needs small state — first realistic Redis use). **Ordering: burst-window, stateful**
  - `deletes`: tombstone set with TTL (bounded, in-memory). **Ordering: commutative set**
- **Task 3.3** Graceful shutdown: stop pulling, ack in-flight, flush, persist cursor; `preStop` + `terminationGracePeriodSeconds`.
- **Deliverable:** per-type workers consuming their own subject; kill one → others keep flowing.

### Phase 4 — k8s manifests (~25 min)
- **Task 4.1** NATS StatefulSet (PVC) + Service; optional GUI (`natsboard`/NUI on `:8222`) + `prometheus-nats-exporter` for queue-depth → worker HPA.
- **Task 4.2** `ingest` Deployment (replica=1 — WS cursor correctness), `worker-*` Deployments each with own HPA, resource limits, DLQ subject, own deployment selects.
- **Task 4.3** (optional) Redis StatefulSet + PVC, `redis:7-alpine`.
- **Task 4.4** (optional) LocalStack (`localstack/localstack`, helm chart) — SQS/EventBridge sink for the posts-notification worker.

### Phase 5 — Tests + docs (~40 min)
- **Task 5.1** Tests: `classify` (table-driven), `broker` dedupe under redelivery, one worker contract (consume sample.jsonl fixture → expected outcomes). 1–3 meaningful tests beat shallow breadth.
- **Task 5.2** DESIGN.md (§ architecture, § router decision & alternatives weighed — Kafka/NATS/SQS, § ordering contract per type, § backpressure/DLQ, § AWS-parity prod path, § scale & fail isolation).
- **Task 5.3** README (build/deploy/see-it-work) + AI.md (setup, models, the subagent prompts behind key decisions + honest "where agent helped / where I overrode").
- **Task 5.4** AGENTS.md + openspec feature targets (keep updated).

---
## 4. Scope control (what NOT to build)

| Idea | Verdict | Why |
|---|---|---|
| Full Redis read-model / KV store for every type | **Cut to ONE worker's small state** (follows burst), optional | Biggest scope surface; assignment explicitly allows simulated downstream. A reviewer shouldn't need Redis stood up to see routing work |
| LocalStack EventBridge *as the router* | **No — document only** (+optional one-type sink) | Outsources exactly the routing being graded; localstack is an emulator with mock latency |
| Kafka/Redpanda broker | **No** | ~1+ GB RAM/node, PVC, admin — overkill for k3s take-home; NATS JetStream covers fanout+durability at ~20 MB |
| Multiple ingest replicas / HPA on ingest | **No (replica=1)** | Single ordered WS cursor can't cleanly shard across pods; 1 WS conn is not the bottleneck |
| WantedCollections=off, full firehose | **Trim with wantedCollections** | Cuts volume massively; identity/account still arrive so you must handle them anyway |
| Live-firehose data in N subagents | **No** | Capture once to a fixture; all analysis offline deterministically |

---
## 5. Deliverable mapping

| Deliverable | Produced by |
|---|---|
| Go service (Dockerfile + manifests, deploys to k3s) | Phases 2–4 |
| README (build/deploy + how to see it work) | 5.3 |
| DESIGN.md (1–3 pp, the graded centerpiece) | 5.2 + findings from 1.2 |
| AI.md (setup + prompts + where agent helped/overrode) | 5.3 |
| 1–3 meaningful tests incl. routing | 5.1 |
| (bonus) AGENTS.md + openspec targets | 5.4 |

---
## 6. Open decisions to confirm with docs/real sample
- Exact per-collection schemas + dedupe key (sha1 of `did|collection|operation|rkey|rev`, or seq for identity/account) — **confirm against sample in Phase 1**
- desired per-type retention (MaxAge) tuned to throughput table
- whether to keep Redis-backed follows burst vs pure in-memory sliding window (default: in-memory, Redis optional)
