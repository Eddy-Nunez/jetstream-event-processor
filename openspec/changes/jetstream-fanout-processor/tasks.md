## 1. Fixture analysis (feeds DESIGN.md numbers)

- [x] 1.1 Analyze the committed `testdata/sample.jsonl` (offline, deterministic) and produce `throughput.md`: per-collection event schemas, dedupe-key heuristic confirmation, `seq` monotonicity check, and a per-type throughput table (rate high→low) — verify: `throughput.md` committed with schema + rates, no live-feed subagents used
- [x] 1.2 Fold the measured throughput + identity/account volume into DESIGN.md (retention/MaxAge values, replay-window claims) — verify: DESIGN.md references measured numbers from 1.1

## 2. Router, broker, ingest

- [x] 2.1 Implement `internal/classify` routing table `(kind, collection, operation) → Type` (delete-first, identity/account first-class, unknown → observable Drop, subject set mirrors wantedCollections) — verify: `go test ./internal/classify` passes table-driven tests and `go build ./...` is clean
- [x] 2.2 Implement `internal/broker`: JetStream stream `EVENTS` over `events.*`, file storage, MaxAge from 1.2, `Nats-Msg-Id` dedupe, per-type DLQ via MaxDeliver — verify: broker unit test publishes duplicates and asserts one delivery; consumer-filter-isolation test asserts an `events.<type>` consumer never sees `events.<type>.dlq` (why DLQ lives under `events.dlq.*`)
- [x] 2.3 Implement `cmd/ingest`: single WS connect → decode-once → classify → publish with MsgId; persist cursor, resume with inclusive-cursor first-event dedupe, monotonic time clamp — verify: fixture replay against local NATS yields exactly 42,101 stream messages (42,186 − 85 identity/account dropped), dedupe + cursor logic covered by unit tests
- [x] 2.4 Implement `internal/health`: `/healthz`, `/readyz`, `/metrics` (prometheus: drop/publish/ack/error counters) wired into ingest and worker binaries — verify: live curls return ok/ready and `/metrics` shows `eventworker_alerts_total`, `eventworker_drop_total`, `eventworker_errors_total`, `eventworker_inflight`

## 3. Workers

- [x] 3.1 Implement `cmd/worker` generic pull-consumer: bounded concurrency, ack/nak with deferral, `MaxDeliver` → DLQ — verify: `internal/workers/consumer_test.go` (embedded nats-server) acks happy path and lands a poison message on `events.dlq.<type>` after MaxDeliver
- [x] 3.2 Implement per-type logic: posts (keyword/language matcher + notification log, optional SQS sink), likes/reposts (in-memory rolling counters + threshold alert), follows (sliding-window burst detection, in-memory default, optional Redis), deletes (TTL tombstone set) — verify: `handlers_test.go` fixture contract test (expected 3,309 posts alerts computed independently == handler delta); live demo reproduced 3,309/41/3/1 exactly
- [x] 3.3 Implement graceful shutdown (stop pulling, ack in-flight, flush, persist cursor; `preStop` + `terminationGracePeriodSeconds` in manifests) — verify: consumer drains in-flight on ctx cancel (unit test); SIGTERM path exercised during the live kill-a-worker demo (pending drained to 0, `num_redelivered=0`)

## 4. k8s manifests

- [x] 4.1 NATS StatefulSet + PVC + Service (optional NATS GUI/exporter) — verify: `kubectl apply -k k8s/` on OrbStack k3s brings up nats-0 with a bound 2Gi PVC; `nats-monitor` service on 8222
- [x] 4.2 `ingest` Deployment (replica=1, cursor volume) + per-type worker Deployments each with own HPA (JetStream queue-depth), resource limits, DLQ — verify: deployed to OrbStack k3s; killed `worker-likes` mid-replay — other types kept flowing, replacement resumed the durable consumer, pending → 0, no redelivery storm. HPA is CPU-based (k3s substitute) with the queue-depth external-metric path documented in-manifest
- [x] 4.3 (optional) Redis StatefulSet + PVC for follows-burst state — verify: `k8s/optional-redis.yaml` ships + applies; follows stays in-memory by design (Redis-backed code deliberately not wired; documented in AI.md)
- [x] 4.4 (optional) LocalStack for posts→SQS notification sink — verify: `k8s/optional-localstack.yaml` ships + applies; sink path documented, not wired (parity story in DESIGN §6)

## 5. Tests and docs

- [x] 5.1 Complete meaningful test suite: classify table-driven (all route + drop cases), broker dedupe under redelivery, one worker contract (fixture → expected outcomes) — verify: `go test ./... -count=1` green incl. `go test -race ./internal/workers/`; fixture contract test asserts 3,309
- [x] 5.2 Write `DESIGN.md`: architecture, router decision + alternatives (Kafka/NATS/SQS), per-type ordering contract, backpressure/DLQ, AWS-parity production path, scale & fail isolation, measured numbers from 1.2 — verify: all 7 required sections present; every number matches throughput.md / independently re-derived from the fixture
- [x] 5.3 Write `README.md` (build, deploy, see-it-work incl. "ingest → NATS GUI shows events.* moving" and "kill a worker" demo) and `AI.md` (setup, model config, subagent prompts behind key decisions, honest "where agent helped / where I overrode") — verify: README alone reproduces the exact alert table and the kill-a-worker demo; AI.md discloses the worktree fanout, overrides, and residual risk
- [x] 5.4 Write/refresh `AGENTS.md` (repo contract: build/test discipline, PLAN pointer, commit conventions) and keep this change's spec/task targets authoritative — verify: AGENTS.md layout matches the repo; converter + build gates untouched; docs committed with this change
