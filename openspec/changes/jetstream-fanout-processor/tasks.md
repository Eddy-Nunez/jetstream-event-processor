## 1. Fixture analysis (feeds DESIGN.md numbers)

- [x] 1.1 Analyze the committed `testdata/sample.jsonl` (offline, deterministic) and produce `throughput.md`: per-collection event schemas, dedupe-key heuristic confirmation, `seq` monotonicity check, and a per-type throughput table (rate high→low) — verify: `throughput.md` committed with schema + rates, no live-feed subagents used
- [x] 1.2 Fold the measured throughput + identity/account volume into DESIGN.md (retention/MaxAge values, replay-window claims) — verify: DESIGN.md references measured numbers from 1.1

## 2. Router, broker, ingest

- [ ] 2.1 Implement `internal/classify` routing table `(kind, collection, operation) → Type` (delete-first, identity/account first-class, unknown → observable Drop, subject set mirrors wantedCollections) — verify: `go test ./internal/classify` passes table-driven tests and `go build ./...` is clean
- [ ] 2.2 Implement `internal/broker`: JetStream stream `EVENTS` over `events.*`, file storage, MaxAge from 1.2, `Nats-Msg-Id` dedupe, per-type DLQ via MaxDeliver — verify: broker unit test publishes duplicates and asserts one delivery, DLQ test with failing consumer lands in `events.<type>.dlq`
- [ ] 2.3 Implement `cmd/ingest`: single WS connect → decode-once → classify → publish with MsgId; persist cursor, resume with inclusive-cursor first-event dedupe, monotonic time clamp — verify: against a local NATS, ingest publishes classified events to `events.*` and survives a reconnect with zero duplicate publishes (fixture replay test)
- [ ] 2.4 Implement `internal/health`: `/healthz`, `/readyz`, `/metrics` (prometheus: drop/publish/ack/error counters) wired into ingest and worker binaries — verify: `curl /healthz` and `/readyz` return 200 in-container; `/metrics` shows per-type counters

## 3. Workers

- [ ] 3.1 Implement `cmd/worker` generic pull-consumer: bounded concurrency, ack/nak with deferral, `MaxDeliver` → DLQ — verify: consumer test acks happy path, nacks transient failure, and lands poison messages in DLQ
- [ ] 3.2 Implement per-type logic: posts (keyword/language matcher + notification log, optional SQS sink), likes/reposts (in-memory rolling counters + threshold alert), follows (sliding-window burst detection, in-memory default, optional Redis), deletes (TTL tombstone set) — verify: one worker contract test consumes the fixture and asserts expected outcomes per type; `go test ./...` green
- [ ] 3.3 Implement graceful shutdown (stop pulling, ack in-flight, flush, persist cursor; `preStop` + `terminationGracePeriodSeconds` in manifests) — verify: SIGTERM during load drains in-flight without redelivery storm (log evidence + test)

## 4. k8s manifests

- [ ] 4.1 NATS StatefulSet + PVC + Service (optional NATS GUI/exporter) — verify: `kubectl apply` brings up NATS with a bound PVC; stream survives pod restart
- [ ] 4.2 `ingest` Deployment (replica=1, cursor volume) + per-type worker Deployments each with own HPA (JetStream queue-depth), resource limits, DLQ — verify: deploy to OrbStack k3s; killing one worker leaves the others flowing; HPA scales a worker on queue depth
- [ ] 4.3 (optional) Redis StatefulSet + PVC for follows-burst state — verify: follows worker switches to Redis-backed burst state with no behavior change
- [ ] 4.4 (optional) LocalStack for posts→SQS notification sink — verify: keyword-hit post publishes a message to SQS in LocalStack

## 5. Tests and docs

- [ ] 5.1 Complete meaningful test suite: classify table-driven (all route + drop cases), broker dedupe under redelivery, one worker contract (fixture → expected outcomes) — verify: `go test ./...` green; tests cover the graded routing surface
- [ ] 5.2 Write `DESIGN.md`: architecture, router decision + alternatives (Kafka/NATS/SQS), per-type ordering contract, backpressure/DLQ, AWS-parity production path, scale & fail isolation, measured numbers from 1.2 — verify: DESIGN.md addresses every PLAN §5.2 bullet
- [ ] 5.3 Write `README.md` (build, deploy, see-it-work incl. "ingest → NATS GUI shows events.* moving" and "kill a worker" demo) and `AI.md` (setup, model config, subagent prompts behind key decisions, honest "where agent helped / where I overrode") — verify: a fresh reader can deploy and observe the pipeline from README alone
- [ ] 5.4 Write/refresh `AGENTS.md` (repo contract: build/test discipline, PLAN pointer, commit conventions) and keep this change's spec/task targets authoritative — verify: AGENTS.md committed and paths match the repo layout
