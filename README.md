# eventworker — Jetstream fan-out processor

A **Zenity Senior SWE take-home**: a Bluesky Jetstream fan-out processor in Go.
The graded core is the **routing kernel** — `ingest → classify → per-type fanout → independent workers` —
implemented as a pure Go routing table, with NATS JetStream as the durable fan-out fabric.

Read `DESIGN.md` for the full architecture rationale (router decision + alternatives, ordering
contract, exactly-once-ish ingest, backpressure/DLQ, AWS-parity path, measured numbers).

## Architecture (one paragraph)

`cmd/ingest` holds one ordered Jetstream WebSocket connection, decodes each frame once, and pushes it
through `internal/classify` — the router: `Classify(kind, collection, operation) → Type` with
delete-first precedence, identity/account first-class drop-by-policy, and unknown → observable `Drop`.
Each classified event is published to **one** of `events.post|like|repost|follow|delete` with
`Nats-Msg-Id = DedupeKey`, into a durable JetStream stream `EVENTS` (`events.>`). Five independent
`cmd/worker` deployments (one per type) are durable pull consumers with bounded concurrency, ack/nak
semantics, and `MaxDeliver → events.dlq.<type>` dead letters. Every pod serves `/healthz`, `/readyz`,
`/metrics` (`eventworker_*` Prometheus metrics).

```
Jetstream WS ──▶ cmd/ingest (replica=1)
                    │ decode-once → internal/classify (THE ROUTER)
                    │ cursor file (time_us, atomic) + Nats-Msg-Id
                    ▼
        NATS JetStream · EVENTS (events.>, file storage, 1h MaxAge)
                    │
        ┌───────┬────┼────┬───────┐
        ▼       ▼    ▼    ▼       ▼
    posts   likes  reposts follows  deletes      (cmd/worker × 5, own HPA + DLQ)
 events.dlq.<type>  (per-type dead letters, same stream)
```

## Repo layout

```
cmd/capture    Phase-1 bounded capture → testdata/sample.jsonl (done, stable)
cmd/ingest     WS → decode-once → classify → JetStream publish, cursor resume, -replay
cmd/worker     generic per-type pull consumer (-type posts|likes|reposts|follows|deletes)
internal/ingest  Jetstream WS client + frame splitting (reused, not rewritten)
internal/classify routing table — the graded artifact
internal/broker  JetStream stream, dedupe, pull consumers, DLQ
internal/workers per-type handlers (posts matcher, like/repost counters, follow burst, delete tombstones)
internal/health  /healthz /readyz /metrics
internal/metrics eventworker_* Prometheus counters/gauges
k8s/           namespace, NATS StatefulSet, ingest, 5 workers, optional hpa.yaml + localstack
Dockerfile     multi-stage; one image (eventworker:dev), k8s picks the binary
DESIGN.md      architecture + decisions (the graded centerpiece)
throughput.md  measured fixture numbers (42,186 events)
```

## Build & test

```sh
go build ./... && go vet ./... && gofmt -l .   # clean
go test ./... -count=1                          # hermetic: embedded nats-server + fixture tests
```

Requires Go ≥ 1.26. No live network needed for tests.

## Local demo (host NATS, deterministic)

1. Start a JetStream NATS:

   ```sh
   docker run -d --rm --name nats-demo -p 4222:4222 -p 8222:8222 nats:2.10-alpine -js -m 8222
   ```

2. Replay the committed fixture (42,186 events, ~4 s — the exact same classify→publish pipeline as live):

   ```sh
   go run ./cmd/ingest -replay testdata/sample.jsonl -nats nats://127.0.0.1:4222
   # "replay done  events=42186" — 42,101 committed + 85 identity/account dropped at publish
   ```

3. Start the five workers:

   ```sh
   go run ./cmd/worker -type posts   -nats nats://127.0.0.1:4222 -keywords test -langs en -http :18001
   go run ./cmd/worker -type likes   -nats nats://127.0.0.1:4222 -like-threshold 20  -http :18002
   go run ./cmd/worker -type reposts -nats nats://127.0.0.1:4222 -repost-threshold 20 -http :18003
   go run ./cmd/worker -type follows -nats nats://127.0.0.1:4222 -follow-burst 5     -http :18004
   go run ./cmd/worker -type deletes -nats nats://127.0.0.1:4222                       -http :18005
   ```

4. Observe `worker event=notify` lines — **these exact counts reproduce run-to-run** (verified live):

   | worker | config | alerts |
   |---|---|---|
   | posts | `-keywords test -langs en` | **3,309** |
   | likes | `-like-threshold 20` | **41** |
   | reposts | `-repost-threshold 20` | **3** |
   | follows | `-follow-burst 5` | **1** |
   | deletes | — | **0** (tombstones, never content-matched) |

   (The fixture's per-URI maxima are 80 likes / 30 reposts / 24 follows-per-target, so high thresholds
   like the defaults 200/150/30 fire zero alerts on this snapshot — tune per environment.)
   Counters are also visible at `:1800X/metrics` → `eventworker_alerts_total{type="..."}`.

5. Force a DLQ (kill a worker mid-flight): stop `worker-likes`, publish more likes, watch a restart —
   the durable consumer resumes where it left off; pending drains to `0` and nothing is lost or redelivered
   (verified: likes delivered_seq grew 207,895 → 232,266 across a killed pod, `num_redelivered=0`).

## Cluster demo (any local k8s: k3s.io, kind, minikube, OrbStack)

Verified on OrbStack k3s; the manifests use only standard k8s resources (Services,
StatefulSets, Deployments, PVCs, probes) plus one *optional* HPA file — so they run unchanged on
k3s.io (`k3s`), kind, minikube, or a real cluster.

```sh
docker build -t eventworker:dev .          # build wherever you have Docker
exec k8s/load-image.sh                      # make it visible to the cluster runtime:
                                           #   kind   → kind load docker-image
                                           #   k3s.io → docker save | k3s ctr images import
                                           #   OrbStack → no-op (shared Docker daemon)
                                           # (or docker save <img> | <node> ctr images import manually)
kubectl apply -k k8s/                      # namespace eventworker: NATS, ingest, 5 workers
                                           #   - remove `- hpa.yaml` here if your cluster lacks
                                           #     the metrics-server component (default k3s.io/kind);
                                           #     HPAs are optional, the core demo deploys without
kubectl get pods -n eventworker            # all 1/1 after ~20s (ConnectRetry absorbs the NATS startup race)
kubectl port-forward -n eventworker svc/nats 4222:4222 &
go run ./cmd/ingest -replay testdata/sample.jsonl -nats nats://127.0.0.1:4222   # from the host
kubectl logs -n eventworker deploy/worker-posts | grep 'event=notify' | wc -l    # 3309
kubectl delete pod -n eventworker -l app=worker-likes   # kill-a-worker demo: others keep flowing,
                                                         # replacement resumes the durable consumer
```

The cluster's `ingest` Deployment (replica=1, cursor PVC) connects to the **live** firehose
(`wss://jetstream2.us-east.bsky.network/subscribe`); in our verification it reached ~400 live events/s
with a monotonically advancing cursor. To demo offline only, first `kubectl scale deploy/ingest --replicas=0`
and use the host replay path above. Tear down with `kubectl delete -k k8s/`.

> HPA: `k8s/hpa.yaml` holds the 5 per-worker HorizontalPodAutoscaler resources. They need the cluster's
> metrics-server component (`k3s enable metrics-server` on k3s.io) — not available on default kind/plain k3s.
> They're CPU-based; production scales per-worker on NATS queue depth via a metrics adapter (see the
> comments in `k8s/hpa.yaml`). The core demo (Deployments + probes) does not depend on HPA.

## CLI contracts

`cmd/ingest`: `-url` (default live firehose), `-nats nats://127.0.0.1:4222`, `-stream EVENTS`,
`-maxage 1h`, `-cursor <path>` (atomic resume cursor), `-http :8080`, `-replay <file>` (offline),
`-publish-identity` (default off — identity/account dropped at publish).

`cmd/worker`: `-type posts|likes|reposts|follows|deletes` (required), `-nats`, `-stream`,
`-concurrency 16` (maxAckPending), `-maxdeliver 5`, `-http :8090`, `-durable EVENTS-<type>`,
`-keywords` (csv), `-langs` (csv), `-like-threshold 200`, `-repost-threshold 150`,
`-follow-burst 30`, `-window 60s`.

## Assumptions

- **One connection, one ordered cursor.** Ingest is a single WS connection (replica=1): a Jetstream
  cursor can't cleanly shard across pods, and measured throughput (~470–700 ev/s) is far below a single
  Go process's ceiling. Anything backpressure-worthy happens downstream, not at the socket.
- **`wantedCollections` trims commit traffic** to the demo collections; `identity`/`account`/`sync`
  frames always arrive regardless and are dropped at publish after counting (`eventworker_drop_total`).
- **Worker analytics state is intentionally in-memory.** Likes/reposts/follows windows and delete
  tombstones reset on restart — alerts are observations, not durable facts. Durability lives in the
  JetStream stream + durable consumers, which is what matters for a fan-out processor.
- **Local k3s means one NATS node, no HA.** Production would run a 3+ JetStream cluster; the manifests
  and code are topology-agnostic, so nothing changes except the broker config.
- **LocalStack is parity, not wiring.** The optional SQS/EventBridge sink is documented and ships as a
  manifest, but the demo runs with zero AWS dependencies.
- **Go ≥ 1.26 and a k8s runtime** (k3s.io / kind / minikube / OrbStack) are required to build and deploy;
  Docker is the only image-build prerequisite. HPA is optional (needs the cluster's metrics-server).
- **The fixture is the contract.** `testdata/sample.jsonl` is a one-off committed capture; tests and the
  deterministic demo replay it. The live feed is used only for the live cluster path, and only the harness
  in this repo touches it.
- **We process structure, not content.** Raw post text can contain anything; every path here consumes
  event metadata/schemas only, and the notification path emits a counter, never post text.
- **Ordering is per-type, deliberately.** No consumer may assume cross-type ordering (see DESIGN §4).

## Known behaviors & limits (honest)

- **In-memory handler state** (like/repost/follow windows, delete tombstones): a worker restart resets
  them — acceptable per design (alerts are observations, not durable facts).
- **Durable consumers are retained server-side** (`EVENTS-<type>`): existing config wins on restart.
- **LocalStack** (`k8s/optional-localstack.yaml`) is the documented posts→SQS parity sink; not wired.
- **Ingest is single-replica by design** (one ordered WS cursor can't shard; not the bottleneck at
  ~470–700 ev/s).
- Counter/pruning code amortizes full sweeps (every 4096 adds); per-key expiry is enforced on touch.

## Task status

All PLAN phases are implemented; authoritative tracking + verification notes live in
`openspec/changes/jetstream-fanout-processor/tasks.md`. `AI.md` documents how the agent collaborated
(and where I overrode it).
