# timeline-core.md — building the core deliverable, end to end

A presentation companion focused on the **core deliverable**: the Go service + k8s manifests + the
docs that ship with them. Covers brief → plan → build → live verification. (The later doc-audit
phase that reviewed these deliverables lives in `DOCS-REVIEW-LEDGER.md` / `timeline.md`.)

**Timezone:** all times **EDT (−0400)** — the machine TZ. Session transcripts store UTC (converted
−4 h); git commits store `-0400`, so the rails are consistent. Evidence keys: sessions (`01a11e37` =
planning, `01a12175` = impl #1, `01a121d8` = impl #2), git SHAs, `subagent-artifacts/`.

> Files named bare (`README.md`, `DESIGN.md`, `AI.md`, `AGENTS.md`, `throughput.md`) live in the
> repo root; `misc/` holds PLAN.md, the ledger, `timeline.md`, and `DELIVERABLES.md`.

> One-line arc: **plan from the real stream's docs → capture real data once (committed fixture) →
> build the router in Go over NATS JetStream with per-type workers → verify live on k3s
> (kill-a-worker) → make it portable — in ~8 h, two failed-attempt lessons included.**

---

## Phase 0 — Orientation (Oct 8)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| ~16:00 | Local-k8s primer: explored OrbStack k3s (`orb start k8s`) via the provided `sample-k8s` scaffold — compose↔manifest mapping, port-forward workflow | session `01a11c3f` |

Output: the k3s command surface (`kubectl apply -k`, `port-forward`, `scale/rollout`) the deliverable
is written against.

---

## Phase 1 — Planning (Oct 8 23:13 → Oct 9 12:17)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| Oct 8 23:13 | **Assignment brief**: event-stream processor in Go on local k3s; talking points — firehose volume, fast per-type distribution, high-volume consumer options, "are there k3s images that implement SQS/EventBridge?" | session `01a11e37` |
| Oct 9 00:00–11:30 | Research + options: **LocalStack** (yes — SQS/EventBridge on k3s, ~1 GB) vs ElasticMQ (SQS-only, tiny) vs **NATS JetStream** (~20 MB, subjects=event types, durable pull consumers) vs Kafka/Redpanda (~1 GB+/node). Grounded against live Jetstream docs: `wantedCollections` filters commit events **only** (identity/account/sync always delivered); the resume cursor is **inclusive** (you re-receive event N); delete commits carry **no record/cid** | planning context; PLAN.md §0 |
| ~11:30 | Architecture decided (human, via ask_user): **B — Go ingest + NATS JetStream fanout + per-type worker Deployments**, with "the router stays in Go" as a hard constraint; LocalStack = parity *story*, not the router | this context |
| 11:52 | Implementation direction + "review and provide feedback, don't implement yet" → scope control: **Redis cut (later removed entirely); identity/account handled at consumer level; guard against EventBridge-as-router** | session `01a11e37` |
| 12:04 | **`PLAN.md` written**: grounding facts, architecture, repo layout, Phase 0–5 tasks with time budgets, scope-control table, deliverable→task mapping | file mtime `Oct 9 12:04:30` |
| 12:17 | **Handoff**: "keep this context plan-only; new session for implementation" — PLAN.md declared the handoff artifact | session `01a11e37` |

Locked design (never re-litigated): routing table in Go (`kind, collection, operation → Type`,
delete-first, unknown→Drop), subjects `events.<type>`, per-type Deployments + HPA + DLQ, ingest
replica=1 (single ordered WS cursor), fixture-as-contract.

---

## Phase 2 — Implementation (Oct 9 12:19 → 19:58)

### 2a. First window — capture + derail (12:19–13:40)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| 12:19 | Impl #1 begins: "Take a look at PLAN.md and begin implementation" | session `01a12175` |
| 12:32 | Tasked with the bounded ~60 s Jetstream capture | session `01a12175` |
| 13:10 | **Capture commit `f78c6e2`** — bounded capture → verbatim `testdata/sample.jsonl` (42,186 events) + reusable WS framing. *The one durable artifact of this window; every number in the repo derives from it.* | git `f78c6e2` |
| 13:30–13:40 | Agent drifts into openspec scaffolding, stops reporting progress → user: "are openspec features tracked?", "give me a status", **"stop"**. **Failed session; its only surviving output is the capture fixture** | session `01a12175` |

### 2b. Rebuild + build-out (14:06–16:16)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| 14:06 | Impl #2 starts: "clean up previous failed work, start fresh with Task 0.1" | session `01a121d8` |
| 14:22 | Clean baseline + OpenSpec scaffold `3f80c80` | git |
| 14:28 | User: "Was the analysis documented, I only see a sample.jsonl?" → analysis must be a committed artifact | session `01a121d8` |
| 14:33 | **`throughput.md` `e3ac17e`** — offline fixture analysis: per-collection schemas, dedupe-key confirmation (42,101 unique, 0 dups), `seq` findings, per-type rates | git |
| 14:55–15:00 | Router `internal/classify`, JetStream broker, health/metrics `b3c16c2`; ingest `cmd/ingest` `4a67ed7` (decode-once, cursor resume, replay) | git |
| 14:56–15:02 | User directs fanout: "look for fanout opportunities for subagents… leverage git work trees" | session `01a121d8` |
| 15:15–15:39 | **Three parallel subagent lanes** in git worktrees (workers / k8s+manifests / DESIGN) — lane transcripts in `subagent-artifacts/`; patches human-reviewed before merge | `subagent-artifacts/` |
| 15:40–16:16 | k8s manifests authored → batched commits 16:16: workers `7edc56a`, manifests `769677a`, DESIGN `b308c70`, ConnectRetry fix `73d4e37`, docs `a1ec7de` | git |

### 2c. Live verification + iteration (16:16–19:58)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| ~16:30–16:48 | Live cluster demo on OrbStack k3s: `kubectl apply -k`, kill-a-worker test (durable consumer resumed, no redelivery storm), /healthz probes | README "verified live"; session `01a121d8` |
| 16:48–16:56 | User feedback: **"lets remove redis as it's not going to be utilized"**, kill-a-worker + health-check questions | session `01a121d8` |
| 17:01 | Redis removed end-to-end `68f2bdd` (in-memory state by design) | git |
| 19:10 | User probes: kubelet on vanilla clusters? ack-before-or-after-kill? → ack-after-commit semantics confirmed + documented | session `01a121d8` |
| 19:52 | **Portability requirement**: "make sure the deliverables work with normal Docker with k3s — the recipient may not be using OrbStack" | session `01a121d8` |
| 19:56–19:58 | Docs finalized (README/DESIGN/AI/AGENTS mtimes 19:55–19:57) → **portability commit `73b6847`**: HPA split to optional `k8s/hpa.yaml` (needs metrics-server), `k8s/load-image.sh` (kind / k3s.io / OrbStack), README/AI/AGENTS generalized | git |

Build closed Oct 9 with 12 commits, one logical step each, tests green, live demo reproduced
(3,309 / 41 / 3 / 1 alert counts; likes delivered_seq 207,895 → 232,266 across a killed pod,
`num_redelivered=0`).

---

## Deliverable state at handoff (Oct 9 19:58)

| Deliverable | State at close | Where |
|---|---|---|
| Go service — ingest → router → 5 per-type workers | complete, tests green, live-verified | repo root |
| Dockerfile + k8s manifests | applied on OrbStack k3s; portable (kind/k3s.io/OrbStack) | `k8s/`, `Dockerfile` |
| README — build/deploy/see-it-work | finalized 19:56 | `README.md` |
| DESIGN.md — architecture/decisions/alternatives | finalized 19:55 | `DESIGN.md` |
| Tests — router, dedupe, worker contract | hermetic, `go test ./...` green | `internal/*_test.go` |
| AI.md / AGENTS.md / throughput.md / openspec | finalized 19:57 | repo root |

---

## Talking points (defendable, evidence-backed)

1. **Router in Go, NATS as fabric** — the graded artifact is the routing kernel; NATS gives durability,
   per-type subjects, DLQ, HPA inputs in ~20 MB — Kafka/Redpanda is ~1 GB+/node, EventBridge-as-router
   outsources exactly what's being graded.
2. **One connection, one cursor, replica=1** — Jetstream's ordered WS cursor can't cleanly shard across
   pods; measured ~470–700 ev/s is trivial for one Go process; backpressure belongs downstream.
3. **wantedCollections trims what we can; identity/account always arrive** (~1/s floor) and must be
   classified-and-dropped in Go, never filtered at the socket.
4. **Fixture-as-contract** — capture once (committed `sample.jsonl`), analyze offline, replay
   deterministically; no subagent ever hit the live firehose.
5. **Per-type ordering contract, not a bus guarantee** — posts/likes/reposts/deletes commutative;
   follows window-based; no cross-type ordering promised; DLQ under `events.dlq.*` so consumers never
   eat their own dead letters.

**Numbers:** 42,186 events / 42,101 commits / 37 identity + 48 account / 16,344 DIDs / 1,181
record-less deletes / 2.8 % deletes / likes 68 % of commits / rates 470→700 ev/s /
3,309+41+3+1 alerts / max 80 likes, 30 reposts, 24 follows per URI.

**Honest lowlights (say them first):** impl #1 derailed and was discarded (its only survivor: the
capture fixture); initial agent output drifted into openspec scaffolding instead of building; Redis
was added then removed after review ("not going to be utilized"); live-demo numbers rest on a commit
trail, not a saved log excerpt.

---

*Evidence rails: session JSONLs under `~/.pi/agent/sessions/--Users-nunez-Work-assessments-zenity--/`,
`git log` in this repo, subagent lane transcripts, and artifact mtimes.*