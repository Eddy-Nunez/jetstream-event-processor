# timeline.md — the process, end to end

A presentation companion for the Zenity take-home: how this assignment went from brief → plan →
build → verify → docs audit, with timestamps, evidence, and the honest lowlights.

**Timezone:** all times below are **EDT (−0400)** — the machine TZ. Session transcripts are stored UTC
(converted −4 h); git commits are stored `-0400`, so the rails are consistent. Evidence keys:
sessions (`01a11e37` = planning, `01a12175` = impl #1, `01a121d8` = impl #2, `01a12cff` = impl #3
resume, `01a12d18` = this docs/audit session), git SHAs, and `DOCS-REVIEW-LEDGER.md`.

> One-line arc: **plan carefully (2 sessions, ~2 h) → brief AI to capture real data once → build the
> router in Go over NATS JetStream with per-type workers → kill-a-worker verify on k3s → make it
> portability-clean → then audit every claim in the docs against both transcripts and fix what the
> docs claimed but didn't do.**

---

## Phase 0 — Orientation (Oct 8)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| ~16:00 | Local-k8s primer: explored OrbStack k3s (`orb start k8s`) via the provided `sample-k8s` scaffold — compose↔manifest mapping, port-forward workflow | session `01a11c3f` |

Output: the k3s command surface (`kubectl apply -k`, `port-forward`, `scale/rollout`) that the whole
deliverable is written against.

---

## Phase 1 — Planning (Oct 8 23:13 → Oct 9 12:17)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| Oct 8 23:13 | **Assignment brief**: event-stream processor in Go on local k3s; talking points — firehose volume, fast per-type distribution, high-volume consumer options, "are there k3s images that implement SQS/EventBridge?" | session `01a11e37` |
| Oct 9 00:00–11:30 | Research + options: **LocalStack** (yes — SQS/EventBridge on k3s, ~1 GB) vs ElasticMQ (SQS-only, tiny) vs **NATS JetStream** (~20 MB, subjects=event types, durable pull consumers) vs Kafka/Redpanda (~1 GB+/node). Grounded against live Jetstream docs: `wantedCollections` filters commit events **only** (identity/account/sync always delivered); the resume cursor is **inclusive** (you re-receive event N); delete commits carry **no record/cid** | planning context; §0 of PLAN.md |
| ~11:30 | Architecture decided (human, via ask_user): **B — Go ingest + NATS JetStream fanout + per-type worker Deployments**, with "the router stays in Go" as a hard constraint; LocalStack = parity *story*, not the router | this context |
| 11:52 | Implementation direction notes + "DO NOT START IMPLEMENTATION — review and provide feedback" → scope control: **Redis cut to one worker's small state (later removed entirely); identity/account handled at consumer level; guard against EventBridge-as-router** | session `01a11e37` |
| 12:04 | **`PLAN.md` written**: grounding facts, architecture, repo layout, Phase 0–5 tasks with time budgets, scope-control table, deliverable→task mapping | file mtime `Oct 9 12:04:30` |
| 12:17 | **Handoff**: "keep this context plan-only; new session for implementation" — PLAN.md declared the handoff artifact, indexed, canvas-pinned | session `01a11e37` |

Design decisions locked here and never re-litigated: routing table in Go (`kind, collection, operation →
Type`, delete-first, unknown→Drop), subjects `events.<type>`, per-type Deployments + HPA + DLQ, ingest
replica=1 (single ordered WS cursor), fixture-as-contract.

---

## Phase 2 — Implementation (Oct 9 12:19 → 19:58)

### 2a. First window — capture + derail (12:19–13:40)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| 12:19 | Impl #1 begins: "Take a look at PLAN.md and begin implementation" | session `01a12175` (meta: deepseek-v4-flash-latest) |
| 12:32 | Tasked with Task 0.x — the bounded ~60 s Jetstream capture | session `01a12175` |
| 13:10 | **Capture commit `f78c6e2`** — bounded capture → verbatim `testdata/sample.jsonl` (42,186 events) + reusable WS framing. *The one durable artifact of this window; every number in the repo derives from it.* | git `f78c6e2` |
| 13:30–13:40 | Agent drifts into openspec scaffolding, stops reporting progress → user: "are openspec features tracked?", "give me a status", **"stop"**. **Failed session, excluded** from evidence | session `01a12175` |

### 2b. Rebuild + build-out (14:06–16:16)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| 14:06 | Impl #2 starts: "clean up previous failed work, start fresh with Task 0.1" | session `01a121d8` (223× deepseek-v4-flash-0731 + 2× latest) |
| 14:22 | Clean baseline + OpenSpec scaffold `3f80c80` | git |
| 14:28 | User: "Was the analysis documented, I only see a sample.jsonl?" → analysis must be a committed artifact | session `01a121d8` |
| 14:33 | **`throughput.md` `e3ac17e`** — offline fixture analysis: per-collection schemas, dedupe-key confirmation (42,101 unique, 0 dups), `seq` findings, per-type rates | git |
| 14:55–15:00 | Router `internal/classify`, JetStream broker, health/metrics `b3c16c2`; ingest `cmd/ingest` `4a67ed7` (decode-once, cursor resume, replay) | git |
| 14:56–15:02 | User directs fanout: "look for fanout opportunities for subagents… leverage git work trees" | session `01a121d8` |
| 15:15–15:39 | **Three parallel subagent lanes** in git worktrees (workers / k8s+manifests / DESIGN) — lane transcripts in `subagent-artifacts/`; patches reviewed by the human before merge | `subagent-artifacts/`, session `01a121d8` |
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

## Phase 3 — Doc provenance & audit (Oct 11 18:05 → 19:21)

| Time (EDT) | What happened | Evidence |
|---|---|---|
| 18:05 | Impl #3 resume session (context carried forward; portability-finalized repo) | session `01a12cff` |
| 18:32 | Docs/audit session (this one) begins — two histories now both reachable: planning + implementation transcripts on disk | session `01a12d18` |
| 18:40 | First AI.md revision: split "data-derived decisions" from "genuine human overrides"; backup `AI.md.bak` | file backstop |
| 19:04 | **`DOCS-REVIEW-LEDGER.md` written** — claim → evidence → verdict audit across all six docs + openspec: ~20 ✅ / 6 ⚠️ / **3 ❌** → **F1** (throughput.md stale "deletes ~7 %" ≠ its own 2.8 %), **F2** (AI.md claimed claude-sonnet-5; *no transcript ran it* — observed deepseek-v4-flash-0731/latest), **F3** (AI.md claimed the 7 % headline was dropped; the audit proved it wasn't) | `DOCS-REVIEW-LEDGER.md` |
| 19:04–19:05 | Fixes applied: F1 throughput.md → `2.8 %`; F2 AI.md → observed-models truth; F3 AI.md → "corrected" wording | backups `*.bak-20261011-190437` |
| 19:10 | AI.md overrides section split further: **Integration catches** (test/deploy-surfaced) vs **Human overrides** (human-steered) | git status |
| 19:19–19:21 | **Alignment pass against the deliverable spec**: README gains `## Assumptions` (was: zero mentions); DESIGN gains `From prototype to production` (hardening: durability/exactly-once/security/observability/drift/rollout) and stays within the 1–3 pp budget (~1,564 prose words) | backups `*.bak-20261011-191903` |

Audit outcome: **every** DESIGN.md number re-verified against fixture + transcripts; F1–F3 were the only
required doc changes, all applied with backups.

---

## The presentation cheat-sheet

**Five decisions to be ready to defend:**
1. **Router in Go, NATS as fabric** — the graded artifact is the routing kernel; NATS gives durability,
   per-type subjects, DLQ, HPA inputs in ~20 MB — Kafka/Redpanda is ~1 GB+/node, EventBridge-as-router
   outsources exactly what's being graded.
2. **One connection, one cursor, replica=1** — Jetstream's ordered WS cursor can't cleanly shard across
   pods; measured ~470–700 ev/s is trivial for one Go process; backpressure belongs downstream.
3. **wantedCollections trims what we can; identity/account are uncabled facts** — they always arrive
   (~1/s floor) and must be classified-and-dropped in Go, never filtered at the socket.
4. **Fixture-as-contract** — capture once (committed `sample.jsonl`), analyze offline, replay
   deterministically; no subagent ever hit the live firehose. Every number in DESIGN/README is
   independently re-verified against it.
5. **Per-type ordering contract, not a bus guarantee** — posts/likes/reposts/deletes commutative;
   follows window-based; no cross-type ordering promised; DLQ under `events.dlq.*` so consumers never
   eat their own dead letters.

**Numbers to have at hand:** 42,186 events / 42,101 commits / 37 identity + 48 account / 16,344 DIDs /
1,181 record-less deletes / 2.8 % deletes / likes 68 % of commits / rates 470→700 ev/s / 3,309+41+3+1
alerts / max 80 likes, 30 reposts, 24 follows per URI.

**Honest lowlights (say them first):** impl #1 derailed and was discarded (its only survivor: the
capture); the "deletes ~7 %" claim survived in throughput.md after docs believed it dropped — the audit
caught the docs contradicting themselves; the model line in AI.md was wrong until the transcripts
corrected it; live-demo numbers rest on a commit trail, not a saved log excerpt.

**Deliverable map:** `README.md` build/deploy/see-it-work/assumptions · `DESIGN.md` architecture/
decisions/alternatives/production path (1–3 pp) · `AI.md` honest provenance · `AGENTS.md` repo contract ·
`throughput.md` measured facts · `misc/DOCS-REVIEW-LEDGER.md` claim→evidence audit · `misc/PLAN.md` the plan ·
`misc/timeline.md` this file.

---

*Evidence rails: session JSONLs under `~/.pi/agent/sessions/--Users-nunez-Work-assessments-zenity--/`,
`git log` in this repo, subagent lane transcripts, and artifact mtimes — all cross-checked in
`DOCS-REVIEW-LEDGER.md`.*