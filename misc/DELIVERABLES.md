# DELIVERABLES.md — one-page companion to the assignment

Concise companion to `timeline.md`: every required deliverable, what the spec asked, where it lives,
the two-minute talking point, and the honest note. Times all EDT; evidence rails in `DOCS-REVIEW-LEDGER.md`.

**Process in one line:** plan from the real stream's docs → capture real data once (committed fixture) →
build the Go router over NATS JetStream with per-type workers → verify live on k3s (kill-a-worker) →
make it portable → audit every doc claim against both session transcripts → fix what the docs claimed
but didn't do.

---

## 1 · The Go service — the repo itself (the artifact)

**Spec:** consume a busy stream, classify each event, fan out to type-specific workers that run/fail/scale
independently. Runs on local k3s via Dockerfile + manifests.

- `cmd/ingest` (replica=1) → `internal/classify` **the router** → NATS JetStream `events.<type>` →
  five `cmd/worker` Deployments (posts / likes / reposts / follows / deletes), each own HPA + DLQ.
- `Dockerfile` (multi-stage, one image, k8s picks the binary) + `k8s/` (NATS StatefulSet+PVC, ingest,
  5 workers, optional `hpa.yaml`, `load-image.sh`). Verified on OrbStack k3s; runs k3s.io/kind/minikube.
- **Talking point:** the graded core is the pure-Go routing table — NATS is the fanout fabric, never the
  routing brain (EventBridge-as-router would have outsourced exactly what's being evaluated).
- **Honest:** cluster was torn down after verification; README reproduces it step-for-step.

## 2 · README.md — build, deploy, see it work, assumptions

**Spec:** exact build-and-deploy steps, how to see it working, and the assumptions you made.

- Build & test · Local demo (host NATS, **deterministic** fixture replay → exact alert counts
  3,309/41/3/1) · Cluster demo (kind/k3s.io/OrbStack paths) · CLI contracts · **`## Assumptions`** ·
  Known behaviors & limits.
- **Talking point:** the demo is reproducible run-to-run off the committed fixture — no live firehose
  needed to see it work; cluster path adds the kill-a-worker isolation demo.

## 3 · DESIGN.md — the graded centerpiece (1–3 pages)

**Spec:** architecture, the decisions behind it, alternatives weighed, and how you'd take it from
prototype to production.

- §1 architecture → §2 router decision + 4 rejected alternatives → §3 exactly-once-ish ingest →
  §4 per-type ordering contract → §5 backpressure/DLQ → §6 AWS parity + **prototype→production
  hardening** → §7 scale & fail isolation + measured table. ~1,564 prose words.
- **Decisions to defend (one line each):** router in Go / NATS as fabric · one connection-one cursor,
  ingest replica=1 · wantedCollections trims commits but identity/account always arrive (classified
  and dropped in Go) · per-type ordering contract (commutative vs window), never cross-type ·
  fixture-as-contract (every number independently re-verified).
- **Honest:** every measured number was re-derived from `sample.jsonl` during the audit — none rest on
  the author's say-so.

## 4 · AI.md — tools, models, prompts, provenance

**Spec:** tools/models/config that shaped output, prompts behind the important decisions, and "a few
honest lines on where the agent helped and where you overrode it."

- Setup: **observed** models from the transcripts (deepseek-v4-flash-0731 + latest; the claude-sonnet-5
  role map is config intent only), research via context-mode doc fetches, worktree subagent lanes.
- 7 representative prompts mapped to the decisions they drove (brief → options → grounding-facts →
  review gate → plan → handoff → implementation lanes).
- Overrides split into: **data-derived** (the fixture/stream decided) · **integration catches**
  (tests/live deploy surfaced) · **human overrides** (human steered).
- **Honest:** the model claim in this file was wrong until the transcripts corrected it; the "7 %
  headline dropped" claim was proven false by the audit.

## Tests — a couple of meaningful ones

`classify` table-driven router tests · broker dedupe under redelivery (embedded nats-server) ·
worker contract test against the fixture (3,309 alerts). Hermetic, no network:
`go test ./... -race ./internal/workers/`.

---

## Supporting artifacts (not required, loaded for review depth)

| File | Purpose |
|---|---|
| `misc/PLAN.md` | the plan the implementation session executed (phases, scope control, OpenSpec decision) |
| `throughput.md` | measured fixture facts behind every DESIGN number |
| `misc/DOCS-REVIEW-LEDGER.md` | the two-history audit: claim → evidence → verdict (~30 claims) |
| `misc/timeline.md` | full process story with timestamps + presentation cheat-sheet |
| `AGENTS.md` | repo contract the build sessions followed |
| `openspec/` | the spec-driven change (proposal → design → specs → tasks) — the authoritative task/verification ledger |

## 60-second pitch

*One Go process holds the single ordered Jetstream connection, decodes each frame once, and routes it by
type through a pure-Go table — delete-first, unknown→observable-drop. Five independent workers each pull
their own durable NATS JetStream subject with bounded concurrency, retry, and dead-lettering, so one slow
type can never starve another. We traced the design to real stream facts on day one, captured a committed
60-second fixture as the contract, verified live on k3s including a kill-a-worker demo, then made the
deployable portable to any local k8s. The docs are audited: every number re-verified against the fixture,
every claim in AI.md checked against the actual session transcripts — including the ones that were wrong
until the audit caught them.*

---

*Cross-checked against `timeline.md` and `DOCS-REVIEW-LEDGER.md`; both available on request.*