# AI.md — how this repo was built, honestly

This file documents where AI agents contributed, what I (the human developer) verified, and how to
reproduce the collaboration — **including the planning phase that preceded the implementation session.**
It's written so a grader can tell exactly what is agent-generated, what is evidence-derived from the
data, and what residual risk remains.

## Setup — models, tools, and configuration that shaped the design

Two phases built this repo. The design decisions came from the **planning phase**; the code, tests,
manifests, and docs came from the **implementation phase**. Both used the pi agent harness.

**Agent harness and models**

- Harness: **pi** (earendil-works/pi), interactive dashboard sessions. Provider: OpenRouter.
- **Observed models (authoritative — read from the session transcripts):** the implementation
  sessions (impl #2 + its resume, impl #3) ran `deepseek/deepseek-v4-flash-0731` (223 message-model
  rows) plus `~deepseek/deepseek-v4-flash-latest` (2); the planning sessions ran
  `~deepseek/deepseek-v4-flash-latest`. **No audited transcript ran claude-sonnet-5.**
- Role map (config intent only): `~/.pi/agent/providers.json`, preset `default`, *configures*
  `planning`/`research` → `openrouter/anthropic/claude-sonnet-5`, `coding`/`fast`/`compact`/`naming`
  → `openrouter/~deepseek/deepseek-v4-flash-latest`, `vision` →
  `openrouter/google/gemini-3.6-flash` — but that is what the roles *point at*, not what ran in
  any audited session. Report from transcripts / `PI_MODEL`, not `list_roles`. (An earlier draft
  claimed "Claude Sonnet-class model"; the transcript-verified statement is the observed-model
  line above.)

**Tools and MCP servers that did the real research**

- **context-mode** (MCP): `ctx_fetch_and_index` + `ctx_search` pulled and grounded the Bluesky
  Jetstream docs (wantedCollections semantics, inclusive cursor, delete event shape) and NATS
  material; `ctx_index` persisted the plan for the handoff. Every doc-grounded claim in PLAN.md and
  DESIGN.md traces to one of these fetches — nothing was asserted from memory.
- **pi-subagents**: implementation-phase worktree fanout (three parallel lanes: workers, k8s, DESIGN).
- **ask_user**: the architecture decision was a human choice, not an implicit agent pick.
- **canvas**: pinned PLAN.md as the handoff artifact for the implementation session.
- **bash/git/python**: fixture capture, offline analysis, `kubectl`, `git` discipline.

**Configuration that shaped output**

- `.pi/settings.json` is empty (`{}`) — no per-project prompt overrides. There is **no**
  CLAUDE.md/.cursorrules anywhere in the repo. The operative rules live in the repo itself:
  `AGENTS.md` (build/test discipline, "routing stays in Go", fixture fidelity) and `PLAN.md`
  (scope-control table, Phase 0–5). The assignment PDF and the `sample-k8s` scaffold
  (establishing the OrbStack/k3s workflow) were inputs to planning.
- Openspec skills (openspec-propose / update / apply / archive) drove change tracking
  (`openspec/changes/jetstream-fanout-processor`). None of these altered model output; they are
  workflow, not system prompts.

## The prompts that did the main work (representative)

A representative selection, mapped to the decision each one drove. Full lane prompts live in the
commit messages (one logical step per commit, message states what and why).

1. **Assignment brief** (user → planning): *"…a event stream processor by event type… Go service
   controlling the fanout… talking points: high volume fire hose / fast distribution per event type
   (select a solution) / consumer options for high volume / are there images that work with k3s that
   implement SQS, EventBridge?"* → drove the options pass (LocalStack · ElasticMQ · NATS JetStream ·
   Kafka/Redpanda) and the "who owns the router" fork (EventBridge-as-router outsources exactly what
   is being evaluated).
2. **Architecture selection** (ask_user → human): options A (pure in-Go fanout) · B (Go ingest +
   NATS JetStream + per-type worker Deployments) · C (LocalStack EventBridge as router) · D
   (LocalStack as downstream sink) → chose **B**, with "routing stays in Go either way" held as a
   constraint. This is the single biggest decision in the repo.
3. **Grounding-facts research** (planning agent): verified against live docs that *wantedCollections
   filters commit events only*, so identity/account/sync are always delivered regardless → why the
   ingest drops identity/account *at publish* and why they are first-class route targets, never WS
   filters; cursor is *inclusive* → dedupe the first event after every resume; delete commits carry
   **no record/cid** → the deletes worker is a key-only tombstone/cleanup path; throughput is order
   low-thousands/s with spikes to tens of thousands → "the WS read is not the bottleneck; the fanout
   design is."
4. **Review gate** (user): *"DO NOT START IMPLEMENTATION — just review and provide feedback."* →
   produced the design review whose key push-back was scope control: the general Redis read-model was
   cut to "one worker's small state (follows burst), optional" (later removed end-to-end, commit
   68f2bdd).
5. **Plan generation** (ask_user → human): *"Turn this into a full implementation plan doc (phases,
   tasks, repo layout, deliverable mapping)"* → produced PLAN.md (Phase 0–5, scope-control table,
   deliverable→task mapping).
6. **Handoff contract** (user): *"keep this context to plan only, new session for implementation."* →
   PLAN.md declared the handoff artifact; the implementation session consumed PLAN.md + openspec and
   captured the committed fixture (`testdata/sample.jsonl`, 42,101 messages) that every verification
   number in this repo derives from.
7. **Implementation lanes** (implementation session): one bounded prompt per lane (fixture analysis /
   router+broker+ingest / workers / k8s / docs), each pointed only at PLAN.md, the openspec artifacts,
   and the fixture — never at the live firehose.

## What the agent produced (verified before commit)

| Artifact | How verified |
|---|---|
| `internal/classify` router (tasks 2.1) | table-driven tests; unknown→Drop; delete-first precedence |
| `internal/broker` JetStream layer (2.2) | hermetic embedded-nats tests: MsgId dedupe, consumer filter isolation, DLQ |
| `cmd/ingest` (2.3) | fixture replay → 42,101 messages, exactly 42,186−85 identity/account (live check) |
| `internal/health`, `internal/metrics` (2.4) | `/healthz` `/readyz` `/metrics` on both binaries |
| `internal/workers` + `cmd/worker` (3.1–3.3) | live demo counts matched fixture-computed expectations **exactly** (3309/41/3/1), DLQ + race-safe tests |
| `k8s/` + `Dockerfile` (4.1–4.4) | `kubectl apply -k k8s/` on OrbStack k3s: pods 1/1, live kill-a-worker demo; portability pass (HPA split to optional `k8s/hpa.yaml`, portable image loader `k8s/load-image.sh`) |
| `DESIGN.md` (5.2) | every number re-verified against `testdata/sample.jsonl` independently |
| `throughput.md` (1.1) | offline deterministic analysis of the committed fixture |

Concurrency note: the worker, k8s, and DESIGN lanes were run as **three parallel subagents, each in its
own git worktree** off clean HEAD, writing disjoint paths (`internal/workers`+`cmd/worker`,
`k8s`+`Dockerfile`, `DESIGN.md`). Patches were reviewed and applied by me after the lanes completed;
the full test suite and the live smoke went green **only after** reconciliation.

## Where decisions came from — and where I overrode the agent

### Decisions that came from the stream/fixture analysis (data, not taste)

These look like "overrides" in an earlier draft of this section, but they are not: each one is a
**consequence of the committed 60 s sample or the documented stream semantics**, surfaced by exactly
the "confirm dedupe heuristic / data-sync ordering / object schemas / per-type throughput" analysis
that Phase 1.2 of the plan called for. The data decided; nobody overrode anyone.

- **Cursor = last `time_us`, not `seq`.** The fixture shows commit events carry **no top-level `seq`**
  (identity/account nest one; commits don't), and the docs specify the resume cursor as a
  unix-microsecond timestamp. The cursor-by-`seq` idea died when checked against the data.
- **Inclusive-cursor first-event dedupe + monotonic time clamp.** The docs state the cursor is
  *inclusive — you'll receive event N itself again*; the broker implements server-side `Nats-Msg-Id`
  dedupe on top. Both are direct consequences of documented semantics, not opinion.
- **Deletes worker is a key-only tombstone/cleanup path.** Delete commits carry no
  record/cid — there was never anything to parse. (1,181 deletes / 42,101 = 2.8% of the fixture.)
- **Demo thresholds 20/20/5, not "production" 200/150/30.** The fixture's per-URI maxima are 80
  likes / 30 reposts / 24 follows, so production-looking defaults would fire **zero** alerts and the
  "see it work" demo would show nothing. Thresholds are calibrated to the fixture (commented in
  `k8s/workers.yaml`; reproduction table in README).
- **"deletes ~7%" corrected.** `throughput.md` §2 originally claimed deletes were ~7 % of commit
  volume; the fixture says 1,181/42,101 = 2.8 %. DESIGN.md carries the verifiable per-row counts.
  The two-history audit (`misc/DOCS-REVIEW-LEDGER.md`, F1) caught that the stale "~7 %" survived in
  `throughput.md` after the docs believed it dropped — §2 is now corrected. Again: PLAN.md's
  "verify every number against the fixture" rule, applied to the docs themselves.
- **identity/account drop-at-publish in Go.** `wantedCollections` cannot filter these kinds — they
  are always delivered. That's a documented property of the stream, so dropping them happens in the
  classifier (with a counter), never at the WS layer or in NATS.

### Integration catches (surfaced by tests / the live deploy, then baked in)

These are defects the *verification path* — hermetic tests or the live cluster demo — surfaced after the
agent's first cut, not decisions the human made in advance. The fix was accepted into the code.

- **DLQ namespace `events.dlq.<type>`** (not `events.<type>.dlq`): JetStream consumer filters are
  prefix-matching (`events.post` matches `events.post.dlq`), so the per-type DLQ must live under its
  own `events.dlq.*` prefix or a type's consumer would eat its own dead letters. Caught by the broker
  test; baked into the broker package comment.
- **Consumer bind via PullSubscribe options** — an `AddConsumer`-then-bind approach silently created an
  unfiltered consumer (caught by test). The worker binds with `nats.BindStream`, `AckExplicit`,
  `MaxDeliver(-1)`, explicit `MaxAckPending`.
- **ConnectRetry added after live deploy**: OrbStack k3s pod startup raced the NATS StatefulSet
  ("nats: no servers available"), so workers crash-looped once before converging. I added a bounded
  retry/backoff (`broker.ConnectRetry`) to both binaries — post-integration polish discovered by the
  live deploy, not by the lane briefs.

### Human overrides (deliberate choices that steered the agent)

These are the places where I made a call *against* what the agent was doing or about to do — not test
failures, but direction set by the human.

- **Cluster independence (later pass)**: when I flagged that "the recipient may not be using OrbStack,"
  HPA resources were split out of `k8s/workers.yaml` into optional `k8s/hpa.yaml` (the
  `HorizontalPodAutoscaler` kind needs the cluster's metrics-server component — not shipped by default
  k3s.io/kind), and `k8s/load-image.sh` was added so a locally built image loads into kind
  (`kind load`), k3s.io (`docker save | k3s ctr images import`), or OrbStack (no-op — shared Docker
  daemon). No manifest or code depends on OrbStack.
- **Fixture fidelity (plan-mandated discipline)**: subagents were pointed at `testdata/sample.jsonl`
  and the OpenSpec artifacts only; **no subagent was pointed at the live firehose** (capture happened
  once, committed raw). This rule came from PLAN.md, and I enforced it against the convenience of
  "just hit the live stream" during the build.

## Residual risk / not done

- In-memory handler state resets on worker restart (design-accepted: like/repost/follow windows and
  delete tombstones are observations, not durable facts — follows burst stays in-memory by design).
  No cross-type ordering promised. LocalStack sink is shipped as an optional manifest only. MsgId dedupe
  window (2 min) + inclusive-cursor dedupe gives exactly-once-ish, not exactly-once, delivery.
- The cluster demo was torn down (`kubectl delete -k k8s/`); README reproduces it.

## Reproduce

```sh
git log --oneline          # one logical step per commit, messages state what & why
go test ./... -race ./internal/workers/   # hermetic, no network
# local demo in README: docker NATS + ingest -replay + five workers → the exact alert table
```

Planning-phase reproduction: `misc/PLAN.md` contains the phases, scope-control table, and
   deliverable mapping the implementation session executed against; the Jetstream grounding facts in its
§0 trace to the context-mode doc fetches noted above.