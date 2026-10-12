# DOCS-REVIEW-LEDGER.md — claim → evidence → verdict

Two-history audit of the `eventworker` doc deliverables. Source histories:
**Planning** (this context, `~/.pi/agent/sessions/.../01a11e37`, `01a12d18`) and
**Implementation** (`01a121d8` = impl #2, `01a12cff` = impl #3; `01a12175` = impl #1, **excluded — failed**).
Evidence types: session transcript (timestamp), git commit (hash, date), subagent artifact, repo code/test, fixture.

Scale: ✅ VERIFIED (evidence in transcript/commit/artifact) · ⚠️ PARTIAL (commit/docs corroborate; not located verbatim in mined transcript) · ❌ CONTRADICTED (docs disagree with each other or evidence) · ⚪ UNVERIFIABLE (no trace in either history)

---

## 0. Session map & evidence inventory

| Session | Role | Model (observed) | What happened | Status |
|---|---|---|---|---|
| `01a11e37` (Oct 9 01:12Z) | Planning | `~deepseek/deepseek-v4-flash-latest` | assignment brief, PLAN.md (§0-6), options A–D | ✅ used |
| `01a12175` (Oct 9 16:19Z) | Impl #1 | `~deepseek/deepseek-v4-flash-latest` | **looped, drifted into openspec, user said "stop", 0 commits** | ❌ **excluded — failed** (user rename: "failed — implementation") |
| `01a121d8` (Oct 9 18:06Z = “impl #2”) | Implementation | `deepseek-v4-flash-0731` ×223, `~latest` ×2 | clean baseline → capture → analysis → build → live demo → Redis removal | ✅ gold mine |
| `01a12cff` (Oct 11 22:05Z = “impl #3”) | Implementation (resume) | same mix | portability pass, docs finalization | ✅ gold mine |
| subagent-artifacts/ (`71f3182c`, `a8ab0699`, `cb00fbbb`; 15:15–15:39 EDT) | Worktree lanes | — | 3 parallel lane transcripts (input/output/meta) | ✅ evidence |
| git `f78c6e2`…`73b6847` (Oct 9 13:10–19:58 EDT) | Commit trail | — | one step per task incl. exact worker alert counts | ✅ evidence |

**Timeline correction applied:** transcript timestamps are UTC (`Z`), commit dates are EDT (−4 h). Impl #2 spans 14:06→19:58 EDT and owns the build; no repo work claims are attributed to impl #1.

---

## 1. AI.md

| # | Claim (doc) | Evidence | Verdict |
|---|---|---|---|
| A1 | "model binding is role-based: planning/research → claude-sonnet-5…" | role map in `~/.pi/agent/providers.json` says so, **but observed model in both planning sessions and both impl sessions was deepseek-v4-flash variants; no transcript ran sonnet-5** | ❌ **CONTRADICTED — fix: report observed (`deepseek-v4-flash-0731` content, `~latest` planning), role map only as config intent** |
| A2 | "cursor = last `time_us`: fixture has no top-level seq; identity/account nest seq" | impl #2 18:30–31Z: `no_time_us=0  top_level_seq=0`; "identity/account frames showed zero seq (contradicts PLAN assumption)" | ✅ VERIFIED (data-derived, correctly labeled) |
| A3 | "dedupe key confirmed did\|collection\|rkey\|operation unique in-window (42,101, 0 dups)" | impl #2 18:31Z: "42,101 unique keys, zero dups" | ✅ VERIFIED |
| A4 | "deletes carry no record/cid (all 1,181 verified)" | impl #2 18:31Z: "deletes carry no record/cid (all 1,181 verified)"; throughput.md §3 | ✅ VERIFIED |
| A5 | "demo thresholds 20/20/5; fixture maxima 80/30/24" | commit `769677a` "thresholds calibrated to the fixture so a replay demo fires exact alert counts"; maxima in README — maxima numbers not located verbatim in mined transcript | ⚠️ PARTIAL |
| A6 | "'deletes ~7%' headline dropped (1,181/42,101 = 2.8%)" | **throughput.md §2 still says "deletes are ~7 %"**; its own table (1,181 = 2.8%) and DESIGN.md agree on 2.8% | ❌ **CONTRADICTED — the 7% headline was never actually removed from throughput.md** |
| A7 | "DLQ namespace `events.dlq.<type>` (prefix-match hazard)" | tasks 2.2 "consumer-filter-isolation test… rely on `events.dlq.*`"; broker test + package comment in code | ⚠️ PARTIAL (code+tasks agree; transcript string not in mined window) |
| A8 | "PullSubscribe bind vs AddConsumer (silent unfiltered consumer)" | tasks 2.2 + worker test; code | ⚠️ PARTIAL (same) |
| A9 | "ConnectRetry added after live deploy (NATS startup race)" | commit `73d4e37` "bounded broker connect retry… fixes k3s deployment startup race found during the live cluster demo" | ✅ VERIFIED |
| A10 | "HPA split → optional `k8s/hpa.yaml`; `load-image.sh` portable" | commit `73b6847` + user msg (impl #2, 23:52Z): "make sure the deliverables can work with normal Docker with k3s. Recipient may not be using OrbStack" | ✅ VERIFIED |
| A11 | "Redis removed end-to-end (commit 68f2bdd)" | commit `68f2bdd` "Remove Redis end-to-end…"; user msg (impl #2, 20:56Z): "lets remove redis as it's not going to be utilized" | ✅ VERIFIED |
| A12 | "three parallel subagents in git worktrees" | user msgs "look for fanout opportunities for subagents" + "For worktree isolation, leverage git work trees"; 3 lane artifact pairs in `subagent-artifacts/` (15:15–15:39 EDT) matching "Subagent updates above." (19:39Z) | ✅ VERIFIED (lane split workers/k8s/DESIGN = ⚠️ PARTIAL) |
| A13 | "no subagent pointed at live firehose; capture once" | commit `f78c6e2` "bounded Jetstream2 capture → verbatim .jsonl fixture"; commit `e3ac17e` "offline fixture analysis" | ✅ VERIFIED |

→ **Net AI.md changes:** fix model truth (A1), fix the "7% dropped" claim — either delete the line from throughput.md or rephrase (A6). Everything else verified; the data-derived vs human-override split stands.

---

## 2. DESIGN.md

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| D1 | 42,186 events; 42,101 commits (99.8%); 37 identity + 48 account; 16,344 DIDs; 1,181 deletes | impl #2 18:30Z: `events_total=42186 … kinds={'commit':42101,'account':48,'identity':37} unique_dids=16344`; throughput.md | ✅ VERIFIED |
| D2 | rates 470→700 ev/s; likes 320→479/s (68.2%) | impl #2 18:30Z: `overall_rate=470/s` + like `28720 (320/s)`; throughput.md §1-2 | ✅ VERIFIED |
| D3 | "time_us present on all events, strictly monotonic (0 regressions)" | impl #2 18:31Z: `no_time_us=0`; throughput.md §4 | ✅ VERIFIED |
| D4 | "no top-level seq; commits carry none" | impl #2 18:31Z: `top_level_seq=0` | ✅ VERIFIED |
| D5 | "42,101 keys, 0 dups" for dedupe | impl #2 18:31Z; throughput.md §4 | ✅ VERIFIED |
| D6 | 1 h MaxAge ≈ 2.5M msgs / ~1.7M likes | throughput.md §5 (computed from D2) | ✅ VERIFIED |
| D7 | "update = replace (10/10 bridgy)" | throughput.md §3 | ⚠️ PARTIAL (single-source: throughput.md) |
| D8 | Router decision / alternatives table | commit `b308c70` (Task 5.2 DESIGN.md) + `internal/classify` + openspec design D1-D6 | ✅ VERIFIED (code) |
| D9 | "delete-first, identity/account first-class, unknown → Drop" | classify tests + impl #2 claim-block [18:12Z] showing Type.Drop enum + route code | ✅ VERIFIED |

→ **Net DESIGN.md changes:** none required; all numbers independently re-verifiable. D7 acceptable as single-source fixture finding.

---

## 3. README.md

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| R1 | replay prints `events=42186`; 42,101 committed + 85 dropped | fixture + impl #2 18:22Z "42186 … 42101 37 48"; AI.md table (impl-v1) "42,101 committed + 85 identity/account" | ✅ VERIFIED |
| R2 | alert table 3309 / 41 / 3 / 1 exactly reproducible | commit `7edc56a` "fixture contract test (3309 alerts)"; README live verify note; impl-v1 AI.md "3309/41/3/1" | ⚠️ PARTIAL (3309 test-verified; 41/3/1 live-claimed — counts shown in commit/README, not in mined transcript window) |
| R3 | kill-a-worker: delivered_seq 207,895→232,266, num_redelivered=0 | README + live demo per user msg "kill-a-worker"; not located in mined transcript | ⚠️ PARTIAL |
| R4 | live ingest ~400 ev/s, monotonic cursor | README; live check [impl-v1 AI.md "live check"]; user msg "Live demo was executed?" | ⚠️ PARTIAL |
| R5 | "verified on OrbStack k3s; runs on k3s.io/kind/minikube" | commit `73b6847` (portability) + load-image.sh | ✅ VERIFIED |
| R6 | default thresholds 200/150/30 fire zero on fixture | fixture maxima 80/30/24 (A5) | ⚠️ PARTIAL |

→ **Net README changes:** none blocking; R2–R4 are live-demo claims with commit trail but no transcript snapshot — acceptable (documented as "verified live"), could add the log excerpt if you want stronger receipts.

---

## 4. throughput.md

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| T1 | all count/rate rows | impl #2 18:30Z analysis output matches row-for-row | ✅ VERIFIED |
| T2 | §2 headline "**deletes are ~7 %**" | **contradicts its own table (1,181/42,101 = 2.8 %), DESIGN §7, and AI.md A6** | ❌ **CONTRADICTED — fix: change to ~2.8 %** |
| T3 | "no top-level seq anywhere" | impl #2 18:31Z `top_level_seq=0` | ✅ VERIFIED |
| T4 | "identity→account nested seq often consecutive" | impl #2 18:31Z analysis; 37/48 counts | ✅ VERIFIED |
| T5 | 89.8 s ts-span; 60 s wall-clock capture | impl #2 18:30Z `window_s=89.77`; capture cmd | ✅ VERIFIED |

→ **Net throughput.md changes:** T2 (one line).

---

## 5. AGENTS.md / PLAN.md / openspec

| # | Claim | Evidence | Verdict |
|---|---|---|---|
| G1 | AGENTS: "fixture stable, do not re-run casually" | fixture committed verbatim (f78c6e2), txt.md 1.1 note | ✅ VERIFIED |
| G2 | AGENTS: "routing stays in Go; NATS/LocalStack are fabric/sinks" | planning decision (this context, options A–D → B); classify code | ✅ VERIFIED |
| P1 | PLAN §0: wantedCollections filters commit events only; identity/account always delivered | planning-phase live-docs fetch (this context) + DESIGN D9 | ✅ VERIFIED |
| P2 | PLAN §0: cursor inclusive → dedupe first event after resume | planning fetch + impl 18:31Z | ✅ VERIFIED |
| P3 | PLAN §0: "delete commits carry no record/cid" | fixture + throughput.md §3 | ✅ VERIFIED |
| P4 | PLAN §4: Redis cut / one-worker optional | planning review (this context) → impl-based removal (68f2bdd) | ✅ VERIFIED |
| O1 | openspec tasks all `[x]` with verification notes | tasks.md (all checked) + git trail matches task names/dates | ✅ VERIFIED |
| O2 | openspec input artifacts match repo | `openspec/changes/jetstream-fanout-processor/{proposal,design,specs,tasks}` | ✅ VERIFIED |

→ **Net changes:** none in these four.

---

## 6. Findings requiring action (before revision)

| # | Location | Issue | Fix |
|---|---|---|---|
| F1 | throughput.md §2 | stale "deletes ~7 %" headline contradicts own table + DESIGN + AI | → `~2.8 %` |
| F2 | AI.md Setup | model claim conflicted with observed transcripts | → state observed (deepseek-v4-flash-0731 for content build, `~latest` for planning+resume); role map as config intent only |
| F3 | AI.md § "Where I overrode" | previously conflated data-derived decisions with human overrides | → already split in latest AI.md; ledger A2-A6 confirm the split is correct, A6 wording must change per F1 |
| F4 | (optional, for stronger receipts) README R2-R4 | live numbers rest on commit + live-demo claim, no transcript snapshot | → add one session log excerpt (delivered_seq / alert counts) or leave as "verified live" |

Everything else in the six docs + openspec ledger rows out **verified**. No claim required any change except F1–F3.

---

*Prepared in planning context; revocation source = session transcripts + git; impl #1 excluded as failed.*