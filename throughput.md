# throughput.md — fixture analysis (Task 1.1)

**Source:** `testdata/sample.jsonl` — one bounded capture from `wss://jetstream2.us-east.bsky.network/subscribe`
with `wantedCollections = posts, likes, reposts, follows`. **Method:** fully offline, deterministic, reproducible
(`python3 /tmp/analyze_sample.py`-style pass; no live feed). Capture was bounded at a 60 s wall-clock window;
the fixture's first-to-last `time_us` span is **89.8 s** (server timestamps across the connection lifetime),
so rates below are given as a range — **ts-span (conservative) → 60 s window (upper)**. Size against the upper bound.

## 1. Overview

| Metric | Value |
|---|---|
| Events | 42,186 |
| Commits | 42,101 (99.8 %) |
| Identity frames | 37 |
| Account frames | 48 |
| Sync frames | 0 (not observed in this capture) |
| Unique DIDs | 16,344 |
| Total rate | ~470→700 ev/s |
| Commit rate | ~470→702 ev/s |

Structure record per frame: `{did, time_us, kind, ...kind-specific}` — no top-level `seq` anywhere (see §4).

## 2. Per-type throughput (commits, high → low)

| Type / op | Count | Rate (ts-span → 60 s) | Share |
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

Headline: **likes alone are ~68 % of commit volume**; deletes are ~7 %; identity+account are a steady ~1 /s
floor that arrives regardless of `wantedCollections`.

## 3. Per-collection schema (sampled 300 records each, verified shapes)

| Collection | Record shape |
|---|---|
| `app.bsky.feed.post` | `{$type, createdAt, text, langs[]?, reply?, embed?}` — `langs` top values: en 3297, ja 264, pt 203, es 164, de 150…; 163/4,889 creates have **empty text** (embed-only: images/external/record quote posts); embed `$type`s seen: images, external, record, recordWithMedia, video, gallery. `reply{root,parent}{cid,uri}` present on ~54 % |
| `app.bsky.feed.like` | `{$type, createdAt, subject{cid,uri}}`; `via{cid,uri}` ~15 % |
| `app.bsky.feed.repost` | `{$type, createdAt, subject{cid,uri}}`; `via` ~25 % |
| `app.bsky.graph.follow` | `{$type, createdAt, subject}` — **`subject` is a bare DID string** (the target account; burst-detection key) |
| delete commits | **No `record`, no `cid`** — always `{rev, operation:"delete", collection, rkey}` (verified on all 1,181) |
| update commits | 10/10 are bridge ("bridgy") re-posts with a **full replacement record** → update = replace semantics |

## 4. Cursor / dedupe findings (corrects PLAN assumptions)

1. **No top-level `seq` on any frame.** `seq` exists **nested** inside `identity`/`account` objects:
   `{did, time_us, kind:"identity", identity:{did, seq, time}}`, likewise `account:{active, did, seq, status?, time}`.
   Commits carry **no seq at all**.
2. **`time_us` is present on all 42,186 events and strictly monotonic (0 regressions)** → it is the correct global
   resume cursor: last processed `time_us`; first event after resume (inclusive cursor) deduped against it.
3. **Dedupe key confirmed:** `did|collection|rkey|operation` was unique within the window (42,101 keys, **0 dups,
   0 keys with multiple revs**). Adding `rev` is still correct conceptually (an update bumps `rev` for the same
   `rkey`+`op`; there were only 10 such events in-window, none colliding). Identity/account dedupe should key on
   the **nested** `seq` (`identity.seq` / `account.seq`), not a top-level field.
4. identity→account pairing: same-DID identity/account frames often carry consecutive nested seqs (37 identity /
   48 account frames in this capture; not a hard 1:1).

## 5. Sizing implications (feeds DESIGN.md / Task 1.2)

- **MaxAge sizing:** at the upper bound (~700 ev/s, ~479 like/s) one hour of `events.*` ≈ **2.5M messages**
  (~1.7M likes). A 1 h `MaxAge` replay window is comfortable for a laptop-k3s demo; production would raise it
  with the throughput table above.
- **≈16k unique DIDs / ~90 s** — per-DID state (e.g. follows burst) stays small for the demo window.
- **Identity/account floor ≈ 1 ev/s** is negligible in volume but mandatory to classify-and-drop (PLAN §0).

## 6. Reproduce

```bash
python3 /tmp/analyze_sample.py   # prints the full table + schema + monotonicity checks
```
