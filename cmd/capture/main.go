// Command capture performs the bounded, ~60s Jetstream2 capture that Phase 1
// is built around (PLAN §1, Task 1.1). It dials ONE ordered WS connection
// with wantedCollections trimmed to the demo collections, drains for a bounded
// wall-clock window, and writes every event verbatim — one raw JSON object per
// line, byte-for-byte as Jetstream sent it — to testdata/sample.jsonl. Then it
// prints a compact summary and exits.
//
// Why verbatim: the fixture doubles as the integration-test fixture and the
// offline analysis fixture (Task 1.2). If we re-marshaled through Go structs we
// would lose unknown fields (identity/account frames carry fields the router
// doesn't know about yet) and could silently re-order — the analysis task
// would then be reasoning about our structs instead of the real firehose. So
// capture, live ingest, and the fixture all share the EXACT same splitRaw
// framing; raw bytes only, no re-marshal, no field loss.
//
// Exit discipline (PLAN §6): single bounded capture, then exit. No live
// subagents draining the firehose; the fixture is what offline analysis and
// resume tests consume deterministically.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"eventworker/internal/ingest"
)

var defaultCollections = []string{
	"app.bsky.feed.post",
	"app.bsky.feed.like",
	"app.bsky.feed.repost",
	"app.bsky.graph.follow",
}

func main() {
	url := flag.String("url", ingest.DefaultURL, "Jetstream2 WS endpoint")
	dur := flag.Duration("duration", 60*time.Second, "bounded capture window")
	out := flag.String("out", "testdata/sample.jsonl", "output fixture path")
	colls := flag.String("collections", strings.Join(defaultCollections, ","),
		"comma-separated wantedCollections (Jetstream ALSO always sends identity/account — we keep those too; classification-and-drop is the Go layer's job)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *dur+30*time.Second)
	defer cancel()

	conn, err := ingest.Dial(ctx, *url, splitCSV(*colls))
	must("dial", err)

	f, err := os.Create(*out)
	must("create "+*out, err)
	w := bufio.NewWriterSize(f, 1<<20)

	var (
		start        = time.Now()
		nTotal       int64
		nCommit      int64
		nDelete      int64
		nIdentity    int64
		nAccount     int64
		nUnknown     int64
		byKind       = map[string]int64{}
		byColl       = map[string]int64{}
		byOp         = map[string]int64{}
		keys         = map[string]bool{}
		nDupKeys     int64
		prevSeq      int64
		seqSet       bool
		nSeqRegress  int64
		prevTime     int64
		timeSet      bool
		nTimeRegress int64
	)

loop:
	for {
		// Bounded window gate: stop at the requested duration, not at the
		// ctx backstop (ctx carries a +30s dial/resume margin and is only a
		// hard stop). Without this the fixture would silently run dur+30s
		// and the "~60s bounded capture" claim in PLAN §1/Task 1.1 would be
		// wrong. NextRaw blocks on ctx, so ctx.Done is still honored when a
		// read is mid-flight.
		if time.Since(start) >= *dur {
			break loop
		}
		raws, err := conn.NextRaw(ctx)
		if err != nil {
			break loop
		}
		for _, raw := range raws {
			nTotal++
			// classify-once is Phase 2; here we just tally the raw envelope
			// fields for the summary + fixture sanity. The offline analysis
			// (Task 1.2) does the real schema/throughput work.
			var env struct {
				Kind string `json:"kind"`
				DID  string `json:"did"`
				TS   int64  `json:"time_us"`
				C    struct {
					Operation  string `json:"operation"`
					Collection string `json:"collection"`
				} `json:"commit"`
			}
			// Best-effort tally decode — never required to succeed.
			_ = decodeRaw(raw, &env)
			switch env.Kind {
			case "commit":
				nCommit++
				byKind["commit"]++
				byColl[env.C.Collection]++
				byOp[env.C.Operation]++
				if env.C.Operation == "delete" {
					nDelete++
				}
			case "identity":
				nIdentity++
				byKind["identity"]++
			case "account":
				nAccount++
				byKind["account"]++
			default:
				nUnknown++
				byKind["kind="+env.Kind]++
			}
			// seq monotonicity + timestamp monotonicity (cursor sanity).
			var env2 struct {
				Seq int64 `json:"seq"`
			}
			_ = decodeRaw(raw, &env2)
			if seqSet && env2.Seq < prevSeq {
				nSeqRegress++
			}
			if env2.Seq != 0 {
				prevSeq, seqSet = env2.Seq, true
			}
			if timeSet && env.TS < prevTime {
				nTimeRegress++
			}
			if env.TS != 0 {
				prevTime, timeSet = env.TS, true
			}
			// dedupe-key collision sanity (fixture-quality check).
			if did, coll, op, rkey := dedupeParts(raw); did != "" && (coll != "" || op == "") {
				k := did + "|" + coll + "|" + op + "|" + rkey
				if keys[k] {
					nDupKeys++
				}
				keys[k] = true
			}
			w.Write(raw)
			w.WriteByte('\n')
		}
	}

	flushErr := w.Flush()
	closeErr := f.Close()
	durActual := time.Since(start)

	fmt.Printf("capture  out=%s frames_scan=%d\n", *out, nTotal)
	fmt.Printf("capture  window=%.1fs rate=%.0f ev/s\n", durActual.Seconds(),
		frac(nTotal, durActual))
	fmt.Printf("capture  kinds commit=%d identity=%d account=%d unknown=%d\n",
		nCommit, nIdentity, nAccount, nUnknown)
	fmt.Printf("capture  operation delete=%d\n", nDelete)
	fmt.Println("capture  dedupe_keys_live_total=", nTotal, "dedupe_keys_dup=", nDupKeys)
	printSorted("collection", byColl)
	printSorted("operation", byOp)
	fmt.Printf("capture  cursor  seq_regressions=%d time_regressions=%d dup_dedupe_keys=%d\n",
		nSeqRegress, nTimeRegress, nDupKeys)
	conn.Close()
	if flushErr != nil {
		fmt.Fprintln(os.Stderr, "flush:", flushErr)
		os.Exit(1)
	}
	if closeErr != nil {
		fmt.Fprintln(os.Stderr, "close:", closeErr)
		os.Exit(1)
	}
}

func printSorted(label string, m map[string]int64) {
	type kv struct {
		k string
		v int64
	}
	var ks []kv
	for k, v := range m {
		ks = append(ks, kv{k, v})
	}
	// descending by count (per-type throughput high→low)
	for i := 1; i < len(ks); i++ {
		for j := i; j > 0 && ks[j].v > ks[j-1].v; j-- {
			ks[j], ks[j-1] = ks[j-1], ks[j]
		}
	}
	var sb strings.Builder
	sb.WriteString("capture  " + label + " [high→low]")
	if len(ks) == 0 {
		sb.WriteString(" (none)")
	}
	for _, e := range ks {
		fmt.Fprintf(&sb, "  %s=%d", e.k, e.v)
	}
	fmt.Println(sb.String())
}

func frac(n int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// decodeRaw is a best-effort single decode used only for the live capture
// summary (throughput table). Jetstream frames always decode; if one ever
// doesn't we keep the byte-for-byte fixture line anyway — the summary is a
// convenience, never a correctness gate.
func decodeRaw(raw []byte, v any) error {
	return json.Unmarshal(raw, v)
}

// dedupeParts pulls the (did, collection, operation, rkey) identity used by
// the dedupe-key heuristic straight out of a raw commit framecarrier. Only the
// LAST object in a frame advances the honest counter; multiple objects are
// split-by-line first, so raw here is exactly one commit/identity/account
// JSON object.
func dedupeParts(raw []byte) (did, coll, op, rkey string) {
	var ev struct {
		DID string `json:"did"`
		C   struct {
			Collection string `json:"collection"`
			Operation  string `json:"operation"`
			RKey       string `json:"rkey"`
		} `json:"commit"`
	}
	if json.Unmarshal(raw, &ev) != nil {
		return "", "", "", ""
	}
	return ev.DID, ev.C.Collection, ev.C.Operation, ev.C.RKey
}

func must(what string, err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "capture:", what, ":", err)
		os.Exit(1)
	}
}
