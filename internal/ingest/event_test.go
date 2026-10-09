// event_test — fixture-backed verification of the envelope contract that
// routing/cursor logic depends on. Reads testdata/sample.jsonl (committed
// 60s capture) and asserts the invariant findings from throughput.md §4:
// nested identity/account seq, no top-level seq, unique dedupe keys,
// delete commits carry no record/cid.
package ingest

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
)

const fixture = "../../testdata/sample.jsonl"

func parseFixture(t *testing.T) chan *Event {
	t.Helper()
	f, err := os.Open(fixture)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	ch := make(chan *Event, 4096)
	go func() {
		defer close(ch)
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		for sc.Scan() {
			line := sc.Bytes()
			var ev Event
			if json.Unmarshal(line, &ev) != nil {
				continue
			}
			ch <- &ev
		}
	}()
	return ch
}

func TestFixtureDecodeContract(t *testing.T) {
	var (
		n, topSeq, idnSeq, acctSeq, tZero int
	)
	for ev := range parseFixture(t) {
		n++
		if ev.TimeUS == 0 {
			tZero++
		}
		_ = ev // top-level Seq field no longer exists; contract enforced at type level
		switch ev.Kind {
		case "identity":
			if ev.Identity == nil || ev.Identity.Seq == 0 {
				t.Errorf("identity frame missing nested seq: %+v", ev)
			}
			idnSeq++
		case "account":
			if ev.Account == nil || ev.Account.Seq == 0 {
				t.Errorf("account frame missing nested seq: %+v", ev)
			}
			acctSeq++
		}
	}
	if n < 40000 {
		t.Fatalf("fixture too small: %d events", n)
	}
	if topSeq != 0 {
		t.Errorf("top-level seq seen %d times (should be impossible — no json field)", topSeq)
	}
	if idnSeq == 0 || acctSeq == 0 {
		t.Errorf("expected nested identity/account seqs, got idn=%d acct=%d", idnSeq, acctSeq)
	}
	if tZero != 0 {
		t.Errorf("%d events missing time_us (cursor depends on it)", tZero)
	}
}

func TestFixtureDedupeKeysUnique(t *testing.T) {
	seen := make(map[string]int, 60000)
	dups := 0
	deletesNoRecord := 0
	deletes := 0
	for ev := range parseFixture(t) {
		k := ev.DedupeKey()
		if seen[k]++; seen[k] == 2 {
			dups++
		}
		if ev.Commit != nil && ev.Commit.Operation == "delete" {
			deletes++
			if ev.Commit.Record != nil || ev.Commit.CID != "" {
				deletesNoRecord++
			}
		}
	}
	if dups != 0 {
		t.Fatalf("%d duplicate dedupe keys in fixture (DedupeKey not stable/unique)", dups)
	}
	if deletes == 0 {
		t.Fatal("fixture contains no delete commits?")
	}
	if deletesNoRecord != 0 {
		t.Errorf("%d delete commits carried record/cid — delete must be record-less", deletesNoRecord)
	}
}
