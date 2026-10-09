// handlers_test — per-type handler behavior tests, including the fixture
// contract test that drives the posts handler over the committed 60s
// capture and asserts its alert count equals an independently computed
// expectation from the same decode.
package workers

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"eventworker/internal/ingest"
	"eventworker/internal/metrics"
)

const fixturePath = "../../testdata/sample.jsonl"

// alertCount reads the current value of metrics.AlertsTotal{type=label}.
func alertCount(t *testing.T, label string) float64 {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range fams {
		if mf.GetName() != "eventworker_alerts_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var typ string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "type" {
					typ = lp.GetValue()
				}
			}
			if typ == label && m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// event builds a commit event with an optional raw record JSON.
func event(collection, operation, record string) *ingest.Event {
	ev := &ingest.Event{
		DID:    "did:plc:test",
		TimeUS: 1,
		Kind:   "commit",
		Commit: &ingest.Commit{
			Rev:        "r1",
			Operation:  operation,
			Collection: collection,
			RKey:       "k1",
		},
	}
	if record != "" {
		ev.Commit.Record = json.RawMessage(record)
	}
	return ev
}

// TestPostHandlerFixtureContract decodes the committed fixture, computes the
// expected posts-alert count independently (text contains "test"
// case-insensitively OR langs intersect {"en"}, over post create|update
// commits), then drives the same events through the posts handler and
// asserts the metrics delta matches.
func TestPostHandlerFixtureContract(t *testing.T) {
	metrics.Init()

	handler, err := NewHandler("posts", Config{
		Keywords: []string{"test"},
		Langs:    []string{"en"},
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	before := alertCount(t, "posts")

	f, err := os.Open(fixturePath)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	ctx := context.Background()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	var want int
	events := 0
	for sc.Scan() {
		var ev ingest.Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			continue
		}
		events++

		if commit := ev.Commit; commit != nil &&
			!commit.IsDelete() && commit.Collection == postCollection && len(commit.Record) > 0 {
			var rec struct {
				Text  string   `json:"text"`
				Langs []string `json:"langs"`
			}
			if err := json.Unmarshal(commit.Record, &rec); err == nil {
				kw := strings.Contains(strings.ToLower(rec.Text), "test")
				lang := false
				for _, l := range rec.Langs {
					if l == "en" {
						lang = true
						break
					}
				}
				if kw || lang {
					want++
				}
			}
		}

		if err := handler.Handle(ctx, &ev); err != nil {
			t.Fatalf("handler.Handle: %v (did=%s)", err, ev.DID)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if events < 40000 {
		t.Fatalf("fixture decode too small: %d events", events)
	}

	got := int(alertCount(t, "posts") - before)
	if got != want {
		t.Fatalf("posts handler alerts = %d, want %d (keyword=%q langs=%v)", got, want, "test", []string{"en"})
	}
}

func TestPostHandlerMatch(t *testing.T) {
	metrics.Init()
	ctx := context.Background()

	h, err := NewHandler("posts", Config{
		Keywords: []string{"tEsT"}, // must be matched case-insensitively
		Langs:    []string{"en"},
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	cases := []struct {
		name   string
		ev     *ingest.Event
		alerts int
	}{
		{"keyword hit (ci)", event(postCollection, "create", `{"text":"A TEST post","langs":["ja"]}`), 1},
		{"lang hit on empty text (embed-only)", event(postCollection, "create", `{"text":"","langs":["en"]}`), 1},
		{"embed-only no langs", event(postCollection, "create", `{"embed":{"$type":"app.bsky.embed.images"}}`), 0},
		{"no match", event(postCollection, "create", `{"text":"hello","langs":["fr"]}`), 0},
		{"update matches", event(postCollection, "update", `{"text":"x test y"}`), 1},
		{"delete never matched", event(postCollection, "delete", ""), 0},
		{"like ignored", event(likeCollection, "create", `{"subject":{"uri":"at://x"}}`), 0},
		{"identity ignored", &ingest.Event{DID: "did:plc:test", Kind: "identity"}, 0},
	}
	for _, tc := range cases {
		before := alertCount(t, "posts")
		if err := h.Handle(ctx, tc.ev); err != nil {
			t.Errorf("[%s] handler: %v", tc.name, err)
			continue
		}
		if got := int(alertCount(t, "posts") - before); got != tc.alerts {
			t.Errorf("[%s] alerts delta = %d, want %d", tc.name, got, tc.alerts)
		}
	}
}

// TestCountHandlerThresholdOncePerWindow covers the like/repost rolling
// window: alerts fire once per uri per window after the threshold, and not
// again until the window has slid past (fresh events re-accumulate).
func TestCountHandlerThresholdOncePerWindow(t *testing.T) {
	metrics.Init()
	ctx := context.Background()

	now := time.Date(2026, 10, 9, 17, 0, 0, 0, time.UTC)
	h, err := NewHandler("likes", Config{
		LikeThreshold: 3,
		Window:        time.Minute,
		Now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	evA := event(likeCollection, "create", `{"subject":{"uri":"at://did:a/app.bsky.feed.post/1"}}`)
	evB := event(likeCollection, "create", `{"subject":{"uri":"at://did:b/app.bsky.feed.post/1"}}`)
	steps := []struct {
		name string
		ev   *ingest.Event
		want int
	}{
		{"A(1)", evA, 0},
		{"A(2)", evA, 0},
		{"B(1)", evB, 0},
		{"A(3) alert", evA, 1},
		{"A(4) once per window", evA, 0},
	}
	for _, s := range steps {
		before := alertCount(t, "likes")
		if err := h.Handle(ctx, s.ev); err != nil {
			t.Fatalf("[%s] handler: %v", s.name, err)
		}
		if got := int(alertCount(t, "likes") - before); got != s.want {
			t.Fatalf("[%s] alerts delta = %d, want %d", s.name, got, s.want)
		}
	}

	// Window slides: past events age out, a fresh 3-event burst alerts again.
	now = now.Add(time.Minute + time.Second)
	steps2 := []struct {
		name string
		ev   *ingest.Event
		want int
	}{
		{"A(5) aged window", evA, 0},
		{"A(6)", evA, 0},
		{"A(7) alert again", evA, 1},
	}
	for _, s := range steps2 {
		before := alertCount(t, "likes")
		if err := h.Handle(ctx, s.ev); err != nil {
			t.Fatalf("[%s] handler: %v", s.name, err)
		}
		if got := int(alertCount(t, "likes") - before); got != s.want {
			t.Fatalf("[%s] alerts delta = %d, want %d", s.name, got, s.want)
		}
	}

	// Reposts use their own threshold and label.
	rh, err := NewHandler("reposts", Config{
		RepostThreshold: 2,
		Window:          time.Minute,
		Now:             func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	evR := event(repostCollection, "create", `{"subject":{"uri":"at://did:r/app.bsky.feed.post/1"}}`)
	before := alertCount(t, "reposts")
	if err := rh.Handle(ctx, evR); err != nil {
		t.Fatal(err)
	}
	if got := int(alertCount(t, "reposts") - before); got != 0 {
		t.Fatalf("reposts delta = %d, want 0", got)
	}
	if err := rh.Handle(ctx, evR); err != nil {
		t.Fatal(err)
	}
	if got := int(alertCount(t, "reposts") - before); got != 1 {
		t.Fatalf("reposts delta = %d, want 1", got)
	}
}

// TestFollowHandlerBurst covers follows burst detection on the bare-DID
// subject shape, rate-limited once per window per target.
func TestFollowHandlerBurst(t *testing.T) {
	metrics.Init()
	ctx := context.Background()

	now := time.Date(2026, 10, 9, 17, 0, 0, 0, time.UTC)
	h, err := NewHandler("follows", Config{
		FollowBurst: 2,
		Window:      time.Minute,
		Now:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	evT := event(followCollection, "create", `{"subject":"did:plc:target"}`)
	before := alertCount(t, "follows")
	for i, want := range []int{0, 1, 1, 1} { // cumulative: alert on burst 2, then rate-limited
		if err := h.Handle(ctx, evT); err != nil {
			t.Fatalf("follow handler: %v", err)
		}
		if got := int(alertCount(t, "follows") - before); got != want {
			t.Fatalf("follow step %d: alerts delta = %d, want %d", i, got, want)
		}
	}
}

// TestDeleteHandlerTombstone covers tombstones keyed did|collection|rkey
// with a TTL, pruned after expiry, and confirms non-delete commits never
// produce tombstones (deletes are never content-matched).
func TestDeleteHandlerTombstone(t *testing.T) {
	metrics.Init()
	ctx := context.Background()

	now := time.Date(2026, 10, 9, 17, 0, 0, 0, time.UTC)
	h, err := NewHandler("deletes", Config{
		DeleteTTL: 60 * time.Second,
		Now:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	dh := h.(*deleteHandler)

	del := event(postCollection, "delete", "")
	if err := h.Handle(ctx, del); err != nil {
		t.Fatalf("delete handler: %v", err)
	}
	dh.mu.Lock()
	_, ok := dh.tombstones["did:plc:test|"+postCollection+"|k1"]
	dh.mu.Unlock()
	if !ok {
		t.Fatal("tombstone missing for delete commit")
	}

	// A create commit must not create a tombstone.
	if err := h.Handle(ctx, event(postCollection, "create", `{"text":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	dh.mu.Lock()
	n := len(dh.tombstones)
	dh.mu.Unlock()
	if n != 1 {
		t.Fatalf("tombstones = %d, want 1 (create must not tombstone)", n)
	}

	// After TTL expiry, a fresh delete for a DIFFERENT key, then a prune,
	// removes the expired tombstone while keeping the fresh one.
	now = now.Add(61 * time.Second)
	del2 := &ingest.Event{
		DID:    "did:plc:other",
		TimeUS: 2,
		Kind:   "commit",
		Commit: &ingest.Commit{
			Rev:        "r1",
			Operation:  "delete",
			Collection: postCollection,
			RKey:       "k2",
		},
	}
	if err := h.Handle(ctx, del2); err != nil {
		t.Fatal(err)
	}
	dh.mu.Lock()
	dh.pruneLocked()
	_, fresh := dh.tombstones["did:plc:other|"+postCollection+"|k2"]
	_, expired := dh.tombstones["did:plc:test|"+postCollection+"|k1"]
	length := len(dh.tombstones)
	dh.mu.Unlock()
	if length != 1 || !fresh || expired {
		t.Fatalf("after prune: len=%d fresh=%v expired=%v (want len=1 fresh=true expired=false)", length, fresh, expired)
	}
}

func TestNewHandlerRejectsUnknownType(t *testing.T) {
	if _, err := NewHandler("bogus", Config{}); err == nil {
		t.Fatal("expected error for unknown type")
	}
}
