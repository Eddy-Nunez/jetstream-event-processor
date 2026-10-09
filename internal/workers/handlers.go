package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"eventworker/internal/classify"
	"eventworker/internal/ingest"
	"eventworker/internal/metrics"
)

const (
	postCollection   = "app.bsky.feed.post"
	likeCollection   = "app.bsky.feed.like"
	repostCollection = "app.bsky.feed.repost"
	followCollection = "app.bsky.graph.follow"

	defaultWindow    = 60 * time.Second
	defaultDeleteTTL = 60 * time.Second

	// deletePruneEvery full-map tombstone sweeps; per-key expiry is also
	// enforced on the lazily-pruned path.
	deletePruneEvery = 1024
)

// Config holds the per-type tuning shared by all handlers. Now is a clock
// hook for tests; when nil, time.Now is used.
type Config struct {
	Keywords        []string      // posts: case-insensitive keyword matches
	Langs           []string      // posts: target languages (intersection)
	LikeThreshold   int           // likes: alert once per uri per window at this count
	RepostThreshold int           // reposts: same semantics
	FollowBurst     int           // follows: burst alert threshold per target
	Window          time.Duration // sliding window for likes/reposts/follows
	DeleteTTL       time.Duration // tombstone expiry for deletes
	Now             func() time.Time
}

// NewHandler builds the per-type handler for a worker type name
// (posts|likes|reposts|follows|deletes). Unknown names are rejected.
func NewHandler(name string, cfg Config) (Handler, error) {
	if _, ok := classify.TypeForWorkerName(name); !ok {
		return nil, fmt.Errorf("workers: unknown type %q", name)
	}
	if cfg.Window <= 0 {
		cfg.Window = defaultWindow
	}
	if cfg.DeleteTTL <= 0 {
		cfg.DeleteTTL = defaultDeleteTTL
	}
	log := slog.Default()

	switch name {
	case "posts":
		for i := range cfg.Keywords {
			cfg.Keywords[i] = strings.ToLower(strings.TrimSpace(cfg.Keywords[i]))
		}
		return &postHandler{cfg: cfg, log: log}, nil
	case "likes":
		return &countHandler{
			name: name, collection: likeCollection, threshold: cfg.LikeThreshold,
			counter: newWindowCounter(cfg.Window, cfg.Now), log: log,
		}, nil
	case "reposts":
		return &countHandler{
			name: name, collection: repostCollection, threshold: cfg.RepostThreshold,
			counter: newWindowCounter(cfg.Window, cfg.Now), log: log,
		}, nil
	case "follows":
		return &followHandler{
			threshold: cfg.FollowBurst, counter: newWindowCounter(cfg.Window, cfg.Now), log: log,
		}, nil
	case "deletes":
		now := cfg.Now
		if now == nil {
			now = time.Now
		}
		return &deleteHandler{ttl: cfg.DeleteTTL, now: now, tombstones: make(map[string]time.Time), log: log}, nil
	}
	return nil, fmt.Errorf("workers: unhandled type %q", name)
}

// postRecord is the app.bsky.feed.post record shape the matcher depends on.
// Embed-only posts carry empty text (fixture-verified) but still run the
// language check.
type postRecord struct {
	Text  string   `json:"text"`
	Langs []string `json:"langs"`
}

// postHandler notifies when an app.bsky.feed.post record (create|update)
// matches a configured keyword (case-insensitive) or intersects a configured
// language set. Anything else — including delete commits and non-post
// collections — is a no-op success.
type postHandler struct {
	cfg Config
	log *slog.Logger
}

func (h *postHandler) Handle(ctx context.Context, ev *ingest.Event) error {
	commit := ev.Commit
	if commit == nil || commit.IsDelete() || commit.Collection != postCollection {
		return nil
	}
	if len(commit.Record) == 0 {
		return nil
	}
	var rec postRecord
	if err := json.Unmarshal(commit.Record, &rec); err != nil {
		return fmt.Errorf("workers: decode post record: %w", err)
	}

	matched := false
	reason := "keyword"
	text := strings.ToLower(rec.Text)
	for _, kw := range h.cfg.Keywords {
		if kw != "" && strings.Contains(text, kw) {
			matched = true
			break
		}
	}
	if !matched && langsIntersect(rec.Langs, h.cfg.Langs) {
		matched = true
		reason = "lang"
	}
	if matched {
		h.log.Info("worker event=notify",
			"type", "posts", "did", ev.DID,
			"uri", postURI(ev.DID, commit), "reason", reason,
		)
		metrics.AlertsTotal.WithLabelValues("posts").Inc()
	}
	return nil
}

func postURI(did string, c *ingest.Commit) string {
	return "at://" + did + "/" + c.Collection + "/" + c.RKey
}

func langsIntersect(recordLangs, configLangs []string) bool {
	if len(configLangs) == 0 {
		return false
	}
	for _, l := range recordLangs {
		for _, c := range configLangs {
			if l == c {
				return true
			}
		}
	}
	return false
}

// countedRecord is the shared app.bsky.feed.like / .repost record shape:
// subject is an object carrying the at:// uri of the liked/reposted post.
type countedRecord struct {
	Subject *countedSubject `json:"subject"`
}

type countedSubject struct {
	URI string `json:"uri"`
}

// countHandler alerts once per subject uri per window once the rolling
// in-window count crosses the configured threshold (likes/reposts).
type countHandler struct {
	name       string
	collection string
	threshold  int
	counter    *windowCounter
	log        *slog.Logger
}

func (h *countHandler) Handle(ctx context.Context, ev *ingest.Event) error {
	commit := ev.Commit
	if commit == nil || commit.IsDelete() || commit.Collection != h.collection {
		return nil
	}
	if len(commit.Record) == 0 {
		return nil
	}
	var rec countedRecord
	if err := json.Unmarshal(commit.Record, &rec); err != nil {
		return fmt.Errorf("workers: decode %s record: %w", h.collection, err)
	}
	if rec.Subject == nil || rec.Subject.URI == "" {
		return nil
	}
	if h.counter.add(rec.Subject.URI, h.threshold) {
		h.log.Info("worker event=notify",
			"type", h.name, "uri", rec.Subject.URI, "reason", "threshold",
		)
		metrics.AlertsTotal.WithLabelValues(h.name).Inc()
	}
	return nil
}

// followedRecord — the follow record's subject is a bare DID string
// (fixture-verified), unlike the like/repost subject objects.
type followedRecord struct {
	Subject string `json:"subject"`
}

// followHandler alerts on a sliding-window burst of follows per target,
// rate-limited to once per window per target. In-memory only (Redis is the
// documented optional path elsewhere; not implemented here).
type followHandler struct {
	threshold int
	counter   *windowCounter
	log       *slog.Logger
}

func (h *followHandler) Handle(ctx context.Context, ev *ingest.Event) error {
	commit := ev.Commit
	if commit == nil || commit.IsDelete() || commit.Collection != followCollection {
		return nil
	}
	if len(commit.Record) == 0 {
		return nil
	}
	var rec followedRecord
	if err := json.Unmarshal(commit.Record, &rec); err != nil {
		return fmt.Errorf("workers: decode follow record: %w", err)
	}
	if rec.Subject == "" {
		return nil
	}
	if h.counter.add(rec.Subject, h.threshold) {
		h.log.Info("worker event=notify",
			"type", "follows", "target", rec.Subject, "reason", "burst",
		)
		metrics.AlertsTotal.WithLabelValues("follows").Inc()
	}
	return nil
}

// deleteHandler records tombstones for delete commits (any collection),
// keyed did|collection|rkey with a TTL. A delete is NEVER content-matched.
type deleteHandler struct {
	ttl time.Duration
	now func() time.Time

	log        *slog.Logger
	mu         sync.Mutex
	tombstones map[string]time.Time
	ops        int
}

func (h *deleteHandler) Handle(ctx context.Context, ev *ingest.Event) error {
	commit := ev.Commit
	if commit == nil || !commit.IsDelete() {
		return nil
	}
	key := ev.DID + "|" + commit.Collection + "|" + commit.RKey

	h.mu.Lock()
	h.ops++
	if h.ops%deletePruneEvery == 0 {
		h.pruneLocked()
	}
	h.tombstones[key] = h.now().Add(h.ttl)
	h.mu.Unlock()

	h.log.Debug("delete tombstone", "key", key, "ttl", h.ttl)
	return nil
}

func (h *deleteHandler) pruneLocked() {
	now := h.now()
	for k, exp := range h.tombstones {
		if !exp.After(now) {
			delete(h.tombstones, k)
		}
	}
}

// windowCounter is a sliding-window event counter keyed by an arbitrary
// string (like/repost URI, follow target DID). It is safe for concurrent
// use and prunes stale state as the map grows.
type windowCounter struct {
	mu     sync.Mutex
	window time.Duration
	now    func() time.Time
	ops    int

	counts  map[string][]time.Time
	alerted map[string]time.Time // last alert per key (once per window)
}

func newWindowCounter(window time.Duration, now func() time.Time) *windowCounter {
	if now == nil {
		now = time.Now
	}
	return &windowCounter{
		window:  window,
		now:     now,
		counts:  make(map[string][]time.Time),
		alerted: make(map[string]time.Time),
	}
}

// add records an event for key. It returns true when the in-window count
// reaches threshold and no alert has fired for that key within the current
// window (once per key per window). Thresholds <= 0 disable alerts.
func (w *windowCounter) add(key string, threshold int) bool {
	if threshold <= 0 {
		return false
	}
	now := w.now()

	w.mu.Lock()
	defer w.mu.Unlock()

	w.ops++
	if w.ops%4096 == 0 {
		w.pruneLocked(now)
	}

	cutoff := now.Add(-w.window)
	bucket := w.counts[key]
	keep := bucket[:0]
	for _, t := range bucket {
		if t.After(cutoff) {
			keep = append(keep, t)
		}
	}
	keep = append(keep, now)
	w.counts[key] = keep

	if len(keep) < threshold {
		return false
	}
	last, ok := w.alerted[key]
	if ok && now.Sub(last) < w.window {
		return false
	}
	w.alerted[key] = now
	return true
}

func (w *windowCounter) pruneLocked(now time.Time) {
	cutoff := now.Add(-w.window)
	for k, bucket := range w.counts {
		keep := bucket[:0]
		for _, t := range bucket {
			if t.After(cutoff) {
				keep = append(keep, t)
			}
		}
		if len(keep) == 0 {
			delete(w.counts, k)
			delete(w.alerted, k)
			continue
		}
		w.counts[k] = keep
	}
	for k, t := range w.alerted {
		if now.Sub(t) >= w.window {
			delete(w.alerted, k)
		}
	}
}
