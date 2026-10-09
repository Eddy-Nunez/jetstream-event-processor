// Command ingest is the single-connection intake of the fanout processor
// (PLAN §1, Task 2.3). One ordered Jetstream WS connection → decode-once →
// classify (internal/classify) → publish to the per-type NATS subject with a
// stable Nats-Msg-Id; identity/account are counted and dropped at publish
// (unless -publish-identity). The resume cursor is the last processed
// time_us (fixture-verified: present on every frame, monotonic); on reconnect
// the inclusive-cursor event is deduped and the MsgId guard covers crash
// windows where the cursor was not yet persisted.
//
// -replay <file> feeds the committed fixture (testdata/sample.jsonl) through
// the exact same classify→publish path, so the whole pipeline is demonstrable
// offline and deterministically, without touching the live firehose.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"eventworker/internal/broker"
	"eventworker/internal/classify"
	"eventworker/internal/health"
	"eventworker/internal/ingest"
	"eventworker/internal/metrics"
)

type cursor struct {
	Time int64  `json:"time_us"` // last processed time_us (monotonic)
	Key  string `json:"key"`     // its DedupeKey — for inclusive-cursor dedupe
}

func loadCursor(path string) cursor {
	if path == "" {
		return cursor{}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return cursor{}
	}
	var c cursor
	if json.Unmarshal(b, &c) != nil {
		return cursor{}
	}
	return c
}

// saveCursor writes atomically (temp + rename) so a crash never leaves a
// truncated cursor file.
func saveCursor(path string, c cursor) error {
	if path == "" {
		return nil
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func wsURLWithCursor(base string, t int64) string {
	if t <= 0 {
		return base
	}
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%scursor=%d", base, sep, t)
}

type app struct {
	log   *slog.Logger
	ready atomic.Bool
}

func (a *app) readiness() error {
	if !a.ready.Load() {
		return errors.New("not connected")
	}
	return nil
}

func main() {
	wsURL := flag.String("url", ingest.DefaultURL, "Jetstream WS endpoint")
	natsURL := flag.String("nats", broker.DefaultURL, "NATS URL")
	streamName := flag.String("stream", "EVENTS", "JetStream stream name")
	maxAge := flag.Duration("maxage", time.Hour, "stream replay window")
	cursorFile := flag.String("cursor", "", "cursor persistence file (empty = no persistence)")
	httpAddr := flag.String("http", ":8080", "health/metrics listen address")
	publishIdentity := flag.Bool("publish-identity", false, "also publish identity/account frames (default: drop-at-publish, counted)")
	replay := flag.String("replay", "", "replay a fixture (.jsonl) instead of the live WS (offline demo)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics.Init()
	a := &app{log: log}

	mux := health.Handler(a.readiness)
	srv := &http.Server{Addr: *httpAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
		}
	}()

	b, err := broker.Connect(ctx, *natsURL)
	if err != nil {
		log.Error("broker connect", "err", err)
		os.Exit(1)
	}
	defer b.Close()
	if err := b.EnsureStream(ctx, broker.StreamConfig{Name: *streamName, MaxAge: *maxAge}); err != nil {
		log.Error("ensure stream", "err", err)
		os.Exit(1)
	}
	log.Info("stream ensured", "stream", *streamName, "subjects", []string{"events.>"})

	if *replay != "" {
		a.runReplay(ctx, b, *streamName, *replay, *publishIdentity)
		return
	}

	a.runLive(ctx, b, *streamName, *wsURL, *cursorFile, *publishIdentity)
}

// publishEvent classifies one decoded event and (per policy) publishes it to
// its per-type subject with Nats-Msg-Id = DedupeKey. Returns the resolved
// type and whether the event advanced the cursor.
func (a *app) publishEvent(ctx context.Context, b *broker.Broker, ev *ingest.Event, publishIdentity bool) (classify.Type, bool) {
	typ := classify.Type(classify.Drop)
	if ev.Commit != nil {
		typ = classify.Classify(ev.Kind, ev.Commit.Collection, ev.Commit.Operation)
	} else {
		typ = classify.Classify(ev.Kind, "", "")
	}
	metrics.EventsTotal.WithLabelValues(typ.String()).Inc()
	key := ev.DedupeKey()

	switch typ {
	case classify.Drop:
		metrics.DropTotal.WithLabelValues("unknown").Inc()
		a.log.Debug("drop unknown", "did", ev.DID, "kind", ev.Kind)
	case classify.Identity, classify.Account:
		metrics.DropTotal.WithLabelValues(typ.String()).Inc()
		if !publishIdentity {
			// drop-at-publish: counted, never lost
			return typ, true
		}
		fallthrough
	default:
		payload, err := json.Marshal(ev)
		if err != nil {
			metrics.ErrorsTotal.Inc()
			return typ, false
		}
		if err := b.Publish(ctx, typ.Subject(), key, payload); err != nil {
			metrics.ErrorsTotal.Inc()
			a.log.Error("publish", "subject", typ.Subject(), "err", err)
			return typ, false
		}
		metrics.PublishTotal.WithLabelValues(typ.String()).Inc()
	}
	return typ, true
}

// runLive is the ordered WS ingest loop with cursor resume.
func (a *app) runLive(ctx context.Context, b *broker.Broker, stream, wsURL, cursorFile string, publishIdentity bool) {
	cur := loadCursor(cursorFile)
	a.log.Info("live ingest", "url", wsURL, "cursor", cur.Time, "cursor_file", cursorFile)

	// First event after EVERY dial may be the inclusive-cursor redelivery.
	first := true
	var (
		conn *ingest.Conn
		n    int
	)

	dial := func() error {
		u := wsURLWithCursor(wsURL, cur.Time)
		c, err := ingest.Dial(ctx, u, classify.TypeColSet)
		if err != nil {
			return err
		}
		conn = c
		first = true
		return nil
	}

	if err := dial(); err != nil {
		a.log.Error("dial", "err", err)
	}

	for ctx.Err() == nil {
		if conn == nil {
			if err := dial(); err != nil {
				a.log.Error("dial retry", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
				continue
			}
			a.log.Info("reconnected", "cursor", cur.Time)
		}

		events, err := conn.NextEvents(ctx)
		if err != nil {
			a.log.Warn("read error", "err", err)
			conn.Close()
			conn = nil
			a.ready.Store(false)
			continue
		}
		a.ready.Store(true)

		for _, ev := range events {
			// Inclusive cursor: the first event after a dial is the last
			// one already processed — skip the duplicate (MsgId side guard
			// covers crash windows where cur was not yet persisted).
			if first && cur.Key != "" && ev.DedupeKey() == cur.Key {
				first = false
				continue
			}
			first = false

			_, ok := a.publishEvent(ctx, b, ev, publishIdentity)
			_ = ok

			// Cursor advances regardless of drop vs publish; only a publish
			// failure keeps it behind (so the event is replayed, deduped by MsgId).
			if ev.TimeUS > cur.Time {
				cur.Time = ev.TimeUS
				cur.Key = ev.DedupeKey()
				metrics.CursorTimeGauge.Set(float64(cur.Time))
				if n%250 == 0 {
					if err := saveCursor(cursorFile, cur); err != nil {
						a.log.Error("cursor save", "err", err)
					}
				}
				n++
			}
		}
	}

	// Flush the cursor once more on graceful shutdown.
	if cursorFile != "" {
		if err := saveCursor(cursorFile, cur); err != nil {
			a.log.Error("cursor save final", "err", err)
		}
	}
	a.log.Info("ingest stopped", "final_cursor", cur.Time)
}

// runReplay feeds a committed fixture through the same publish path — the
// offline, deterministic demo/test mode.
func (a *app) runReplay(ctx context.Context, b *broker.Broker, stream, path string, publishIdentity bool) {
	f, err := os.Open(path)
	if err != nil {
		a.log.Error("open replay", "err", err)
		os.Exit(1)
	}
	defer f.Close()
	a.ready.Store(true)

	dec := json.NewDecoder(f)
	total := 0
	var start time.Time
	for ctx.Err() == nil {
		var ev ingest.Event
		if err := dec.Decode(&ev); err != nil {
			break // EOF or malformed tail
		}
		if total == 0 {
			start = time.Now()
		}
		if _, ok := a.publishEvent(ctx, b, &ev, publishIdentity); !ok && ctx.Err() == nil {
			a.log.Error("replay publish failed")
		}
		total++
	}
	a.log.Info("replay done", "events", total, "elapsed", time.Since(start).Round(time.Millisecond))
}
