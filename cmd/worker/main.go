// Command worker is the generic per-type JetStream pull consumer
// (PLAN §1, Tasks 3.1–3.3). One deployment per type
// (posts|likes|reposts|follows|deletes): it pulls only its type's live
// subject, applies the real per-type logic in internal/workers, moves poison
// messages to its own events.dlq.<type> subject after MaxDeliver, and shuts
// down gracefully (stops fetching, finishes and acks in-flight work).
package main

import (
	"context"
	"errors"
	"flag"
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
	"eventworker/internal/metrics"
	"eventworker/internal/workers"
)

func main() {
	natsURL := flag.String("nats", broker.DefaultURL, "NATS JetStream URL")
	stream := flag.String("stream", "EVENTS", "JetStream stream name")
	typeName := flag.String("type", "", "worker type: posts|likes|reposts|follows|deletes (required)")
	concurrency := flag.Int("concurrency", 16, "bounded in-flight messages (maxAckPending)")
	maxDeliver := flag.Int("maxdeliver", 5, "deliveries before a message moves to the DLQ")
	httpAddr := flag.String("http", ":8090", "health/metrics listen address")
	durable := flag.String("durable", "", "durable consumer name (default EVENTS-<type>)")
	keywords := flag.String("keywords", "", "comma-separated case-insensitive post keywords")
	langs := flag.String("langs", "", "comma-separated post languages")
	likeThreshold := flag.Int("like-threshold", 200, "like alerts per uri per window")
	repostThreshold := flag.Int("repost-threshold", 150, "repost alerts per uri per window")
	followBurst := flag.Int("follow-burst", 30, "follow burst alerts per target per window")
	window := flag.Duration("window", 60*time.Second, "rolling window for like/repost/follow counters")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(log)

	if *typeName == "" {
		log.Error("worker type required", "flag", "-type", "values", "posts|likes|reposts|follows|deletes")
		os.Exit(1)
	}
	typ, ok := classify.TypeForWorkerName(*typeName)
	if !ok {
		log.Error("unknown worker type", "type", *typeName)
		os.Exit(1)
	}

	durableName := *durable
	if durableName == "" {
		durableName = "EVENTS-" + *typeName
	}
	dlqSubject := "events.dlq." + *typeName
	liveSubject := typ.Subject()

	handler, err := workers.NewHandler(*typeName, workers.Config{
		Keywords:        splitCSV(*keywords),
		Langs:           splitCSV(*langs),
		LikeThreshold:   *likeThreshold,
		RepostThreshold: *repostThreshold,
		FollowBurst:     *followBurst,
		Window:          *window,
	})
	if err != nil {
		log.Error("handler", "err", err)
		os.Exit(1)
	}

	metrics.Init()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	b, err := broker.ConnectRetry(ctx, *natsURL, 10, 500*time.Millisecond)
	if err != nil {
		log.Error("broker connect", "err", err)
		os.Exit(1)
	}
	defer b.Close()

	if err := b.EnsureStream(ctx, broker.StreamConfig{Name: *stream, MaxAge: time.Hour}); err != nil {
		log.Error("ensure stream", "err", err)
		os.Exit(1)
	}

	consumer, err := workers.NewConsumer(b, workers.ConsumerConfig{
		Subject:     liveSubject,
		DLQSubject:  dlqSubject,
		Durable:     durableName,
		Concurrency: *concurrency,
		MaxDeliver:  *maxDeliver,
		Handler:     handler,
	})
	if err != nil {
		log.Error("consumer", "err", err)
		os.Exit(1)
	}

	var ready atomic.Bool
	mux := health.Handler(func() error {
		if !ready.Load() {
			return errors.New("consumer not running")
		}
		return nil
	})
	srv := &http.Server{Addr: *httpAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		ready.Store(true)
		if err := consumer.Run(ctx); err != nil {
			log.Error("consumer run", "err", err)
		}
		ready.Store(false)
	}()

	log.Info("worker started",
		"type", *typeName, "subject", liveSubject, "dlq", dlqSubject,
		"durable", durableName, "concurrency", *concurrency, "maxdeliver", *maxDeliver,
		"http", *httpAddr,
	)

	<-ctx.Done()
	stop() // restore default signal behavior so a second signal kills us

	log.Info("worker shutting down", "type", *typeName)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	<-done // consumer drained in-flight work
	b.Close()
	log.Info("worker stopped", "type", *typeName)
}

// splitCSV parses a comma-separated flag value, trimming and dropping empties.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
