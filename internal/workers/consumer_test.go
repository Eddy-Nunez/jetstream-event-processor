// consumer_test — hermetic delivery-semantics tests for the generic pull
// consumer (embedded nats-server, no external dependency).
package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"eventworker/internal/broker"
	"eventworker/internal/ingest"
	"eventworker/internal/metrics"
)

func startJetStream(t *testing.T) *server.Server {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func newTestBroker(t *testing.T, srv *server.Server) *broker.Broker {
	t.Helper()
	b, err := broker.Connect(context.Background(), srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(b.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.EnsureStream(ctx, broker.StreamConfig{}); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}
	return b
}

// errHandler always fails processing; used to push a message to the DLQ.
type errHandler struct{}

func (errHandler) Handle(context.Context, *ingest.Event) error {
	return errors.New("always failing handler")
}

// TestConsumerMaxDeliverToDLQ verifies the retry path: a message that always
// fails is nacked with a delay, redelivered, then moved to its per-type DLQ
// subject once NumDelivered exhausts MaxDeliver and acked (terminated).
func TestConsumerMaxDeliverToDLQ(t *testing.T) {
	metrics.Init()

	srv := startJetStream(t)
	b := newTestBroker(t, srv)

	payload := []byte(`{"did":"did:plc:probe","time_us":1,"kind":"commit","commit":{"rev":"r1","operation":"create","collection":"app.bsky.graph.follow","rkey":"k1","record":{"$type":"app.bsky.graph.follow","subject":"did:plc:target"}}}`)

	ctx := context.Background()
	if err := b.Publish(ctx, "events.follow", "probe-1", payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	c, err := NewConsumer(b, ConsumerConfig{
		Subject:     "events.follow",
		DLQSubject:  "events.dlq.follow",
		Durable:     "workers-dlq-test",
		Concurrency: 2,
		MaxDeliver:  2,
		NakDelay:    100 * time.Millisecond,
		Handler:     errHandler{},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	cctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(cctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	// Poll the DLQ until the exhausted message lands there: delivery #1 →
	// nak(100ms) → delivery #2 → DLQ publish + ack, comfortably inside 10s.
	probe, err := b.PullConsumer("events.dlq.follow", "workers-dlq-probe", 4)
	if err != nil {
		t.Fatalf("dlq consumer: %v", err)
	}
	defer probe.Drain()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		msgs, err := probe.Fetch(1, nats.MaxWait(time.Second))
		switch {
		case err == nil && len(msgs) > 0:
			if string(msgs[0].Data) != string(payload) {
				t.Fatalf("dlq payload mismatch: got %s", msgs[0].Data)
			}
			_ = msgs[0].Ack()
			return
		case errors.Is(err, nats.ErrTimeout):
			// keep polling
		case err != nil:
			t.Fatalf("dlq fetch: %v", err)
		}
	}
	t.Fatal("message did not reach the DLQ within 10s")
}
