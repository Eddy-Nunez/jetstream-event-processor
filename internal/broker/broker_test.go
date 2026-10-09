// broker_test — hermetic JetStream tests via an embedded nats-server
// (no external dependency; runs in CI). Covers the broker spec claims:
// stream topology, server-side MsgId dedupe, per-type DLQ namespace.
package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"eventworker/internal/broker"
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

func TestStreamTopology(t *testing.T) {
	srv := startJetStream(t)
	b := newTestBroker(t, srv)

	info, err := b.JetStream().StreamInfo("EVENTS")
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.Config.Name != "EVENTS" {
		t.Errorf("stream name = %s", info.Config.Name)
	}
	if info.Config.Storage != nats.FileStorage {
		t.Errorf("storage = %v, want file", info.Config.Storage)
	}
	wantSubjects := map[string]bool{"events.>": true}
	for _, s := range info.Config.Subjects {
		if !wantSubjects[s] {
			t.Errorf("unexpected stream subject %q", s)
		}
		delete(wantSubjects, s)
	}
	if len(wantSubjects) != 0 {
		t.Errorf("missing stream subjects: %v", wantSubjects)
	}
	// MaxAge applied
	if info.Config.MaxAge != time.Hour {
		t.Errorf("MaxAge = %v, want 1h", info.Config.MaxAge)
	}
}

func TestPublishDedupeServerSide(t *testing.T) {
	srv := startJetStream(t)
	b := newTestBroker(t, srv)

	ctx := context.Background()
	payload := []byte(`{"kind":"commit"}`)
	const key = "did:plc:x|app.bsky.feed.like|rkey1|create|rev1"

	// Publish the same logical event twice (e.g. a resume race).
	if err := b.Publish(ctx, "events.like", key, payload); err != nil {
		t.Fatalf("publish #1: %v", err)
	}
	if err := b.Publish(ctx, "events.like", key, payload); err != nil {
		t.Fatalf("publish #2: %v", err)
	}
	// And an event with a different key — must NOT be deduped away.
	if err := b.Publish(ctx, "events.like", key+"#2", []byte(`{"n":2}`)); err != nil {
		t.Fatalf("publish #3: %v", err)
	}

	info, err := b.JetStream().StreamInfo("EVENTS")
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if got := info.State.Msgs; got != 2 {
		t.Fatalf("stream msgs = %d, want 2 (one duplicate deduped server-side)", got)
	}
}

func TestPullConsumerDelivers(t *testing.T) {
	srv := startJetStream(t)
	b := newTestBroker(t, srv)

	ctx := context.Background()
	if err := b.Publish(ctx, "events.post", "k1", []byte(`{"n":1}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	sub, err := b.PullConsumer("events.post", "posts-test", 4)
	if err != nil {
		t.Fatalf("pull consumer: %v", err)
	}
	defer sub.Drain()

	msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Data) != `{"n":1}` {
		t.Fatalf("unexpected delivery: %d msgs", len(msgs))
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// Same durable consumer sees nothing new after ack.
	if _, err := sub.Fetch(1, nats.MaxWait(500*time.Millisecond)); err != nats.ErrTimeout {
		t.Fatalf("expected timeout after ack, got %v", err)
	}
}

func TestDLQNamespaceInStream(t *testing.T) {
	srv := startJetStream(t)
	b := newTestBroker(t, srv)

	// DLQ subjects are inside the stream (events.> scope) but on events.dlq.*
	// — a live consumer filtered on events.<type> must NOT receive them
	// (worker of one type never sees its own dead letters).
	ctx := context.Background()
	if err := b.PublishDLQ(ctx, "events.dlq.post", []byte(`{"dead":true}`)); err != nil {
		t.Fatalf("publish dlq: %v", err)
	}

	live, err := b.PullConsumer("events.post", "dlq-watch", 2)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	defer live.Drain()
	if _, err := live.Fetch(1, nats.MaxWait(400*time.Millisecond)); err != nats.ErrTimeout {
		t.Fatalf("live consumer saw a DLQ message: %v", err)
	}

	dlq, err := b.PullConsumer("events.dlq.post", "dlq-watch-dead", 2)
	if err != nil {
		t.Fatalf("pull dlq: %v", err)
	}
	defer dlq.Drain()
	msgs, err := dlq.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("dlq fetch: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Data) != `{"dead":true}` {
		t.Fatalf("dlq delivery wrong: %v", msgs)
	}
	_ = msgs[0].Ack()
}
