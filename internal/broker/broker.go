// Package broker owns the NATS JetStream fan-out fabric (PLAN §1, Task 2.2).
// The router (internal/classify) decides WHAT lands on a subject; this package
// makes the subject durable, deduped, and retryable: stream EVENTS covering
// the events namespace, publish with Nats-Msg-Id, and a durable pull-consumer
// factory. It never routes — it only fans out.
//
// Subject layout: live subjects events.<type>; dead-letter subjects
// events.dlq.<type>. JetStream consumer filters behave as prefixes (filter
// events.post also matches events.post.dlq), so keeping DLQs on events.dlq.*
// guarantees an events.<type> consumer never sees its own dead letters.
package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// DefaultURL matches the local dev-loop default (nats-server -js on 4222).
const DefaultURL = "nats://127.0.0.1:4222"

// StreamConfig describes the EVENTS stream. The stream covers the whole
// events namespace — live subjects events.* AND the per-type DLQ subjects
// events.<type>.dlq (NATS `>` matches the remaining tokens, so one pattern
// covers both).
type StreamConfig struct {
	Name     string        // e.g. "EVENTS"
	Subjects []string      // e.g. ["events.>"]
	MaxAge   time.Duration // replay window
}

func (c StreamConfig) withDefaults() StreamConfig {
	if c.Name == "" {
		c.Name = "EVENTS"
	}
	if len(c.Subjects) == 0 {
		c.Subjects = []string{"events.>"}
	}
	if c.MaxAge == 0 {
		c.MaxAge = time.Hour
	}
	return c
}

// DedupeWindow is the NATS server-side dedupe window; Jetstream discards a
// published message whose Nats-Msg-Id was seen within this window. 2 minutes
// comfortably covers reconnect/resume races.
const DedupeWindow = 2 * time.Minute

// Broker is a connected JetStream client scoped to one stream.
type Broker struct {
	nc  *nats.Conn
	js  nats.JetStreamContext
	cfg StreamConfig
}

// Connect dials NATS with reconnect enabled (worker/ingest survive broker
// restarts) and returns a JetStream-context broker. Call EnsureStream before
// publishing or consuming.
func Connect(ctx context.Context, url string) (*Broker, error) {
	if url == "" {
		url = DefaultURL
	}
	nc, err := nats.Connect(url,
		nats.Name("eventworker"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
		nats.Timeout(3*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("broker connect %s: %w", url, err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("broker jetstream: %w", err)
	}
	return &Broker{nc: nc, js: js}, nil
}

// Close closes the underlying NATS connection.
func (b *Broker) Close() { b.nc.Close() }

// JetStream exposes the context for advanced uses (e.g. workers building
// consumers / publishing DLQ messages).
func (b *Broker) JetStream() nats.JetStreamContext { return b.js }

// EnsureStream creates the EVENTS stream if absent (idempotent). Existing
// streams are left untouched, so an operator-tuned MaxAge wins.
func (b *Broker) EnsureStream(ctx context.Context, cfg StreamConfig) error {
	b.cfg = cfg.withDefaults()
	if _, err := b.js.StreamInfo(b.cfg.Name); err == nil {
		return nil
	}
	_, err := b.js.AddStream(&nats.StreamConfig{
		Name:     b.cfg.Name,
		Subjects: b.cfg.Subjects,
		Storage:  nats.FileStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:   b.cfg.MaxAge,
		Discard:  nats.DiscardOld,
		// unlimited per-subject history: workers consume at their own pace and
		// backpressure absorbs spikes instead of dropping events.
		MaxMsgsPerSubject: -1,
	}, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("ensure stream %s: %w", b.cfg.Name, err)
	}
	return nil
}

// Publish sends payload to subject with Nats-Msg-Id=key so Jetstream
// deduplicates server-side under redelivery/resume races (broker spec:
// "Server-side publish dedupe").
func (b *Broker) Publish(ctx context.Context, subject, key string, payload []byte) error {
	if _, err := b.js.Publish(subject, payload,
		nats.MsgId(key),
		// MsgId dedupe only applies within this window; the Nats-Msg-Id header
		// is what Jetstream keys on.
	); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}

// PublishDLQ moves a failed message to its dead-letter subject (the
// events.dlq.<type> namespace, inside the stream). The message keeps its
// original payload; delivery metadata/headers are dropped.
func (b *Broker) PublishDLQ(ctx context.Context, subject string, payload []byte) error {
	if _, err := b.js.Publish(subject, payload); err != nil {
		return fmt.Errorf("publish dlq %s: %w", subject, err)
	}
	return nil
}

// PullConsumer returns a durable pull subscription bound to the EVENTS stream
// with explicit-ack policy and the filter subject derived from `subject`
// (created via PullSubscribe options so the consumer's FilterSubject matches
// exactly — a consumer on events.post must never see events.*.dlq messages).
// The worker owns delivery semantics (bounded concurrency, MaxDeliver → DLQ,
// graceful shutdown). On restart the durable is reused (retained config wins).
func (b *Broker) PullConsumer(subject, durable string, maxAckPending int) (*nats.Subscription, error) {
	if b.cfg.Name == "" {
		return nil, fmt.Errorf("broker: EnsureStream must be called before PullConsumer")
	}
	sub, err := b.js.PullSubscribe(subject, durable,
		nats.BindStream(b.cfg.Name),
		nats.AckExplicit(),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(-1), // worker routes to DLQ via delivery metadata
		nats.MaxAckPending(maxAckPending),
		nats.DeliverAll(),
	)
	if err != nil {
		return nil, fmt.Errorf("pull subscribe %s: %w", subject, err)
	}
	return sub, nil
}
