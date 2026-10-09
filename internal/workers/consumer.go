// Package workers implements the generic per-type JetStream pull consumer
// (Tasks 3.1/3.3) and the real per-type handlers (Task 3.2): posts
// keyword/language matching, likes/reposts rolling-window counters, follows
// burst detection, and deletes TTL tombstones.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"eventworker/internal/broker"
	"eventworker/internal/ingest"
	"eventworker/internal/metrics"
)

// fetchWait bounds each pull fetch: when the queue is idle the consumer
// polls once per fetchWait, which also bounds shutdown latency.
const fetchWait = 300 * time.Millisecond

// Handler processes one decoded event. Implementations must be safe for
// concurrent use — the consumer runs up to `concurrency` handler goroutines.
type Handler interface {
	Handle(ctx context.Context, ev *ingest.Event) error
}

// ConsumerConfig configures a Consumer.
type ConsumerConfig struct {
	Subject     string // live subject to pull (events.<type>)
	DLQSubject  string // dead-letter subject (events.dlq.<type>)
	Durable     string // durable consumer name (reused across restarts)
	Concurrency int    // max in-flight messages (maxAckPending)
	MaxDeliver  int    // per-message delivery cap before DLQ
	NakDelay    time.Duration
	Handler     Handler
}

// Consumer is a generic bounded pull consumer: it fetches up to
// `concurrency` messages per pull, processes each asynchronously under a
// concurrency semaphore, acks on success, naks (with deferral) on transient
// failure, and moves a message to its DLQ subject once it exhausts
// MaxDeliver. Context cancellation stops the fetch loop and waits for
// in-flight processing to finish before returning, so graceful shutdown
// never loses or redelivers acknowledged work.
type Consumer struct {
	log         *slog.Logger
	b           *broker.Broker
	sub         *nats.Subscription
	subject     string
	durable     string
	dlqSubject  string
	handler     Handler
	concurrency int
	maxDeliver  int
	nakDelay    time.Duration
	sem         chan struct{}
	wg          sync.WaitGroup
}

// NewConsumer binds a durable pull consumer on cfg.Subject and returns a
// Consumer ready to Run. The broker must have EnsureStream called first.
func NewConsumer(b *broker.Broker, cfg ConsumerConfig) (*Consumer, error) {
	if b == nil {
		return nil, errors.New("workers: nil broker")
	}
	if cfg.Subject == "" || cfg.DLQSubject == "" {
		return nil, errors.New("workers: subject and dlq subject are required")
	}
	if cfg.Handler == nil {
		return nil, errors.New("workers: handler is required")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 16
	}
	if cfg.MaxDeliver <= 0 {
		cfg.MaxDeliver = 5
	}
	if cfg.NakDelay <= 0 {
		cfg.NakDelay = time.Second
	}
	sub, err := b.PullConsumer(cfg.Subject, cfg.Durable, cfg.Concurrency)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		log:         slog.Default(),
		b:           b,
		sub:         sub,
		subject:     cfg.Subject,
		durable:     cfg.Durable,
		dlqSubject:  cfg.DLQSubject,
		handler:     cfg.Handler,
		concurrency: cfg.Concurrency,
		maxDeliver:  cfg.MaxDeliver,
		nakDelay:    cfg.NakDelay,
		sem:         make(chan struct{}, cfg.Concurrency),
	}, nil
}

// Run pulls and dispatches until ctx is canceled, then drains in-flight
// work and returns nil.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.sub.Drain()

	c.log.Info("worker consumer started", "subject", c.subject, "durable", c.durable, "concurrency", c.concurrency)
	for {
		if ctx.Err() != nil {
			return c.drain(ctx)
		}
		msgs, err := c.sub.Fetch(c.concurrency, nats.MaxWait(fetchWait))
		if err != nil {
			// Expected on an idle queue.
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			c.log.Error("consumer fetch", "err", err)
			select {
			case <-ctx.Done():
				return c.drain(ctx)
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		for _, m := range msgs {
			// Bound in-flight work: acquire a concurrency slot before
			// dispatching; on shutdown, nak undelivered messages so the
			// server redelivers them promptly instead of after AckWait.
			select {
			case c.sem <- struct{}{}:
				c.wg.Add(1)
				go c.process(ctx, m)
			case <-ctx.Done():
				if err := m.NakWithDelay(c.nakDelay); err != nil {
					c.log.Debug("consumer nak undelivered", "err", err)
				}
				return c.drain(ctx)
			}
		}
	}
}

// drain waits for in-flight processing to finish and ack, so graceful
// shutdown loses no work.
func (c *Consumer) drain(ctx context.Context) error {
	c.log.Info("consumer stopping", "subject", c.subject)
	c.wg.Wait()
	c.log.Info("consumer stopped", "subject", c.subject)
	return nil
}

// process runs one message through the handler under the consumer's
// concurrency budget, acking after success and applying retry/DLQ semantics
// after failure.
func (c *Consumer) process(ctx context.Context, m *nats.Msg) {
	metrics.InFlightGauge.Inc()
	defer func() {
		metrics.InFlightGauge.Dec()
		<-c.sem
		c.wg.Done()
	}()

	var ev ingest.Event
	err := json.Unmarshal(m.Data, &ev)
	if err == nil {
		err = c.handler.Handle(ctx, &ev)
	}
	if err != nil {
		metrics.ErrorsTotal.Inc()
		c.retryOrDLQ(m, err)
		return
	}
	if err := m.Ack(); err != nil {
		c.log.Error("consumer ack", "err", err)
	}
}

// retryOrDLQ implements ack semantics: transient failures are nacked with a
// short deferral; a message that exhausts MaxDeliver is moved to the
// per-type DLQ subject and acked (terminated).
func (c *Consumer) retryOrDLQ(m *nats.Msg, cause error) {
	meta, err := m.Metadata()
	if err != nil {
		c.log.Error("consumer metadata", "err", err)
		_ = m.NakWithDelay(c.nakDelay)
		return
	}
	delivered := int(meta.NumDelivered)
	if delivered < c.maxDeliver {
		c.log.Warn("worker processing error, retrying", "subject", c.subject, "delivered", delivered, "err", cause)
		if err := m.NakWithDelay(c.nakDelay); err != nil {
			c.log.Error("consumer nak", "err", err)
		}
		return
	}

	c.log.Error("worker max deliveries exceeded, moving to DLQ", "subject", c.subject, "dlq", c.dlqSubject, "delivered", delivered, "err", cause)
	dlqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.b.PublishDLQ(dlqCtx, c.dlqSubject, m.Data); err != nil {
		// Could not record the failure; keep the delivery alive so the
		// attempt repeats (NumDelivered keeps climbing, the DLQ is retried).
		c.log.Error("consumer publish dlq", "err", err)
		metrics.ErrorsTotal.Inc()
		_ = m.NakWithDelay(c.nakDelay)
		return
	}
	_ = m.Ack()
}
