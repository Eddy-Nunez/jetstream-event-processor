// Package metrics centralizes the prometheus counters both binaries register.
// Names are prefixed eventworker_; each binary is a separate process so the
// same names are safe across ingest/worker.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// EventsTotal counts every classified event by resolved type
	// (post/like/repost/follow/delete/identity/account/drop).
	EventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "eventworker_events_total",
		Help: "Events classified by fan-out type.",
	}, []string{"type"})

	// PublishTotal counts events actually published to JetStream, by type.
	PublishTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "eventworker_publish_total",
		Help: "Events published to JetStream by type.",
	}, []string{"type"})

	// DropTotal counts events dropped by policy, by drop kind
	// (identity|account|unknown — for ingress Drop resolution).
	DropTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "eventworker_drop_total",
		Help: "Events dropped by policy (drop-at-publish / unknown route).",
	}, []string{"kind"})

	// AlertsTotal counts worker alerts emitted, by type.
	AlertsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "eventworker_alerts_total",
		Help: "Worker alerts emitted by type.",
	}, []string{"type"})

	// ErrorsTotal counts processing/publish errors.
	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "eventworker_errors_total",
		Help: "Total processing/publish errors.",
	})

	// CursorTimeGauge is the last persisted ingest cursor (time_us).
	CursorTimeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "eventworker_cursor_time_us",
		Help: "Last persisted ingest cursor (Jetstream time_us).",
	})

	// InFlightGauge is the worker's currently in-flight message count.
	InFlightGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "eventworker_inflight",
		Help: "Messages currently being processed by the worker.",
	})
)

var initOnce sync.Once

// Init registers all metrics; idempotent (tests may call more than once).
func Init() {
	initOnce.Do(func() {
		prometheus.MustRegister(
			EventsTotal, PublishTotal, DropTotal,
			AlertsTotal, ErrorsTotal,
			CursorTimeGauge, InFlightGauge,
		)
	})
}
