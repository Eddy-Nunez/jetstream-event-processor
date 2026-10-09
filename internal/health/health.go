// Package health serves the standard liveness/readiness/metrics trio
// (Task 2.4): /healthz always 200 while the process runs; /readyz reflects a
// caller-supplied readiness func; /metrics exposes prometheus.
package health

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadyFunc reports readiness; returning non-nil marks the process not-ready.
type ReadyFunc func() error

// Handler returns an http.Handler serving /healthz, /readyz, /metrics.
func Handler(ready ReadyFunc) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready != nil && ready() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}
