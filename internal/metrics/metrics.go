// Package metrics defines the Prometheus instruments used by the service.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Charge outcome label values.
const (
	OutcomeSucceeded = "succeeded"
	OutcomeDeclined  = "declined"
	OutcomeReplayed  = "replayed"
	OutcomeConflict  = "key_conflict"
	OutcomeRejected  = "rejected"
)

type Metrics struct {
	Registry *prometheus.Registry

	RPCRequests *prometheus.CounterVec   // method, code
	RPCLatency  *prometheus.HistogramVec // method
	RPCInFlight prometheus.Gauge

	ChargeOutcomes       *prometheus.CounterVec // outcome
	IdempotentReplays    *prometheus.CounterVec // method
	IdempotencyConflicts *prometheus.CounterVec // method
}

func New() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { r.MustRegister(c) }

	m := &Metrics{
		Registry: r,
		RPCRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "billing_grpc_requests_total", Help: "Completed gRPC requests by method and status code.",
		}, []string{"method", "code"}),
		RPCLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "billing_grpc_request_duration_seconds", Help: "gRPC request latency by method.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"method"}),
		RPCInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "billing_grpc_requests_in_flight", Help: "gRPC requests currently being served.",
		}),
		ChargeOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "billing_charge_outcomes_total", Help: "ChargeInvoice outcomes: succeeded, declined, replayed, key_conflict, rejected.",
		}, []string{"outcome"}),
		IdempotentReplays: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "billing_idempotent_replays_total", Help: "Requests answered from a stored idempotency record.",
		}, []string{"method"}),
		IdempotencyConflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "billing_idempotency_key_conflicts_total", Help: "Requests rejected for reusing a key with a different payload.",
		}, []string{"method"}),
	}
	f(m.RPCRequests)
	f(m.RPCLatency)
	f(m.RPCInFlight)
	f(m.ChargeOutcomes)
	f(m.IdempotentReplays)
	f(m.IdempotencyConflicts)
	return m
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
