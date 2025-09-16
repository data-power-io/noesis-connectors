// Package metrics provides Prometheus metrics for connectors
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Connection metrics
	ConnectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_connections_total",
			Help: "Total number of connection attempts",
		},
		[]string{"connector", "tenant", "status"},
	)

	ConnectionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "connector_connection_duration_seconds",
			Help:    "Time taken to establish connections",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"connector", "tenant"},
	)

	// Session metrics
	SessionsActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "connector_sessions_active",
			Help: "Number of active sessions",
		},
		[]string{"connector", "tenant"},
	)

	SessionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "connector_session_duration_seconds",
			Help:    "Duration of connector sessions",
			Buckets: []float64{1, 5, 10, 30, 60, 300, 600, 1800, 3600},
		},
		[]string{"connector", "tenant"},
	)

	// Data extraction metrics
	RecordsExtracted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_records_extracted_total",
			Help: "Total number of records extracted",
		},
		[]string{"connector", "tenant", "entity"},
	)

	BytesExtracted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_bytes_extracted_total",
			Help: "Total bytes extracted",
		},
		[]string{"connector", "tenant", "entity"},
	)

	ExtractionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "connector_extraction_duration_seconds",
			Help:    "Time taken for data extraction",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300, 600},
		},
		[]string{"connector", "tenant", "entity", "mode"},
	)

	// Error metrics
	ErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_errors_total",
			Help: "Total number of errors",
		},
		[]string{"connector", "tenant", "type", "entity"},
	)

	// API call metrics
	APICallsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_api_calls_total",
			Help: "Total number of API calls made to source systems",
		},
		[]string{"connector", "tenant", "endpoint", "status"},
	)

	APICallDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "connector_api_call_duration_seconds",
			Help:    "Duration of API calls to source systems",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"connector", "tenant", "endpoint"},
	)

	// Rate limiting metrics
	RateLimitHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_rate_limit_hits_total",
			Help: "Total number of rate limit hits",
		},
		[]string{"connector", "tenant"},
	)

	RateLimitDelay = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "connector_rate_limit_delay_seconds",
			Help:    "Time delayed due to rate limiting",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120},
		},
		[]string{"connector", "tenant"},
	)
)

// ConnectorMetrics provides a convenient interface for recording connector metrics
type ConnectorMetrics struct {
	connectorName string
	tenantID      string
}

// NewConnectorMetrics creates a new metrics recorder for a connector
func NewConnectorMetrics(connectorName, tenantID string) *ConnectorMetrics {
	return &ConnectorMetrics{
		connectorName: connectorName,
		tenantID:      tenantID,
	}
}

// RecordConnection records connection metrics
func (m *ConnectorMetrics) RecordConnection(status string, duration time.Duration) {
	ConnectionsTotal.WithLabelValues(m.connectorName, m.tenantID, status).Inc()
	ConnectionDuration.WithLabelValues(m.connectorName, m.tenantID).Observe(duration.Seconds())
}

// RecordSessionStart records the start of a session
func (m *ConnectorMetrics) RecordSessionStart() {
	SessionsActive.WithLabelValues(m.connectorName, m.tenantID).Inc()
}

// RecordSessionEnd records the end of a session
func (m *ConnectorMetrics) RecordSessionEnd(duration time.Duration) {
	SessionsActive.WithLabelValues(m.connectorName, m.tenantID).Dec()
	SessionDuration.WithLabelValues(m.connectorName, m.tenantID).Observe(duration.Seconds())
}

// RecordExtraction records data extraction metrics
func (m *ConnectorMetrics) RecordExtraction(entity string, mode string, records int64, bytes int64, duration time.Duration) {
	RecordsExtracted.WithLabelValues(m.connectorName, m.tenantID, entity).Add(float64(records))
	BytesExtracted.WithLabelValues(m.connectorName, m.tenantID, entity).Add(float64(bytes))
	ExtractionDuration.WithLabelValues(m.connectorName, m.tenantID, entity, mode).Observe(duration.Seconds())
}

// RecordError records an error
func (m *ConnectorMetrics) RecordError(errorType string, entity string) {
	ErrorsTotal.WithLabelValues(m.connectorName, m.tenantID, errorType, entity).Inc()
}

// RecordAPICall records an API call
func (m *ConnectorMetrics) RecordAPICall(endpoint string, status string, duration time.Duration) {
	APICallsTotal.WithLabelValues(m.connectorName, m.tenantID, endpoint, status).Inc()
	APICallDuration.WithLabelValues(m.connectorName, m.tenantID, endpoint).Observe(duration.Seconds())
}

// RecordRateLimit records rate limiting events
func (m *ConnectorMetrics) RecordRateLimit(delay time.Duration) {
	RateLimitHits.WithLabelValues(m.connectorName, m.tenantID).Inc()
	RateLimitDelay.WithLabelValues(m.connectorName, m.tenantID).Observe(delay.Seconds())
}

// Timer is a helper for measuring duration
type Timer struct {
	start time.Time
}

// NewTimer creates a new timer
func NewTimer() *Timer {
	return &Timer{start: time.Now()}
}

// Duration returns the elapsed time since the timer was created
func (t *Timer) Duration() time.Duration {
	return time.Since(t.start)
}

// Stop returns the elapsed time and can be used to stop timing
func (t *Timer) Stop() time.Duration {
	return time.Since(t.start)
}
