package middleware

import (
	"context"
	"log/slog"
	"path"
	"time"

	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MonitoringMiddleware is a gRPC unary interceptor recording per-method
// request counts, failure counts, and latency, and logging one line per
// request (method, status, latency). Each server that mounts it picks its
// own serviceName (lowercase, e.g. "moab", "grackle") so the resulting
// Prometheus metric names don't collide across services sharing a registry.
type MonitoringMiddleware struct {
	logger *slog.Logger

	totalRequests    *prometheus.CounterVec
	failedRequests   *prometheus.CounterVec
	requestsDuration *prometheus.HistogramVec
}

// NewMonitoringMiddleware builds a MonitoringMiddleware that logs to logger,
// or to slog.Default() if logger is nil.
func NewMonitoringMiddleware(serviceName string, logger *slog.Logger) *MonitoringMiddleware {
	if logger == nil {
		logger = slog.Default()
	}
	return &MonitoringMiddleware{
		logger: logger,
		totalRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: serviceName,
			Name:      "server_requests_total",
			Help:      "Total number of requests",
		}, []string{"method"}),
		failedRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: serviceName,
			Name:      "server_requests_failed",
			Help:      "Number of failed requests",
		}, []string{"method", "error"}),
		requestsDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:                       serviceName,
			Name:                            "server_request_duration_seconds",
			Help:                            "Request duration",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"method"}),
	}
}

// Register registers this middleware's collectors with reg. Call once per
// process, before serving traffic.
func (m *MonitoringMiddleware) Register(reg prometheus.Registerer) {
	reg.MustRegister(m.totalRequests, m.failedRequests, m.requestsDuration)
}

func (m *MonitoringMiddleware) Unary(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, err error) {
	method := path.Base(info.FullMethod)
	start := time.Now()

	m.totalRequests.WithLabelValues(method).Inc()

	defer func() {
		duration := time.Since(start)
		m.requestsDuration.WithLabelValues(method).Observe(duration.Seconds())

		attrs := []any{"method", method, "status", status.Code(err).String(), slog.Duration("duration", duration)}

		category, message := metrics.FromGrpcError(err)
		switch {
		case category == "":
			m.logger.Info("rpc request", attrs...)
		case category == "internal":
			m.failedRequests.WithLabelValues(method, category).Inc()
			m.logger.Error("rpc request", append(attrs, "error", message)...)

			// The real message is logged above; the caller only gets the
			// code, so internal implementation details never leak in the
			// response.
			err = status.Error(codes.Internal, "internal error")
		default:
			m.failedRequests.WithLabelValues(method, category).Inc()
			m.logger.Info("rpc request", append(attrs, "error", message)...)
		}
	}()

	resp, err = handler(ctx, req)
	return resp, err
}
