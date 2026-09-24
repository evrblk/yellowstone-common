package middleware

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMonitoringMiddleware_Unary(t *testing.T) {
	m := NewMonitoringMiddleware("testsvc", nil)
	reg := prometheus.NewRegistry()
	m.Register(reg)

	info := &grpc.UnaryServerInfo{FullMethod: "/testsvc.v0.TestApi/DoThing"}

	okHandler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	if _, err := m.Unary(context.Background(), nil, info, okHandler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	failHandler := func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.NotFound, "nope")
	}
	if _, err := m.Unary(context.Background(), nil, info, failHandler); err == nil {
		t.Fatal("expected an error")
	}

	if got := testutil.ToFloat64(m.totalRequests.WithLabelValues("DoThing")); got != 2 {
		t.Errorf("total requests = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.failedRequests.WithLabelValues("DoThing", "not_found")); got != 1 {
		t.Errorf("failed requests = %v, want 1", got)
	}
	if count := testutil.CollectAndCount(m.requestsDuration); count != 1 {
		t.Errorf("requestsDuration series count = %d, want 1", count)
	}
}

func TestNewMonitoringMiddleware_NamespacesMetrics(t *testing.T) {
	m := NewMonitoringMiddleware("moab", nil)
	reg := prometheus.NewRegistry()
	m.Register(reg)

	// Vec collectors only report label combinations that have been created,
	// so force one of each into existence before gathering.
	m.totalRequests.WithLabelValues("DoThing")
	m.failedRequests.WithLabelValues("DoThing", "not_found")
	m.requestsDuration.WithLabelValues("DoThing")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	names := make(map[string]bool)
	for _, mf := range families {
		names[mf.GetName()] = true
	}

	for _, want := range []string{
		"moab_server_requests_total",
		"moab_server_requests_failed",
		"moab_server_request_duration_seconds",
	} {
		if !names[want] {
			t.Errorf("missing metric family %q, got %v", want, names)
		}
	}
}

func TestMonitoringMiddleware_Logging(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	m := NewMonitoringMiddleware("testsvc", logger)
	m.Register(prometheus.NewRegistry())

	info := &grpc.UnaryServerInfo{FullMethod: "/testsvc.v0.TestApi/DoThing"}

	okHandler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	if _, err := m.Unary(context.Background(), nil, info, okHandler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertLogLine(t, lastLine(&buf), "level=INFO", "msg=\"rpc request\"", "method=DoThing", "status=OK", "duration=")

	buf.Reset()
	notFoundHandler := func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.NotFound, "no such thing")
	}
	if _, err := m.Unary(context.Background(), nil, info, notFoundHandler); err == nil {
		t.Fatal("expected an error")
	}
	line := lastLine(&buf)
	assertLogLine(t, line, "level=INFO", "status=NotFound", `error="no such thing"`)
	if strings.Contains(line, "rpc error") {
		t.Errorf("log line %q still contains the raw gRPC error wrapping", line)
	}

	buf.Reset()
	internalHandler := func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.Internal, "db connection to 10.0.0.7:5432 refused")
	}
	resp, err := m.Unary(context.Background(), nil, info, internalHandler)
	if err == nil {
		t.Fatal("expected an error")
	}
	if resp != nil {
		t.Errorf("expected nil response, got %v", resp)
	}

	line = lastLine(&buf)
	assertLogLine(t, line, "level=ERROR", "status=Internal", `error="db connection to 10.0.0.7:5432 refused"`)

	// The real message must reach the log, but never the caller.
	if got := err.Error(); strings.Contains(got, "10.0.0.7") || strings.Contains(got, "db connection") {
		t.Errorf("internal error details leaked to the caller: %v", got)
	}
	if got := status.Convert(err).Message(); got != "internal error" {
		t.Errorf("caller-facing message = %q, want %q", got, "internal error")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("caller-facing code = %v, want %v", got, codes.Internal)
	}
}

func TestNewMonitoringMiddleware_NilLoggerDefaultsToSlogDefault(t *testing.T) {
	m := NewMonitoringMiddleware("testsvc", nil)
	if m.logger != slog.Default() {
		t.Error("expected nil logger to fall back to slog.Default()")
	}
}

func lastLine(buf *bytes.Buffer) string {
	s := strings.TrimRight(buf.String(), "\n")
	lines := strings.Split(s, "\n")
	return lines[len(lines)-1]
}

func assertLogLine(t *testing.T, line string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(line, w) {
			t.Errorf("log line %q missing %q", line, w)
		}
	}
}
