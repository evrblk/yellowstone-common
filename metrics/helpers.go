package metrics

import (
	"context"
	"fmt"
	"log"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func FromGrpcError(err error) string {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.OK:
			return ""

		case codes.DeadlineExceeded:
		case codes.Canceled:
			return "timeout"

		case codes.Aborted:
		case codes.FailedPrecondition:
		case codes.AlreadyExists:
		case codes.InvalidArgument:
		case codes.OutOfRange:
			return "invalid_request"

		case codes.Unknown:
		case codes.Unimplemented:
		case codes.Internal:
		case codes.Unavailable:
		case codes.DataLoss:
			return "internal"

		case codes.NotFound:
			return "not_found"

		case codes.PermissionDenied:
			return "permission_denied"

		case codes.ResourceExhausted:
			return "resource_exhausted"

		case codes.Unauthenticated:
			return "unauthenticated"
		}
	}

	return "internal"
}

type MetricsServer struct {
	port int
	e    *echo.Echo
}

func NewMetricsServer(port int) *MetricsServer {
	e := echo.New()
	e.HideBanner = true
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))

	return &MetricsServer{
		e:    e,
		port: port,
	}
}

func (s *MetricsServer) Start() {
	go func() {
		s.e.Start(fmt.Sprintf(":%d", s.port))
	}()
}

func (s *MetricsServer) Stop() {
	log.Printf("Stopping Metrics Server")

	s.e.Shutdown(context.TODO())
}
