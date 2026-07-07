package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func FromGrpcError(err error) string {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.OK:
			return ""

		case codes.DeadlineExceeded,
			codes.Canceled:
			return "timeout"

		case codes.Aborted,
			codes.FailedPrecondition,
			codes.AlreadyExists,
			codes.InvalidArgument,
			codes.OutOfRange:
			return "invalid_request"

		case codes.Unknown,
			codes.Unimplemented,
			codes.Internal,
			codes.Unavailable,
			codes.DataLoss:
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

func MeasureSince(o prometheus.Observer, t1 time.Time) {
	o.Observe(time.Since(t1).Seconds())
}
