package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FromGrpcError classifies a gRPC error into a coarse category (suitable for
// a low-cardinality metric label) and extracts its message with no gRPC
// wrapping — just st.Message(), not the "rpc error: code = ... desc = ..."
// form err.Error() would give. err == nil returns ("", "").
//
// A non-status err (one that doesn't carry a gRPC code) is treated as
// codes.Unknown per status.FromError, which this function categorizes as
// "internal"; message falls back to err.Error() in that case since there's
// no gRPC message to extract.
func FromGrpcError(err error) (category string, message string) {
	st, _ := status.FromError(err)
	message = st.Message()

	switch st.Code() {
	case codes.OK:
		return "", message

	case codes.DeadlineExceeded,
		codes.Canceled:
		return "timeout", message

	case codes.Aborted,
		codes.FailedPrecondition,
		codes.AlreadyExists,
		codes.InvalidArgument,
		codes.OutOfRange:
		return "invalid_request", message

	case codes.Unknown,
		codes.Unimplemented,
		codes.Internal,
		codes.Unavailable,
		codes.DataLoss:
		return "internal", message

	case codes.NotFound:
		return "not_found", message

	case codes.PermissionDenied:
		return "permission_denied", message

	case codes.ResourceExhausted:
		return "resource_exhausted", message

	case codes.Unauthenticated:
		return "unauthenticated", message
	}

	return "internal", message
}

func MeasureSince(o prometheus.Observer, t1 time.Time) {
	o.Observe(time.Since(t1).Seconds())
}
