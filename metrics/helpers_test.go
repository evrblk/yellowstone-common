package metrics

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFromGrpcError(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantCat     string
		wantMessage string
	}{
		// status.Error(codes.OK, ...) collapses to nil in grpc-go, so the OK
		// path is exercised by this case rather than a separate one.
		{"nil", nil, "", ""},
		{"not_found", status.Error(codes.NotFound, "no such queue"), "not_found", "no such queue"},
		{"invalid_request", status.Error(codes.InvalidArgument, "bad batch size"), "invalid_request", "bad batch size"},
		{"timeout", status.Error(codes.DeadlineExceeded, "took too long"), "timeout", "took too long"},
		{"permission_denied", status.Error(codes.PermissionDenied, "nope"), "permission_denied", "nope"},
		{"resource_exhausted", status.Error(codes.ResourceExhausted, "quota"), "resource_exhausted", "quota"},
		{"unauthenticated", status.Error(codes.Unauthenticated, "bad key"), "unauthenticated", "bad key"},
		{"internal", status.Error(codes.Internal, "db down"), "internal", "db down"},
		{"non_status_error", errors.New("boom"), "internal", "boom"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			category, message := FromGrpcError(c.err)
			if category != c.wantCat {
				t.Errorf("category = %q, want %q", category, c.wantCat)
			}
			if message != c.wantMessage {
				t.Errorf("message = %q, want %q", message, c.wantMessage)
			}
		})
	}
}

func TestFromGrpcError_MessageHasNoGrpcWrapping(t *testing.T) {
	err := status.Error(codes.Internal, "db connection refused")
	_, message := FromGrpcError(err)

	if message != "db connection refused" {
		t.Errorf("message = %q, want the raw message with no gRPC wrapping", message)
	}
	// err.Error() is what we're explicitly avoiding: "rpc error: code = ... desc = ...".
	if message == err.Error() {
		t.Errorf("message should differ from err.Error(), got the same wrapped string: %q", message)
	}
}
