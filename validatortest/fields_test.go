package validatortest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeMessage stands in for a protoc-gen-go-generated message: A and B carry
// a `protobuf:` tag like a real generated field, while state simulates the
// unexported bookkeeping fields (state/sizeCache/unknownFields) that real
// generated messages carry and that must never be required to be checked.
type fakeMessage struct {
	state int

	A string `protobuf:"bytes,1,opt,name=a,proto3"`
	B int64  `protobuf:"varint,2,opt,name=b,proto3"`
}

func validateFakeMessageOK(req *fakeMessage) error {
	if req.A == "" {
		return errors.New("A is required")
	}
	if req.B < 0 {
		return errors.New("B must be non-negative")
	}
	return nil
}

func validateFakeMessageMissingB(req *fakeMessage) error {
	if req.A == "" {
		return errors.New("A is required")
	}
	return nil
}

func validateFakeMessageWithExtraParam(req *fakeMessage, fieldName string) error {
	_ = fieldName
	if req.A == "" {
		return errors.New("A is required")
	}
	if req.B < 0 {
		return errors.New("B must be non-negative")
	}
	return nil
}

// fakeT is a minimal TestingT that records failures instead of aborting the
// goroutine, so this package's own tests can assert on both the pass and
// fail paths of AssertAllFieldsChecked.
type fakeT struct {
	failed  bool
	message string
}

func (f *fakeT) Helper() {}

func (f *fakeT) Fatalf(format string, args ...interface{}) {
	f.failed = true
	f.message = fmt.Sprintf(format, args...)
}

func TestAssertAllFieldsChecked_AllFieldsReferenced(t *testing.T) {
	ft := &fakeT{}
	AssertAllFieldsChecked(ft, validateFakeMessageOK)
	require.False(t, ft.failed, ft.message)
}

func TestAssertAllFieldsChecked_MissingFieldFails(t *testing.T) {
	ft := &fakeT{}
	AssertAllFieldsChecked(ft, validateFakeMessageMissingB)
	require.True(t, ft.failed)
	require.Contains(t, ft.message, "B")
}

func TestAssertAllFieldsChecked_SkipFieldsExcludesField(t *testing.T) {
	ft := &fakeT{}
	AssertAllFieldsChecked(ft, validateFakeMessageMissingB, "B")
	require.False(t, ft.failed, ft.message)
}

func TestAssertAllFieldsChecked_ExtraTrailingParameterIsFine(t *testing.T) {
	ft := &fakeT{}
	AssertAllFieldsChecked(ft, validateFakeMessageWithExtraParam)
	require.False(t, ft.failed, ft.message)
}
