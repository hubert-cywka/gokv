package test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func AssertTrue(t *testing.T, got bool) {
	t.Helper()

	if !got {
		t.Errorf("expected %v to be true", got)
	}
}

func AssertFalse(t *testing.T, got bool) {
	t.Helper()

	if got {
		t.Errorf("expected %v to be false", got)
	}
}

func AssertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func AssertNotEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()

	if reflect.DeepEqual(got, want) {
		t.Errorf("expected %v to not equal %v", want, got)
	}
}

func AssertBytesEqual(t *testing.T, got, want []byte) {
	t.Helper()

	if !bytes.Equal(got, want) {
		t.Errorf("expected %x, got %x", want, got)
	}
}

func AssertBytesNotEqual(t *testing.T, a, b []byte) {
	t.Helper()

	if bytes.Equal(a, b) {
		t.Errorf("expected %x and %x to be different", a, b)
	}
}

func AssertError(t *testing.T, got, want error) {
	t.Helper()

	if !errors.Is(got, want) {
		t.Errorf("expected error %v, got %v", want, got)
	}
}

func AssertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
