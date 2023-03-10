package assert

import (
	"bytes"
	"testing"
)

func Err(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func NoErr(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

func Equal[T comparable](t *testing.T, expected, actual T) {
	t.Helper()

	if expected != actual {
		t.Fatalf("Not equal, expected %v, got %v", expected, actual)
	}
}

func EqualBytes(t *testing.T, expected, actual []byte) {
	t.Helper()

	if !bytes.Equal(expected, actual) {
		t.Fatalf("Not equal, expected %v, got %v", expected, actual)
	}
}
