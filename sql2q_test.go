package sql2q

import (
	"bytes"
	"testing"
)

func checkerBuilder[T any](comp func(a, b T) (same bool)) func(t *testing.T, got, expected T) {
	return func(t *testing.T, got, expected T) {
		var same bool = comp(got, expected)
		if !same {
			t.Errorf("Unexpected value got.\n")
			t.Errorf("expected: %v\n", expected)
			t.Fatalf("got:      %v\n", got)
		}
	}
}

func checker[T comparable](t *testing.T, got, expected T) {
	var chk func(t *testing.T, got, expected T) = checkerBuilder(func(a, b T) (same bool) { return a == b })
	chk(t, got, expected)
}

var checkBytes func(t *testing.T, got, expected []byte) = checkerBuilder(func(a, b []byte) (same bool) { return 0 == bytes.Compare(a, b) })

func checkOk(t *testing.T, e error, msg func() string) {
	if nil != e {
		t.Fatalf(msg())
	}
}

func checkNg(t *testing.T, e error, msg func() string) {
	if nil == e {
		t.Fatalf(msg())
	}
}
