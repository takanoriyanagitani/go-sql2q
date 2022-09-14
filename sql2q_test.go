package sql2q

import (
	"bytes"
	"context"
	"fmt"
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

func TestAll(t *testing.T) {
	t.Parallel()

	t.Run("QueueBuilder", func(t *testing.T) {
		t.Parallel()

		t.Run("Build", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				bldr := QueueBuilder{}
				_, e := bldr.Build()
				checkNg(t, e, func() string { return "Must fail" })
			})
		})
	})

	t.Run("Queue", func(t *testing.T) {
		t.Parallel()

		t.Run("Pop", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				e = q.Pop(context.Background(), func(ctx context.Context, msg Msg) error {
					panic("Must not call")
				})
				checkNg(t, e, func() string { return "Must fail" })
			})

			t.Run("single", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				e = q.Push(context.Background(), []byte("hw"))
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.Pop(context.Background(), func(_ context.Context, m Msg) error {
					checkBytes(t, m.Data(), []byte("hw"))
					return nil
				})
				checkOk(t, e, func() string { return fmt.Sprintf("Must not fail: %v", e) })

				e = q.Pop(context.Background(), func(_ context.Context, m Msg) error {
					panic("Must not call")
				})
				checkNg(t, e, func() string { return "Must fail" })
			})

			t.Run("multi", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				e = q.Push(context.Background(), []byte("hw"))
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.Push(context.Background(), []byte("hh"))
				checkOk(t, e, func() string { return "Must not fail" })

				chk := func(expected []byte) func(t *testing.T) {
					return func(t *testing.T) {
						e = q.Pop(context.Background(), func(_ context.Context, m Msg) error {
							checkBytes(t, m.Data(), expected)
							return nil
						})
						checkOk(t, e, func() string { return fmt.Sprintf("Must not fail: %v", e) })
					}
				}

				t.Run("last", chk([]byte("hw")))
				t.Run("first", chk([]byte("hh")))

				e = q.Pop(context.Background(), func(_ context.Context, m Msg) error {
					panic("Must not call")
				})
				checkNg(t, e, func() string { return "Must fail" })
			})
		})
	})
}
