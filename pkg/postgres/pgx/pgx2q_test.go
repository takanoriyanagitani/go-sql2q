package pgx2q

import (
	"bytes"
	"context"
	"os"
	"testing"

	p2q "github.com/takanoriyanagitani/go-sql2q"
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

var checkBytes func(t *testing.T, got, expected []byte) = checkerBuilder(func(a, b []byte) (same bool) { return 0 == bytes.Compare(a, b) })

func TestAll(t *testing.T) {
	t.Parallel()

	var ITEST_SQL2Q_PGX_DBNAME = os.Getenv("ITEST_SQL2Q_PGX_DBNAME")
	if len(ITEST_SQL2Q_PGX_DBNAME) < 1 {
		t.Skip("skipping pgx test...")
	}

	t.Run("QueueBuilderNewDefault", func(t *testing.T) {
		t.Parallel()

		var qbld func(conn string) (p2q.Queue, error) = QueueBuilderNewDefault("tablename_queue")

		q, e := qbld("")
		if nil != e {
			t.Fatalf("Unable to get queue: %v", e)
		}

		t.Run("non-parallel", func(t *testing.T) {

			t.Run("empty", func(t *testing.T) {
				e := q.Push(context.Background(), nil)
				if nil != e {
					t.Fatalf("Unable to push data: %v", e)
				}
			})

			t.Run("get empty", func(t *testing.T) {
				e := q.Pop(context.Background(), func(ctx context.Context, msg p2q.Msg) error {
					checkBytes(t, msg.Data(), nil)
					return nil
				})

				if nil != e {
					t.Fatalf("Unexpected error: %v", e)
				}
			})

		})

		t.Cleanup(func() {
			q.Close()
		})
	})
}
