package pgx2q

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"

	p2q "github.com/takanoriyanagitani/go-sql2q"
	jsd "github.com/takanoriyanagitani/go-sql2q/pkg/codec/text/json/jsons"
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

func chkOk(e error, msg func() string) func(t *testing.T) {
	return func(t *testing.T) {
		checkOk(t, e, msg)
	}
}

func chkNg(e error, msg func() string) func(t *testing.T) {
	return func(t *testing.T) {
		checkNg(t, e, msg)
	}
}

func chkOrPanic(e error) func(t *testing.T) {
	return func(t *testing.T) {
		if nil != e {
			panic(e)
		}
	}
}

func TestAll(t *testing.T) {
	t.Parallel()

	var ITEST_SQL2Q_PGX_DBNAME = os.Getenv("ITEST_SQL2Q_PGX_DBNAME")
	if len(ITEST_SQL2Q_PGX_DBNAME) < 1 {
		t.Skip("skipping pgx test...")
	}

	t.Run("QueueBuilderNewDefault", func(t *testing.T) {
		t.Parallel()

		const tablename string = "tablename_queue"
		const tablereset string = `
            DROP TABLE IF EXISTS tablename_queue
        `
		const constr string = ""

		func() {
			db, e := sql.Open("pgx", constr)
			checkOk(t, e, func() string { return "Must not fail" })
			defer db.Close()

			_, e = db.ExecContext(context.Background(), tablereset)
			checkOk(t, e, func() string { return "Unable to drop test table" })
		}()

		var qbld func(conn string) (p2q.Queue, error) = QueueBuilderNewDefault(tablename)

		q, e := qbld(constr)
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

			t.Run("push non empty", func(t *testing.T) {
				e := q.Push(context.Background(), []byte("hw"))
				if nil != e {
					t.Fatalf("Unable to push data: %v", e)
				}
			})

			t.Run("get non empty", func(t *testing.T) {
				e := q.Pop(context.Background(), func(ctx context.Context, msg p2q.Msg) error {
					checkBytes(t, msg.Data(), []byte("hw"))
					return nil
				})

				if nil != e {
					t.Fatalf("Unexpected error: %v", e)
				}
			})

			t.Run("many", func(t *testing.T) {
				jsonChk := func(codec p2q.Codec) func(t *testing.T) {
					return func(t *testing.T) {
						msgs := []p2q.Msg{
							p2q.MsgEmpty().WithData([]byte(`{"bucket":"data_2022_09_15_cafef00ddeadbeafface864299792458", "key":"09:41:38.0Z", "val":"[333,634]"}`)),
							p2q.MsgEmpty().WithData([]byte(`{"bucket":"data_2022_09_15_f00ddeadbeaffacecafe864299792458", "key":"09:41:38.1Z", "val":"[599,634]"}`)),
						}

						t.Run("push", func(t *testing.T) {
							e := q.PushMany(context.Background(), msgs, codec)
							checkOk(t, e, func() string { return "Must not fail" })
						})

						t.Run("pop", func(t *testing.T) {
							e := q.PopMany(context.Background(), codec, func(_c context.Context, got []p2q.Msg) error {
								checker(t, len(got), 2)
								checkBytes(t, got[0].Data(), msgs[0].Data())
								checkBytes(t, got[1].Data(), msgs[1].Data())
								return nil
							})
							checkOk(t, e, func() string { return "Must not fail" })
						})
					}
				}

				t.Run("many check using json codec", jsonChk(p2q.CodecMust(jsd.JsonsCodecNew())))
			})

		})

		t.Run("invalid connection string", func(t *testing.T) {
			t.Parallel()

			invlQ, e := qbld("PGHOST=localhost PGPORT=0")
			checkOk(t, e, func() string { return "Must not fail" })

			t.Cleanup(func() {
				invlQ.Close()
			})
		})

		t.Cleanup(func() {
			q.Close()
		})
	})
}
