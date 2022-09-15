package pgx2q

import (
	"context"
	"database/sql"
	"math/rand"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"

	p2q "github.com/takanoriyanagitani/go-sql2q"
	jsd "github.com/takanoriyanagitani/go-sql2q/pkg/codec/text/json/jsons"
)

func mustSameBuilderNew[T any](comp func(a, b T) (same bool)) func(a, b T, onError func()) {
	return func(a, b T, onError func()) {
		if !comp(a, b) {
			onError()
		}
	}
}

func mustSame[T comparable](a, b T, onError func()) {
	comp := func(a, b T) (same bool) { return a == b }
	mustSameBuilderNew(comp)(a, b, onError)
}

func BenchmarkAll(b *testing.B) {
	var ITEST_SQL2Q_PGX_DBNAME = os.Getenv("ITEST_SQL2Q_PGX_DBNAME")
	if len(ITEST_SQL2Q_PGX_DBNAME) < 1 {
		b.Skip("skipping pgx benchmark...")
	}

	b.Run("Push Single", func(b *testing.B) {
		const testname string = "bench_push"
		const tablerst string = "DROP TABLE IF EXISTS bench_push"
		var constr string = "dbname=" + ITEST_SQL2Q_PGX_DBNAME

		func() {
			db := p2q.MustOk(sql.Open("pgx", constr))
			defer db.Close()

			_, e := db.ExecContext(context.Background(), tablerst)
			p2q.IfNg(e, func() { b.Fatalf("Must not fail: %v", e) })
		}()

		var cfg Config = p2q.MustOk(ConfigNew(testname)).
			WithMaxQueue(int64(b.N))

		type queueBuilder func(conn string) (p2q.Queue, error)

		var qb queueBuilder = QueueBuilderNew(cfg)

		var dat []byte = make([]byte, 8192)
		_, e := rand.Read(dat)
		p2q.IfNg(e, func() { b.Fatalf("Unable to get random data: %v", e) })

		func(q p2q.Queue) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				e := q.Push(context.Background(), dat)
				p2q.IfNg(e, func() { b.Fatalf("Must not fail: %v", e) })
			}
		}(p2q.MustOk(qb(constr)))

		b.Cleanup(func() {
			db := p2q.MustOk(sql.Open("pgx", constr))
			defer db.Close()

			var row *sql.Row = db.QueryRowContext(context.Background(), `
				SELECT COUNT(*) FROM pg_class
				WHERE
					relname=$1::TEXT
					AND relkind='r'
			`, testname)

			var cnt int64
			e = row.Scan(&cnt)
			p2q.IfNg(e, func() { b.Fatalf("Must not fail: %v", e) })

			mustSame(cnt, 1, func() { b.Fatalf("Unexpected table count: %v", cnt) })
		})
	})

	b.Run("PushMany", func(b *testing.B) {
		b.Run("json codec", func(b *testing.B) {
			chkr := func(bulkSz int) func(b *testing.B) {
				return func(b *testing.B) {
					const testname string = "bench_pushmany_json"
					const tablerst string = "DROP TABLE IF EXISTS bench_pushmany_json"
					var constr string = "dbname=" + ITEST_SQL2Q_PGX_DBNAME

					func() {
						db := p2q.MustOk(sql.Open("pgx", constr))
						defer db.Close()

						_, e := db.ExecContext(context.Background(), tablerst)
						p2q.IfNg(e, func() { b.Fatalf("Must not fail: %v", e) })
					}()

					var cfg Config = p2q.MustOk(ConfigNew(testname)).
						WithMaxQueue(int64(b.N))

					type queueBuilder func(conn string) (p2q.Queue, error)

					var qb queueBuilder = QueueBuilderNew(cfg)

					var datGen func(sz int) []byte = func(sz int) []byte {
						var buf []byte = make([]byte, sz)
						_, e := rand.Read(buf)
						p2q.IfNg(e, func() { b.Fatalf("Unable to generate random data: %v", e) })
						return buf
					}

					var i2msg func(i int) p2q.Msg = p2q.Compose(
						func(_ int) []byte { return datGen(8192) },
						func(b []byte) p2q.Msg { return p2q.MsgEmpty().WithData(b) },
					)

					var imsg p2q.Iter[p2q.Msg] = p2q.IterMap(p2q.IterInts(0, bulkSz), i2msg)
					var msgs []p2q.Msg = imsg.ToArray()
					mustSame(len(msgs), bulkSz, func() { b.Fatalf("Unexpected size: %v", len(msgs)) })

					var jcodec p2q.Codec = p2q.CodecMust(jsd.JsonsCodecNew())

					func(q p2q.Queue, codec p2q.Codec) {
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							e := q.PushMany(context.Background(), msgs, codec)
							p2q.IfNg(e, func() { b.Fatalf("Must not fail: %v", e) })
						}
						b.ReportMetric(float64(b.N)*float64(bulkSz), "inserts")
					}(p2q.MustOk(qb(constr)), jcodec)

					b.Cleanup(func() {
						db := p2q.MustOk(sql.Open("pgx", constr))
						defer db.Close()

						var row *sql.Row = db.QueryRowContext(context.Background(), `
							SELECT COUNT(*) FROM pg_class
							WHERE
								relname=$1::TEXT
								AND relkind='r'
						`, testname)

						var cnt int64
						e := row.Scan(&cnt)
						p2q.IfNg(e, func() { b.Fatalf("Must not fail: %v", e) })

						mustSame(cnt, 1, func() { b.Fatalf("Unexpected table count: %v", cnt) })
					})
				}
			}

			b.Run("bulksz=16", chkr(16))
			b.Run("bulksz=128", chkr(128))
			b.Run("bulksz=1024", chkr(1024))
		})
	})
}
