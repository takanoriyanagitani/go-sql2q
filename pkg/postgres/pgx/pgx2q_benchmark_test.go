package pgx2q

import (
	"context"
	"database/sql"
	"math/rand"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"

	p2q "github.com/takanoriyanagitani/go-sql2q"
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

	b.Run("Push", func(b *testing.B) {
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

		func() {
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
		}()
	})

	b.Run("PushMany", func(b *testing.B) {
		b.Run("json codec", func(b *testing.B) {
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

			func() {
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
			}()
		})
	})
}
