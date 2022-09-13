package pgx2q

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v4/stdlib"

	p2q "github.com/takanoriyanagitani/go-sql2q"
)

type pgxq struct {
	db *sql.DB

	tab string

	add string
	get string
	del string
	cnt string

	max int64
}

func pgxNew(connStr string) func(tableName validTableName, max int64) (pgxq, error) {
	return func(tableName validTableName, max int64) (pgxq, error) {
		db, e := sql.Open("pgx", connStr)
		if nil != e {
			return pgxq{}, e
		}
		q, e := dbNew(db, tableName, max)
		if nil != e {
			_ = db.Close()
			return pgxq{}, e
		}
		return q, nil
	}
}

func dbNew(db *sql.DB, tableName validTableName, max int64) (pgxq, error) {
	var tab builtQuery = createTab(tableName)
	var add builtQuery = createAdd(tableName)
	var get builtQuery = createGet(tableName)
	var del builtQuery = createDel(tableName)
	var cnt builtQuery = createCnt(tableName)

	e := p2q.Error1st([]func() error{
		func() error { return tab.err },
		func() error { return add.err },
		func() error { return get.err },
		func() error { return del.err },
		func() error { return cnt.err },
	})

	p := pgxq{
		db: db,

		tab: tab.txt,

		add: add.txt,
		get: get.txt,
		del: del.txt,
		cnt: cnt.txt,

		max: max,
	}

	return p, e
}

func QueueBuilderNew(conf Config) func(connStr string) (p2q.Queue, error) {
	return func(connStr string) (q p2q.Queue, e error) {
		var qNew func(tableName validTableName, max int64) (pgxq, error) = pgxNew(connStr)
		p, e := qNew(conf.ValidName(), conf.MaxQueue())
		if nil != e {
			return q, e
		}
		return p.ToQueue()
	}
}

func QueueBuilderNewDefault(tableName string) func(connStr string) (p2q.Queue, error) {
	return func(connStr string) (q p2q.Queue, e error) {
		conf, e := ConfigNew(tableName)
		if nil != e {
			return
		}
		return QueueBuilderNew(conf)(connStr)
	}
}

func (p pgxq) Close() error { return p.db.Close() }

func (p pgxq) Tab(ctx context.Context) error {
	_, e := p.db.ExecContext(ctx, p.tab)
	return e
}

func (p pgxq) Add(ctx context.Context, data []byte) error {
	_, e := p.db.ExecContext(ctx, p.add, data)
	return e
}

func (p pgxq) Get(ctx context.Context) (m p2q.Msg, e error) {
	var row *sql.Row = p.db.QueryRowContext(ctx, p.get)

	var id int64
	var dat []byte
	e = row.Scan(&id, &dat)

	m = p2q.MsgNew(id, dat)
	return
}

func (p pgxq) Del(ctx context.Context, id p2q.Id) error {
	_, e := p.db.ExecContext(ctx, p.del, id.AsInteger())
	return e
}

func (p pgxq) Cnt(ctx context.Context) (int64, error) {
	e := p.Tab(ctx)
	if nil != e {
		return 0, e
	}

	var row *sql.Row = p.db.QueryRowContext(ctx, p.cnt)

	var cnt int64
	e = row.Scan(&cnt)

	return cnt, e
}

func (p pgxq) Lmt(_ context.Context) int64 { return p.max }

func (p pgxq) ToQueue() (p2q.Queue, error) {
	var b p2q.QueueBuilder = p2q.QueueBuilder{
		Add: p.Add,
		Get: p.Get,
		Del: p.Del,
		Cnt: p.Cnt,
		Lmt: p.Lmt,
		Cls: p.Close,
	}
	return b.Build()
}
