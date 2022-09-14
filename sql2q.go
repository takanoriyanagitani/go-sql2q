package sql2q

import (
	"context"
	"fmt"
)

// Add push data to queue.
type Add func(ctx context.Context, data []byte) error

// Get gets oldest msg from queue.
type Get func(ctx context.Context) (Msg, error)

// Del removes queue using its unique id.
type Del func(ctx context.Context, id Id) error

// Cnt gets queue count.
type Cnt func(ctx context.Context) (int64, error)

// Lmt gets queue limit.
type Lmt func(ctx context.Context) int64

// Cls closes queue(optional)
type Cls func() error

type Queue struct {
	add Add
	get Get
	del Del
	cnt Cnt
	lmt Lmt
	cls Cls
}

type QueueBuilder struct {
	Add
	Get
	Del
	Cnt
	Lmt
	Cls
}

func (b QueueBuilder) Build() (q Queue, e error) {
	egen := func(ok bool, ng func() string) func() error {
		e := ErrorFromBool(ok, func() error { return fmt.Errorf(ng()) })
		return func() error { return e }
	}
	e = Error1st([]func() error{
		egen(nil != b.Add, func() string { return "Invalid adder" }),
		egen(nil != b.Get, func() string { return "Invalid getter" }),
		egen(nil != b.Del, func() string { return "Invalid remover" }),
		egen(nil != b.Cnt, func() string { return "Invalid counter" }),
		egen(nil != b.Lmt, func() string { return "Invalid limit" }),
		egen(nil != b.Cls, func() string { return "Invalid closer" }),
	})

	q.add = b.Add
	q.get = b.Get
	q.del = b.Del
	q.cnt = b.Cnt
	q.lmt = b.Lmt
	q.cls = b.Cls
	return
}

// Pop gets msg and remove it if client received.
// 1. try get msg
// 2. call callback cb and send msg
// 3. if callback recv msg, try remove queue
func (q Queue) Pop(ctx context.Context, cb func(context.Context, Msg) error) error {
	msg, e := q.get(ctx)
	if nil != e {
		return e
	}

	e = cb(ctx, msg)
	if nil != e {
		return e
	}

	return q.del(ctx, msg.id)
}

// Push push msg if queue has enough space.
// 1. get queue size
// 2. add msg if queue has enough space
func (q Queue) Push(ctx context.Context, data []byte) error {
	cnt, e := q.cnt(ctx)
	if nil != e {
		return e
	}
	if q.lmt(ctx) <= cnt {
		return fmt.Errorf("Out of capacity. lmt: %v, cnt: %v", q.lmt(ctx), cnt)
	}
	return q.add(ctx, data)
}

// PushMany push many messages as single msg.
func (q Queue) PushMany(ctx context.Context, messages []Msg, codec Codec) error {
	if len(messages) < 1 {
		return nil
	}

	packed, e := codec.Pack(ctx, messages)
	if nil != e {
		return e
	}
	return q.Push(ctx, packed.Data())
}

// PopMany pop many messages from single virtual msg.
func (q Queue) PopMany(ctx context.Context, codec Codec, cb func(context.Context, []Msg) error) error {
	msg, e := q.get(ctx)
	if nil != e {
		return e
	}

	msgs, e := codec.Unpack(ctx, msg)
	if nil != e {
		return e
	}

	e = cb(ctx, msgs)
	if nil != e {
		return e
	}

	return q.del(ctx, msg.id)
}

// Close closes queue(optional)
func (q Queue) Close() error { return q.cls() }
