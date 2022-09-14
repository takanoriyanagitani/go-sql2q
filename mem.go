package sql2q

import (
	"context"
	"fmt"
	_ "sort"
)

type memQueue struct {
	raw map[int]Msg
	key []int
	lmt int64
	cnt Counter
}

func (m *memQueue) Lmt(_ context.Context) int64 { return m.lmt }
func (m *memQueue) Cls() error                  { return nil }

func (m *memQueue) Cnt(_ context.Context) (int64, error) {
	return int64(len(m.key)), nil
}

func (m *memQueue) Add(_ context.Context, dat []byte) error {
	var id int64 = m.cnt()
	var msg Msg = MsgNew(id, dat)
	var k int = int(id)
	m.raw[k] = msg
	m.key = append(m.key, k)
	return nil
}

func (m *memQueue) lastKey(_ context.Context) (int, error) {
	if 0 < len(m.key) {
		return m.key[len(m.key)-1], nil
	}
	return -1, fmt.Errorf("No data")
}

func (m *memQueue) Get(_ context.Context) (Msg, error) {
	if 0 < len(m.key) {
		var k int = m.key[0]
		return m.raw[k], nil
	}
	return MsgNew(-1, nil), fmt.Errorf("No data")
}

func (m *memQueue) Del(_ context.Context, id Id) error {
	var ix int64 = id.AsInteger()
	var i int = int(ix)
	_, ok := m.raw[i]
	if ok {
		delete(m.raw, i)
		var ik Iter[int] = IterFromArray(m.key)
		filter := func(si int) bool { return si != i }
		reducer := func(neo []int, si int) []int { return append(neo, si) }
		m.key = IterReduceFilter(ik, nil, filter, reducer)
		return nil
	}
	return fmt.Errorf("Invalid id: %v", ix)
}

func MemQueueNew(lmt int64) (Queue, error) {
	m := memQueue{
		raw: make(map[int]Msg),
		key: nil,
		lmt: lmt,
		cnt: CounterNew(),
	}
	b := QueueBuilder{
		Add: m.Add,
		Get: m.Get,
		Del: m.Del,
		Cnt: m.Cnt,
		Lmt: m.Lmt,
		Cls: m.Cls,
	}
	return b.Build()
}
