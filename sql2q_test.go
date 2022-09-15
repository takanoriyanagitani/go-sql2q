package sql2q

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func simpleCodecNew() (Codec, error) {
	sep := []byte("\n")
	return CodecNew(
		func(_ context.Context, msgs []Msg) (Msg, error) {
			var imsg Iter[Msg] = IterFromArray(msgs)
			var msg Msg = MsgEmpty()
			return IterReduce(imsg, msg, func(packed Msg, m Msg) Msg {
				var buf bytes.Buffer

				var old []byte = packed.Data()
				if nil != old {
					_, _ = buf.Write(packed.Data()) // never fail
				}

				_, _ = buf.Write(m.Data())
				_, _ = buf.Write(sep)

				return packed.WithData(buf.Bytes())
			}), nil
		},
		func(_ context.Context, packed Msg) ([]Msg, error) {
			var serialized []byte = packed.Data()
			var splited [][]byte = bytes.Split(serialized, sep)
			var ibs Iter[[]byte] = IterFromArray(splited)
			var imsg Iter[Msg] = IterMap(ibs, func(b []byte) Msg {
				return MsgEmpty().WithData(b)
			})
			var ret []Msg = imsg.ToArray()
			if 0 < len(ret) {
				var nl int = len(ret) - 1
				return ret[:nl], nil
			}
			return nil, nil
		},
	)
}

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

			t.Run("callback error", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				e = q.Push(context.Background(), []byte("hw"))
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.Pop(context.Background(), func(_ context.Context, m Msg) error {
					return fmt.Errorf("Must fail")
				})
				checkNg(t, e, func() string { return "Must fail" })
			})
		})

		t.Run("PopMany", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				c, e := simpleCodecNew()
				if nil != e {
					t.Fatalf("Unexpected error: %v", e)
				}

				e = q.PushMany(context.Background(), nil, c)
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.PopMany(context.Background(), c, func(_ context.Context, msgs []Msg) error {
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

				c, e := simpleCodecNew()
				if nil != e {
					t.Fatalf("Unexpected error: %v", e)
				}

				e = q.PushMany(context.Background(), []Msg{
					MsgEmpty().WithData([]byte(`idx,1,csv,sample`)),
				}, c)
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.PopMany(context.Background(), c, func(_ context.Context, msgs []Msg) error {
					checker(t, len(msgs), 1)
					return nil
				})
				checkOk(t, e, func() string { return "Must not fail" })
			})

			t.Run("multi", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				c, e := simpleCodecNew()
				if nil != e {
					t.Fatalf("Unexpected error: %v", e)
				}

				inMsgs := []Msg{
					MsgEmpty().WithData([]byte(`idx,1,csv,sample`)),
					MsgEmpty().WithData([]byte(`idx,2,csv,sample`)),
				}
				e = q.PushMany(context.Background(), inMsgs, c)
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.PopMany(context.Background(), c, func(_ context.Context, msgs []Msg) error {
					checker(t, len(msgs), 2)
					checkBytes(t, inMsgs[0].Data(), msgs[0].Data())
					checkBytes(t, inMsgs[1].Data(), msgs[1].Data())
					return nil
				})
				checkOk(t, e, func() string { return "Must not fail" })
			})

			t.Run("callback error", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				c, e := simpleCodecNew()
				if nil != e {
					t.Fatalf("Unexpected error: %v", e)
				}

				e = q.PushMany(context.Background(), []Msg{
					MsgEmpty().WithData([]byte(`idx,1,csv,sample`)),
				}, c)
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.PopMany(context.Background(), c, func(_ context.Context, msgs []Msg) error {
					return fmt.Errorf("Must fail")
				})
				checkNg(t, e, func() string { return "Must fail" })
			})

			t.Run("invalid unpack", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				var upk Unpack = func(_ context.Context, p Msg) ([]Msg, error) {
					return nil, fmt.Errorf("Must fail")
				}
				var pck Pack = func(_ context.Context, msgs []Msg) (m Msg, e error) {
					return
				}

				c, e := CodecNew(pck, upk)
				t.Run("codec check", chkOk(e, func() string { return "Must not fail" }))

				e = q.PushMany(context.Background(), []Msg{
					MsgEmpty().WithData([]byte(`idx,1,csv,sample`)),
				}, c)
				t.Run("push check", chkOk(e, func() string { return "Must not fail" }))

				e = q.PopMany(context.Background(), c, func(_ context.Context, msgs []Msg) error {
					return fmt.Errorf("Must fail")
				})
				t.Run("pop check", chkNg(e, func() string { return "Must fail" }))
			})
		})

		t.Run("PushMany", func(t *testing.T) {
			t.Parallel()

			t.Run("invalid pack", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				var upk Unpack = func(_ context.Context, p Msg) ([]Msg, error) {
					return nil, nil
				}
				var pck Pack = func(_ context.Context, msgs []Msg) (m Msg, e error) {
					e = fmt.Errorf("Must fail")
					return
				}

				c, e := CodecNew(pck, upk)
				t.Run("codec check", chkOk(e, func() string { return "Must not fail" }))

				e = q.PushMany(context.Background(), []Msg{
					MsgEmpty().WithData([]byte(`idx,1,csv,sample`)),
				}, c)
				t.Run("push check", chkNg(e, func() string { return "Must fail" }))
			})

			t.Run("too many", func(t *testing.T) {
				t.Parallel()

				q, e := MemQueueNew(3)
				if nil != e {
					t.Fatalf("Unable to create in-mem q: %v", e)
				}
				defer q.Close()

				var upk Unpack = func(_ context.Context, p Msg) ([]Msg, error) {
					return nil, nil
				}
				var pck Pack = func(_ context.Context, msgs []Msg) (m Msg, e error) {
					return
				}

				c, e := CodecNew(pck, upk)
				t.Run("codec check", chkOk(e, func() string { return "Must not fail" }))

				e = q.PushMany(context.Background(), []Msg{
					MsgEmpty().WithData([]byte(`idx,1,csv,sample`)),
				}, c)
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.PushMany(context.Background(), []Msg{
					MsgEmpty().WithData([]byte(`idx,1,csv,sample`)),
				}, c)
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.PushMany(context.Background(), []Msg{
					MsgEmpty().WithData([]byte(`idx,1,csv,sample`)),
				}, c)
				checkOk(t, e, func() string { return "Must not fail" })

				e = q.PushMany(context.Background(), []Msg{
					MsgEmpty().WithData([]byte(`idx,1,csv,sample`)),
				}, c)
				checkNg(t, e, func() string { return "Must fail" })
			})
		})
	})
}
