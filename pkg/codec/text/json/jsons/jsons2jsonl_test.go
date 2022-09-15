package jsons

import (
	"bytes"
	"context"
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

func checker[T comparable](t *testing.T, a, b T) {
	var chk func(t *testing.T, got, expected T) = checkerBuilder(func(a, b T) (same bool) { return a == b })
	chk(t, a, b)
}

var checkBytes func(t *testing.T, got, expected []byte) = checkerBuilder(func(a, b []byte) (same bool) { return 0 == bytes.Compare(a, b) })

func TestAll(t *testing.T) {
	t.Parallel()

	t.Run("JsonsCodecNew", func(t *testing.T) {
		t.Parallel()

		conv, e := JsonsCodecNew()
		if nil != e {
			t.Fatalf("Unable to initialize json codec: %v", e)
		}

		t.Run("Pack", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				msg, e := conv.Pack(context.Background(), nil)
				if nil != e {
					t.Fatalf("Unable to pack: %v", e)
				}

				var dat []byte = msg.Data()
				checkBytes(t, dat, nil)
			})

			t.Run("single json", func(t *testing.T) {
				t.Parallel()
				json := `{"bucket":"data_2022_09_13_cafef00ddeadbeafface864299792458", "key":"15:13:46.0Z", "val":"[333,634]"}`
				jsonl := json + "\n"
				msgs := []p2q.Msg{
					p2q.MsgEmpty().WithData([]byte(json)),
				}

				msg, e := conv.Pack(context.Background(), msgs)
				if nil != e {
					t.Fatalf("Unable to pack: %v", e)
				}

				var packed []byte = msg.Data()
				checkBytes(t, packed, []byte(jsonl))
			})

			t.Run("many jsons", func(t *testing.T) {
				t.Parallel()
				json1 := `{"bucket":"data_2022_09_13_cafef00ddeadbeafface864299792458", "key":"15:13:46.0Z", "val":"[333,634]"}`
				json2 := `{"bucket":"data_2022_09_13_f00ddeadbeaffacecafe864299792458", "key":"15:13:46.1Z", "val":"[599,634]"}`
				jsonl := json1 + "\n" + json2 + "\n"

				msgs := []p2q.Msg{
					p2q.MsgEmpty().WithData([]byte(json1)),
					p2q.MsgEmpty().WithData([]byte(json2)),
				}

				msg, e := conv.Pack(context.Background(), msgs)
				if nil != e {
					t.Fatalf("Unable to pack: %v", e)
				}

				var packed []byte = msg.Data()
				checkBytes(t, packed, []byte(jsonl))
			})
		})

		t.Run("Unpack", func(t *testing.T) {
			t.Parallel()

			t.Run("empty", func(t *testing.T) {
				msgs, e := conv.Unpack(context.Background(), p2q.MsgEmpty())
				if nil != e {
					t.Fatalf("Unable to unpack: %v", e)
				}

				checker(t, len(msgs), 0)
			})

			t.Run("single json", func(t *testing.T) {
				json1 := `{"bucket":"data_2022_09_12_cafef00ddeadbeafface864299792458", "key":"15:27:36.0Z", "val":"[333,634]"}`
				jsonl := json1 + "\n"

				msgs, e := conv.Unpack(context.Background(), p2q.MsgEmpty().WithData([]byte(jsonl)))
				if nil != e {
					t.Fatalf("Unable to unpack: %v", e)
				}

				checker(t, len(msgs), 1)
				var unpacked p2q.Msg = msgs[0]
				checkBytes(t, unpacked.Data(), []byte(json1))
			})

			t.Run("many jsons", func(t *testing.T) {
				json1 := `{"bucket":"data_2022_09_12_cafef00ddeadbeafface864299792458", "key":"15:27:36.0Z", "val":"[333,634]"}`
				json2 := `{"bucket":"data_2022_09_12_f00ddeadbeaffacecafe864299792458", "key":"15:32:25.0Z", "val":"[634,599]"}`
				jsonl := json1 + "\n" + json2 + "\n"

				msgs, e := conv.Unpack(context.Background(), p2q.MsgEmpty().WithData([]byte(jsonl)))
				if nil != e {
					t.Fatalf("Unable to unpack: %v", e)
				}

				checker(t, len(msgs), 2)

				chk := func(msg p2q.Msg, expected []byte) func(*testing.T) {
					return func(t *testing.T) {
						t.Parallel()

						checkBytes(t, msg.Data(), expected)
					}
				}

				t.Run("json1", chk(msgs[0], []byte(json1)))
				t.Run("json2", chk(msgs[1], []byte(json2)))
			})
		})
	})
}
