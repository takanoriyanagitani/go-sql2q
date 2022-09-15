package sql2q

import (
	"context"
	"testing"
)

func TestCodec(t *testing.T) {
	t.Parallel()

	t.Run("invalid", func(t *testing.T) {
		t.Parallel()

		t.Run("both", func(t *testing.T) {
			t.Parallel()

			_, e := CodecNew(nil, nil)
			checkNg(t, e, func() string { return "Must fail" })
		})

		t.Run("pack", func(t *testing.T) {
			t.Parallel()

			_, e := CodecNew(nil, func(_c context.Context, _m Msg) ([]Msg, error) { return nil, nil })
			checkNg(t, e, func() string { return "Must fail" })
		})

		t.Run("unpack", func(t *testing.T) {
			t.Parallel()

			_, e := CodecNew(func(_c context.Context, _m []Msg) (Msg, error) { return MsgEmpty(), nil }, nil)
			checkNg(t, e, func() string { return "Must fail" })
		})
	})
}
