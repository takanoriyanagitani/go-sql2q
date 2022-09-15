package sql2q

import (
	"context"
	"fmt"
)

// Pack serializes many messages into single msg.
type Pack func(ctx context.Context, msgs []Msg) (Msg, error)

// Unpack deserializes single message into many messages.
type Unpack func(ctx context.Context, packed Msg) ([]Msg, error)

type Codec struct {
	ser Pack
	de  Unpack
}

func (c Codec) Pack(ctx context.Context, msgs []Msg) (Msg, error)     { return c.ser(ctx, msgs) }
func (c Codec) Unpack(ctx context.Context, packed Msg) ([]Msg, error) { return c.de(ctx, packed) }

func CodecNew(ser Pack, de Unpack) (Codec, error) {
	egen := func(ok bool, ng func() string) func() error {
		e := ErrorFromBool(ok, func() error { return fmt.Errorf(ng()) })
		return func() error { return e }
	}
	e := Error1st([]func() error{
		egen(nil != ser, func() string { return "Invalid packer" }),
		egen(nil != de, func() string { return "Invalid unpacker" }),
	})

	c := Codec{
		ser,
		de,
	}
	return c, e
}

func CodecMust(c Codec, e error) Codec {
	if nil != e {
		panic(e)
	}
	return c
}
