package jsons

import (
	"bytes"
	"context"

	p2q "github.com/takanoriyanagitani/go-sql2q"
)

func msgs2jsonl(ctx context.Context, msgs []p2q.Msg) ([]byte, error) {
	var im p2q.Iter[p2q.Msg] = p2q.IterFromArray(msgs)
	var buf bytes.Buffer
	var lf []byte = []byte("\n")
	e := p2q.IterReduce(im, nil, func(e error, msg p2q.Msg) error {
		var dat []byte = msg.Data()
		_, _ = buf.Write(dat) // always nil error
		_, _ = buf.Write(lf)  // always nil error
		return nil
	})
	return buf.Bytes(), e
}

var packMsg p2q.Pack = func(ctx context.Context, msgs []p2q.Msg) (p2q.Msg, error) {
	packed, e := msgs2jsonl(ctx, msgs)
	if nil != e {
		return p2q.MsgNew(-1, nil), e
	}
	return p2q.MsgNew(-1, packed), nil
}

var jsonl2msgs p2q.Unpack = func(ctx context.Context, packed p2q.Msg) ([]p2q.Msg, error) {
	var msgs []byte = packed.Data()
	if nil == msgs {
		return nil, nil
	}
	var sep []byte = []byte("\n")
	var splited [][]byte = bytes.Split(msgs, sep)
	var isp p2q.Iter[[]byte] = p2q.IterFromArray(splited)
	var imsg p2q.Iter[p2q.Msg] = p2q.IterMap(isp, func(dt []byte) p2q.Msg {
		return p2q.MsgNew(-1, dt)
	})
	var arr []p2q.Msg = imsg.ToArray()
	if 0 < len(arr) {
		neo := len(arr) - 1
		return arr[:neo], nil
	}
	return arr, nil
}

func JsonsCodecNew() (p2q.Codec, error) { return p2q.CodecNew(packMsg, jsonl2msgs) }
