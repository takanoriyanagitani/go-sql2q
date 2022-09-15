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
	}) // always nil
	return buf.Bytes(), e
}

var packMsg p2q.Pack = func(ctx context.Context, msgs []p2q.Msg) (p2q.Msg, error) {
	packed, _ := msgs2jsonl(ctx, msgs) // always nil error
	return p2q.MsgEmpty().WithData(packed), nil
}

var jsonl2msgs p2q.Unpack = func(ctx context.Context, packed p2q.Msg) ([]p2q.Msg, error) {
	var msgs []byte = packed.Data()
	var sep []byte = []byte("\n")
	var splited [][]byte = bytes.Split(msgs, sep)
	var isp p2q.Iter[[]byte] = p2q.IterFromArray(splited)
	var imsg p2q.Iter[p2q.Msg] = p2q.IterMap(isp, func(dt []byte) p2q.Msg {
		return p2q.MsgEmpty().WithData(dt)
	})
	var arr []p2q.Msg = imsg.ToArray()
	return p2q.PopLast(arr), nil
}

func JsonsCodecNew() (p2q.Codec, error) { return p2q.CodecNew(packMsg, jsonl2msgs) }
