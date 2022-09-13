package sql2q

// Id is unique queue identifier.
type Id struct {
	id int64
}

func (i Id) AsInteger() int64 { return i.id }

// Msg is queue data with identifier.
type Msg struct {
	id Id
	dt []byte
}

func (m Msg) Data() []byte { return m.dt }

func MsgNew(i int64, dt []byte) Msg {
	var id Id = Id{id: i}
	return Msg{
		id,
		dt,
	}
}
