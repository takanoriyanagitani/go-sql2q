package sql2q

type Counter func() int64

func CounterNew() Counter {
	var i int64 = 0
	return func() int64 {
		var r int64 = i
		i += 1
		return r
	}
}
