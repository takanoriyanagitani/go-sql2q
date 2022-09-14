package sql2q

func Error1st(ef []func() error) error {
	var fi Iter[func() error] = IterFromArray(ef)
	return IterReduce(fi, nil, func(e error, f func() error) error {
		if nil == e {
			return f()
		}
		return e
	})
}

func ErrorFromBool(ok bool, ng func() error) error {
	if ok {
		return nil
	}
	return ng()
}

func PopLast[T any](s []T) []T {
	if 0 < len(s) {
		var neo int = len(s) - 1
		return s[:neo]
	}
	return s
}
