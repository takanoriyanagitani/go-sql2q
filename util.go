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

func MustOk[T any](t T, e error) T {
	if nil != e {
		panic(e)
	}
	return t
}

func IfNg(ng error, f func()) {
	if nil != ng {
		f()
	}
}

func Compose[T, U, V any](f func(T) U, g func(U) V) func(T) V {
	return func(t T) V {
		var u U = f(t)
		return g(u)
	}
}
