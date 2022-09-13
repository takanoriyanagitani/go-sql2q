package sql2q

type Iter[T any] func() Option[T]

func IterReduce[T, U any](i Iter[T], init U, reducer func(state U, item T) U) U {
	state := init
	for o := i(); o.HasValue(); o = i() {
		state = reducer(state, o.Value())
	}
	return state
}

func IterFromArray[T any](a []T) Iter[T] {
	ix := 0
	return func() Option[T] {
		if ix < len(a) {
			var t T = a[ix]
			ix += 1
			return OptionNew(t)
		}
		return OptionEmpty[T]()
	}
}
