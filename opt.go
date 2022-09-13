package sql2q

type optionValue[T any] func() T
type optionEmpty func() bool

type Option[T any] struct {
	optionValue[T]
	optionEmpty
}

func OptionNew[T any](t T) Option[T] {
	return Option[T]{
		optionValue: func() T { return t },
		optionEmpty: func() bool { return false },
	}
}

func OptionEmpty[T any]() Option[T] {
	return Option[T]{
		optionValue: func() (t T) { return },
		optionEmpty: func() bool { return true },
	}
}

func (o Option[T]) Value() T       { return o.optionValue() }
func (o Option[T]) Empty() bool    { return o.optionEmpty() }
func (o Option[T]) HasValue() bool { return !o.Empty() }

func OptionMap[T, U any](o Option[T], f func(T) U) Option[U] {
	if o.HasValue() {
		var t T = o.Value()
		var u U = f(t)
		return OptionNew(u)
	}
	return OptionEmpty[U]()
}
