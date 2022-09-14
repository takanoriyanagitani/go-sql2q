package sql2q

import (
	"testing"
)

func TestOpt(t *testing.T) {
	t.Parallel()

	t.Run("OptionEmpty", func(t *testing.T) {
		t.Parallel()

		var oi Option[int] = OptionEmpty[int]()
		checker(t, oi.Value(), 0)
	})

	t.Run("OrElse", func(t *testing.T) {
		t.Parallel()

		chk := func(op1, op2 Option[int], alt, expected int, hasValue bool) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()

				var o Option[int] = op1.OrElse(func() Option[int] { return op2 })
				var got int = o.UnwrapOr(alt)
				checker(t, o.HasValue(), hasValue)
				checker(t, got, expected)
			}
		}

		t.Run("empty-empty", chk(OptionEmpty[int](), OptionEmpty[int](), 2022915, 2022915, false))
		t.Run("empty-value", chk(OptionEmpty[int](), OptionNew(1234567), 2022915, 1234567, true))
		t.Run("value-empty", chk(OptionNew(7654321), OptionEmpty[int](), 2022915, 7654321, true))
		t.Run("value-value", chk(OptionNew(6343776), OptionNew(3776333), 2022915, 6343776, true))
	})
}
