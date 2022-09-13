package sql2q

import (
	"testing"
)

func TestIter(t *testing.T) {
	t.Parallel()

	t.Run("IterReduce", func(t *testing.T) {
		t.Parallel()

		add := func(a, b int) int { return a + b }

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var i Iter[int] = IterFromArray[int](nil)
			var tot int = IterReduce(i, 0, add)
			checker(t, tot, 0)
		})

		t.Run("tot 0,1,2, ..., 10", func(t *testing.T) {
			t.Parallel()

			var i Iter[int] = IterInts(0, 11)
			var tot int = IterReduce(i, 0, add)
			checker(t, tot, 55)
		})

		t.Run("cnt 0,1,2, ..., 10", func(t *testing.T) {
			t.Parallel()

			var i Iter[int] = IterInts(0, 11)
			var cnt int = i.Count()
			checker(t, cnt, 11)
		})
	})

	t.Run("IterMap", func(t *testing.T) {
		t.Parallel()

		var str2len = func(s string) int { return len(s) }

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var i Iter[string] = IterEmpty[string]()
			var ic Iter[int] = IterMap(i, str2len)
			var tot int = IterReduce(ic, 0, func(a, b int) int { return a + b })
			checker(t, tot, 0)
		})

		t.Run("many strings", func(t *testing.T) {
			t.Parallel()

			var i Iter[string] = IterFromArray([]string{
				"0123456",
				"789abcd",
				"efghijk",
				"lmnopqr",
				"stuv012",
				"3456789",
			})
			var ic Iter[int] = IterMap(i, str2len)
			var tot int = IterReduce(ic, 0, func(a, b int) int { return a + b })
			checker(t, tot, 42)
		})
	})

	t.Run("ToArray", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var i Iter[int] = IterEmpty[int]()
			var a []int = i.ToArray()
			checker(t, len(a), 0)
		})

		t.Run("many ints", func(t *testing.T) {
			t.Parallel()

			var i Iter[int] = IterInts(0, 10)
			var a []int = i.ToArray()
			checker(t, len(a), 10)
		})
	})
}
