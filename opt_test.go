package sql2q

import (
	"testing"
)

func TestOpt(t *testing.T) {
	t.Parallel()

	t.Run("OptionEmpty", func(t *testing.T) {
		var oi Option[int] = OptionEmpty[int]()
		checker(t, oi.Value(), 0)
	})
}
