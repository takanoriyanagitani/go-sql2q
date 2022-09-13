package pgx2q

import (
	"testing"
)

func TestConf(t *testing.T) {
	t.Parallel()

	t.Run("ConfigNew", func(t *testing.T) {
		t.Parallel()

		t.Run("invalid table name", func(t *testing.T) {
			t.Parallel()

			_, e := ConfigNew("0table")
			if nil == e {
				t.Fatalf("Must fail")
			}
		})
	})

	t.Run("WithMaxQueue", func(t *testing.T) {
		t.Parallel()

		cfg1, e := ConfigNew("q_table_name")
		if nil != e {
			t.Fatalf("Unexpected error: %v", e)
		}

		checker(t, cfg1.MaxQueue(), DefaultMaxQueue)

		cfg2 := cfg1.WithMaxQueue(127)
		checker(t, cfg1.MaxQueue(), DefaultMaxQueue)
		checker(t, cfg2.MaxQueue(), 127)
	})
}
