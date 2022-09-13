package sql2q

import (
	"fmt"
	"testing"
)

func TestUtil(t *testing.T) {
	t.Parallel()

	t.Run("Error1st", func(t *testing.T) {
		t.Parallel()

		t.Run("empty", func(t *testing.T) {
			t.Parallel()

			var e error = Error1st(nil)
			checkOk(t, e, func() string { return "Must not fail" })
		})

		t.Run("single ok", func(t *testing.T) {
			t.Parallel()

			var e error = Error1st([]func() error{
				func() error { return nil },
			})
			checkOk(t, e, func() string { return "Must not fail" })
		})

		t.Run("many ok", func(t *testing.T) {
			t.Parallel()

			var e error = Error1st([]func() error{
				func() error { return nil },
				func() error { return nil },
			})
			checkOk(t, e, func() string { return "Must not fail" })
		})

		t.Run("1st ng", func(t *testing.T) {
			t.Parallel()

			var e error = Error1st([]func() error{
				func() error { return fmt.Errorf("Must fail") },
				func() error { panic("Must not execute this func") },
			})
			checkNg(t, e, func() string { return "Must fail" })
		})

		t.Run("2nd ng", func(t *testing.T) {
			t.Parallel()

			var e error = Error1st([]func() error{
				func() error { return nil },
				func() error { return fmt.Errorf("Must fail") },
				func() error { panic("Must not execute this func") },
			})
			checkNg(t, e, func() string { return "Must fail" })
		})
	})

	t.Run("ErrorFromBool", func(t *testing.T) {
		t.Parallel()

		t.Run("ok", func(t *testing.T) {
			t.Parallel()

			e := ErrorFromBool(true, func() error { return fmt.Errorf("Must not fail") })
			checkOk(t, e, func() string { return "Must not fail" })
		})

		t.Run("ng", func(t *testing.T) {
			t.Parallel()

			e := ErrorFromBool(false, func() error { return fmt.Errorf("Must fail") })
			checkNg(t, e, func() string { return "Must fail" })
		})
	})
}
