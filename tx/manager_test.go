package tx

import (
	"kv/test"
	"testing"
)

func TestTransactionManager_Begin(t *testing.T) {
	tm := NewTransactionManager()

	t.Run("it increments transaction IDs", func(t *testing.T) {
		tx1 := tm.Begin()
		tx2 := tm.Begin()

		test.AssertTrue(t, tx2.ID > tx1.ID)

		_ = tx1.Commit()
		_ = tx2.Commit()
	})

	t.Run("it sets correct xMin and xMax in snapshot", func(t *testing.T) {
		tx1 := tm.Begin()
		tx2 := tm.Begin()
		tx3 := tm.Begin()

		test.AssertEqual(t, tx3.snapshot.xMin, tx1.ID)
		test.AssertEqual(t, tx3.snapshot.xMax, tx3.ID)

		_ = tx1.Commit()
		_ = tx2.Commit()
		_ = tx3.Commit()
	})

	t.Run("it captures active transactions in snapshot", func(t *testing.T) {
		tx1 := tm.Begin()
		tx2 := tm.Begin()
		tx3 := tm.Begin()

		_, ok1 := tx3.snapshot.active[tx1.ID]
		_, ok2 := tx3.snapshot.active[tx2.ID]
		_, ok3 := tx3.snapshot.active[tx3.ID]

		test.AssertTrue(t, ok1)
		test.AssertTrue(t, ok2)
		test.AssertTrue(t, ok3)

		_ = tx1.Commit()
		_ = tx2.Commit()
		_ = tx3.Commit()
	})

	t.Run("it captures snapshots with incremented xMin after oldest transaction commits", func(t *testing.T) {
		tx1 := tm.Begin()
		tx2 := tm.Begin()

		_ = tx1.Commit()

		tx3 := tm.Begin()

		test.AssertEqual(t, tx2.ID, tx3.snapshot.xMin)

		_ = tx2.Commit()
		_ = tx3.Commit()
	})
}

func TestTransactionManager_Horizon(t *testing.T) {
	tm := NewTransactionManager()

	t.Run("it returns next ID when no transactions are active", func(t *testing.T) {
		tx1 := tm.Begin()
		_ = tx1.Commit()

		expectedNextID := tx1.ID + 1
		test.AssertEqual(t, expectedNextID, tm.Horizon())
	})

	t.Run("it returns oldest active transaction ID", func(t *testing.T) {
		tx1 := tm.Begin()
		tx2 := tm.Begin()
		tx3 := tm.Begin()

		test.AssertEqual(t, tx1.ID, tm.Horizon())

		_ = tx1.Commit()
		test.AssertEqual(t, tx2.ID, tm.Horizon())

		_ = tx2.Commit()
		test.AssertEqual(t, tx3.ID, tm.Horizon())

		_ = tx3.Commit()
	})

	t.Run("it handles out-of-order commits", func(t *testing.T) {
		tx1 := tm.Begin()
		tx2 := tm.Begin()

		_ = tx2.Commit()
		test.AssertEqual(t, tx1.ID, tm.Horizon())

		_ = tx1.Commit()
	})
}
