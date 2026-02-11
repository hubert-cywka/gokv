package tx

import (
	"kv/test"
	"testing"
)

func TestTransaction_Commit(t *testing.T) {
	tm := NewTransactionManager()

	t.Run("it changes transaction state to 'committed'", func(t *testing.T) {
		tx := tm.Begin()
		before := tx.Committed()
		_ = tx.Commit()
		after := tx.Committed()

		test.AssertFalse(t, before)
		test.AssertTrue(t, after)
	})
}

func TestTransaction_CanSee(t *testing.T) {
	tm := NewTransactionManager()

	t.Run("it can see frozen transactions", func(t *testing.T) {
		tx := tm.Begin()

		got := tx.CanSee(FROZEN_TX_ID, FROZEN_TX_ID)

		test.AssertTrue(t, got)
	})

	t.Run("it can see its own inserts", func(t *testing.T) {
		tx := tm.Begin()

		got := tx.CanSee(tx.ID, FROZEN_TX_ID)

		test.AssertTrue(t, got)
	})

	t.Run("it does not see its own deletes", func(t *testing.T) {
		tx := tm.Begin()

		got := tx.CanSee(tx.ID, tx.ID)

		test.AssertFalse(t, got)
	})

	t.Run("it cannot see uncommitted transaction", func(t *testing.T) {
		active := tm.Begin()
		tx := tm.Begin()

		got := tx.CanSee(active.ID, FROZEN_TX_ID)

		test.AssertFalse(t, got)
	})

	t.Run("it ignores deletes from uncommitted transaction", func(t *testing.T) {
		creator := tm.Begin()
		_ = creator.Commit()

		tx := tm.Begin()
		deleter := tm.Begin()

		got := tx.CanSee(creator.ID, deleter.ID)

		test.AssertTrue(t, got)
	})

	t.Run("it can see inserts committed before snapshot", func(t *testing.T) {
		old := tm.Begin()
		_ = old.Commit()
		tx := tm.Begin()

		got := tx.CanSee(old.ID, FROZEN_TX_ID)

		test.AssertTrue(t, got)
	})

	t.Run("it cannot see inserts committed after snapshot", func(t *testing.T) {
		tx := tm.Begin()
		other := tm.Begin()
		_ = other.Commit()

		got := tx.CanSee(other.ID, FROZEN_TX_ID)

		test.AssertFalse(t, got)
	})

	t.Run("it ignores deletes started before and committed after snapshot", func(t *testing.T) {
		creator := tm.Begin()
		_ = creator.Commit()
		deleter := tm.Begin()
		tx := tm.Begin()
		_ = deleter.Commit()

		got := tx.CanSee(creator.ID, deleter.ID)

		test.AssertTrue(t, got)
	})

	t.Run("it ignores deletes started and committed after snapshot", func(t *testing.T) {
		creator := tm.Begin()
		_ = creator.Commit()
		tx := tm.Begin()
		deleter := tm.Begin()
		_ = deleter.Commit()

		got := tx.CanSee(creator.ID, deleter.ID)

		test.AssertTrue(t, got)
	})
}
