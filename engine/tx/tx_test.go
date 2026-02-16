package tx

import (
	"kv/engine/internal/mocks"
	"kv/engine/wal/record"
	storagemocks "kv/storage/mocks"
	"kv/test"
	"testing"
)

func TestTransaction_Commit(t *testing.T) {
	tm, appender := setup()

	t.Run("it appends 'commit' record", func(t *testing.T) {
		tx, err := tm.Begin()
		test.AssertNoError(t, err)

		err = tx.Commit()
		test.AssertNoError(t, err)
		test.AssertEqual(t, len(appender.Records), 1)

		commitRecord := appender.Records[0]
		test.AssertEqual(t, commitRecord.TxID, tx.ID.Uint64())
		test.AssertEqual(t, commitRecord.Kind, record.Commit)
	})

	t.Run("it stops transaction", func(t *testing.T) {
		tx, err := tm.Begin()
		test.AssertNoError(t, err)
		test.AssertTrue(t, tm.isActive(tx.ID))

		err = tx.Commit()
		test.AssertNoError(t, err)

		test.AssertFalse(t, tm.isActive(tx.ID))
	})
}

func TestTransaction_Abort(t *testing.T) {
	tm, _ := setup()

	setup := func(t *testing.T) (*Transaction, version) {
		tx, err := tm.Begin()
		test.AssertNoError(t, err)
		newVersion := newMockVersion("key", []byte("value"), IdFrozen)
		return tx, newVersion
	}

	t.Run("it stops transaction", func(t *testing.T) {
		tx, _ := setup(t)
		test.AssertTrue(t, tm.isActive(tx.ID))

		tx.Abort()

		test.AssertFalse(t, tm.isActive(tx.ID))
	})

	t.Run("it restores tracked removed records", func(t *testing.T) {
		tx, rec := setup(t)
		rec.TryKill(tx.ID)

		test.AssertFalse(t, tx.CanSee(rec.XMin(), rec.XMax()))

		tx.Track(rec)
		tx.Abort()

		test.AssertTrue(t, tx.CanSee(rec.XMin(), rec.XMax()))
	})

	t.Run("it removes tracked added records", func(t *testing.T) {
		tx, err := tm.Begin()
		test.AssertNoError(t, err)

		newVersion := newMockVersion("key", []byte("value"), tx.ID)
		test.AssertTrue(t, tx.CanSee(newVersion.XMin(), newVersion.XMax()))

		tx.Track(newVersion)
		tx.Abort()

		test.AssertFalse(t, tx.CanSee(newVersion.XMin(), newVersion.XMax()))
	})

	t.Run("it does nothing if already committed", func(t *testing.T) {
		tx, rec := setup(t)
		rec.TryKill(tx.ID)

		tx.Track(rec)
		err := tx.Commit()
		test.AssertNoError(t, err)
		test.AssertFalse(t, tx.CanSee(rec.XMin(), rec.XMax()))

		tx.Abort()

		test.AssertFalse(t, tx.CanSee(rec.XMin(), rec.XMax()))
	})
}

func TestTransaction_CanSee(t *testing.T) {
	tm, _ := setup()

	t.Run("it can see frozen transactions", func(t *testing.T) {
		tx, err := tm.Begin()
		test.AssertNoError(t, err)

		got := tx.CanSee(IdFrozen, IdAlive)

		test.AssertTrue(t, got)
	})

	t.Run("it can see its own inserts", func(t *testing.T) {
		tx, err := tm.Begin()
		test.AssertNoError(t, err)

		got := tx.CanSee(tx.ID, IdAlive)

		test.AssertTrue(t, got)
	})

	t.Run("it does not see its own deletes", func(t *testing.T) {
		tx, err := tm.Begin()
		test.AssertNoError(t, err)

		got := tx.CanSee(tx.ID, tx.ID)

		test.AssertFalse(t, got)
	})

	t.Run("it cannot see uncommitted transaction", func(t *testing.T) {
		active, err := tm.Begin()
		test.AssertNoError(t, err)
		tx, err := tm.Begin()
		test.AssertNoError(t, err)

		got := tx.CanSee(active.ID, IdAlive)

		test.AssertFalse(t, got)
	})

	t.Run("it ignores deletes from uncommitted transaction", func(t *testing.T) {
		creator, err := tm.Begin()
		test.AssertNoError(t, err)
		_ = creator.Commit()

		tx, err := tm.Begin()
		test.AssertNoError(t, err)
		deleter, err := tm.Begin()
		test.AssertNoError(t, err)

		got := tx.CanSee(creator.ID, deleter.ID)

		test.AssertTrue(t, got)
	})

	t.Run("it can see inserts committed before snapshot", func(t *testing.T) {
		old, err := tm.Begin()
		test.AssertNoError(t, err)
		_ = old.Commit()
		test.AssertNoError(t, err)
		tx, err := tm.Begin()

		got := tx.CanSee(old.ID, IdAlive)

		test.AssertTrue(t, got)
	})

	t.Run("it cannot see inserts committed after snapshot", func(t *testing.T) {
		tx, err := tm.Begin()
		test.AssertNoError(t, err)
		other, err := tm.Begin()
		test.AssertNoError(t, err)
		_ = other.Commit()

		got := tx.CanSee(other.ID, IdAlive)

		test.AssertFalse(t, got)
	})

	t.Run("it ignores deletes started before and committed after snapshot", func(t *testing.T) {
		creator, err := tm.Begin()
		_ = creator.Commit()
		test.AssertNoError(t, err)
		deleter, err := tm.Begin()
		test.AssertNoError(t, err)
		tx, err := tm.Begin()
		test.AssertNoError(t, err)
		_ = deleter.Commit()

		got := tx.CanSee(creator.ID, deleter.ID)

		test.AssertTrue(t, got)
	})

	t.Run("it ignores deletes started and committed after snapshot", func(t *testing.T) {
		creator, err := tm.Begin()
		test.AssertNoError(t, err)
		_ = creator.Commit()
		tx, err := tm.Begin()
		test.AssertNoError(t, err)
		deleter, err := tm.Begin()
		test.AssertNoError(t, err)
		_ = deleter.Commit()

		got := tx.CanSee(creator.ID, deleter.ID)

		test.AssertTrue(t, got)
	})
}

func setup() (*Manager, *mocks.MockAppender) {
	file := storagemocks.NewFile()
	manifest := NewManifest(file)
	appender := mocks.NewAppender()

	return NewManager(manifest, appender, ManagerOptions{
		ReservedIDsPerBatch:   5,
		MaxActiveTransactions: 100,
	}), appender
}

type mockVersion struct {
	Key   string
	Value []byte

	xMin ID
	xMax ID
	prev *mockVersion
}

func newMockVersion(key string, value []byte, txID ID) *mockVersion {
	return &mockVersion{
		Key:   key,
		Value: value,
		xMin:  txID,
		xMax:  IdAlive,
		prev:  nil,
	}
}

func (v *mockVersion) XMin() ID {
	return v.xMin
}

func (v *mockVersion) XMax() ID {
	return v.xMax
}

func (v *mockVersion) Freeze() {
	v.xMin = IdFrozen
}

func (v *mockVersion) Resurrect() {
	v.xMax = IdAlive
}

func (v *mockVersion) TryKill(x ID) bool {
	if v.xMax == IdAlive {
		v.xMax = x
		return true
	}

	return false
}
