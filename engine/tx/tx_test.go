package tx

import (
	"kv/assert"
	"kv/engine/internal/mocks"
	"kv/engine/wal/record"
	storagemocks "kv/storage/mocks"
	"testing"
	"time"
)

func TestTransaction_Commit(t *testing.T) {
	tm, appender := setup()

	t.Run("it appends 'commit' record", func(t *testing.T) {
		tx, err := tm.BeginTx()
		assert.NoError(t, err)

		err = tx.Commit()
		assert.NoError(t, err)
		assert.Equal(t, len(appender.Records), 1)

		commitRecord := appender.Records[0]
		assert.Equal(t, commitRecord.TxID, tx.ID.Uint64())
		assert.Equal(t, commitRecord.Kind, record.Commit)
	})

	t.Run("it stops transaction", func(t *testing.T) {
		tx, err := tm.BeginTx()
		assert.NoError(t, err)
		assert.True(t, tm.isActive(tx.ID))

		err = tx.Commit()
		assert.NoError(t, err)

		assert.False(t, tm.isActive(tx.ID))
	})
}

func TestTransaction_Abort(t *testing.T) {
	tm, _ := setup()

	setupTx := func(t *testing.T) (*Transaction, version) {
		tx, err := tm.BeginTx()
		assert.NoError(t, err)
		newVersion := newMockVersion("key", []byte("value"), IdFrozen)
		return tx, newVersion
	}

	t.Run("it stops transaction", func(t *testing.T) {
		tx, _ := setupTx(t)
		assert.True(t, tm.isActive(tx.ID))

		tx.Abort()

		assert.False(t, tm.isActive(tx.ID))
	})

	t.Run("it restores tracked removed records", func(t *testing.T) {
		tx, rec := setupTx(t)
		rec.TryKill(tx.ID)

		assert.False(t, tx.CanSee(rec.XMin(), rec.XMax()))

		tx.Track(rec)
		tx.Abort()

		assert.True(t, tx.CanSee(rec.XMin(), rec.XMax()))
	})

	t.Run("it removes tracked added records", func(t *testing.T) {
		tx, err := tm.BeginTx()
		assert.NoError(t, err)

		newVersion := newMockVersion("key", []byte("value"), tx.ID)
		assert.True(t, tx.CanSee(newVersion.XMin(), newVersion.XMax()))

		tx.Track(newVersion)
		tx.Abort()

		assert.False(t, tx.CanSee(newVersion.XMin(), newVersion.XMax()))
	})

	t.Run("it does nothing if already committed", func(t *testing.T) {
		tx, rec := setupTx(t)
		rec.TryKill(tx.ID)

		tx.Track(rec)
		err := tx.Commit()
		assert.NoError(t, err)
		assert.False(t, tx.CanSee(rec.XMin(), rec.XMax()))

		tx.Abort()

		assert.False(t, tx.CanSee(rec.XMin(), rec.XMax()))
	})
}

func TestTransaction_AbortAfter(t *testing.T) {
	tm, _ := setup()

	setupTx := func(t *testing.T) (*Transaction, version) {
		tx, err := tm.BeginTx()
		assert.NoError(t, err)
		newVersion := newMockVersion("key", []byte("value"), IdFrozen)
		return tx, newVersion
	}

	t.Run("it aborts transaction after timeout", func(t *testing.T) {
		tx, _ := setupTx(t)

		tx.abortAfter(10)
		time.Sleep(15 * time.Millisecond)

		assert.False(t, tm.isActive(tx.ID))
	})

	t.Run("it does nothing when transaction was committed before timeout", func(t *testing.T) {
		tx, rec := setupTx(t)
		rec.TryKill(tx.ID)

		tx.Track(rec)
		tx.abortAfter(50)

		err := tx.Commit()
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		assert.False(t, tx.CanSee(rec.XMin(), rec.XMax()))
		assert.False(t, tm.isActive(tx.ID))
	})
}

func TestTransaction_CanSee(t *testing.T) {
	tm, _ := setup()

	t.Run("it can see frozen transactions", func(t *testing.T) {
		tx, err := tm.BeginTx()
		assert.NoError(t, err)

		got := tx.CanSee(IdFrozen, IdAlive)

		assert.True(t, got)
	})

	t.Run("it can see its own inserts", func(t *testing.T) {
		tx, err := tm.BeginTx()
		assert.NoError(t, err)

		got := tx.CanSee(tx.ID, IdAlive)

		assert.True(t, got)
	})

	t.Run("it does not see its own deletes", func(t *testing.T) {
		tx, err := tm.BeginTx()
		assert.NoError(t, err)

		got := tx.CanSee(tx.ID, tx.ID)

		assert.False(t, got)
	})

	t.Run("it cannot see uncommitted transaction", func(t *testing.T) {
		active, err := tm.BeginTx()
		assert.NoError(t, err)
		tx, err := tm.BeginTx()
		assert.NoError(t, err)

		got := tx.CanSee(active.ID, IdAlive)

		assert.False(t, got)
	})

	t.Run("it ignores deletes from uncommitted transaction", func(t *testing.T) {
		creator, err := tm.BeginTx()
		assert.NoError(t, err)
		_ = creator.Commit()

		tx, err := tm.BeginTx()
		assert.NoError(t, err)
		deleter, err := tm.BeginTx()
		assert.NoError(t, err)

		got := tx.CanSee(creator.ID, deleter.ID)

		assert.True(t, got)
	})

	t.Run("it can see inserts committed before snapshot", func(t *testing.T) {
		old, err := tm.BeginTx()
		assert.NoError(t, err)
		_ = old.Commit()
		assert.NoError(t, err)
		tx, err := tm.BeginTx()

		got := tx.CanSee(old.ID, IdAlive)

		assert.True(t, got)
	})

	t.Run("it cannot see inserts committed after snapshot", func(t *testing.T) {
		tx, err := tm.BeginTx()
		assert.NoError(t, err)
		other, err := tm.BeginTx()
		assert.NoError(t, err)
		_ = other.Commit()

		got := tx.CanSee(other.ID, IdAlive)

		assert.False(t, got)
	})

	t.Run("it ignores deletes started before and committed after snapshot", func(t *testing.T) {
		creator, err := tm.BeginTx()
		_ = creator.Commit()
		assert.NoError(t, err)
		deleter, err := tm.BeginTx()
		assert.NoError(t, err)
		tx, err := tm.BeginTx()
		assert.NoError(t, err)
		_ = deleter.Commit()

		got := tx.CanSee(creator.ID, deleter.ID)

		assert.True(t, got)
	})

	t.Run("it ignores deletes started and committed after snapshot", func(t *testing.T) {
		creator, err := tm.BeginTx()
		assert.NoError(t, err)
		_ = creator.Commit()
		tx, err := tm.BeginTx()
		assert.NoError(t, err)
		deleter, err := tm.BeginTx()
		assert.NoError(t, err)
		_ = deleter.Commit()

		got := tx.CanSee(creator.ID, deleter.ID)

		assert.True(t, got)
	})
}

func setup() (*Manager, *mocks.MockAppender) {
	file := storagemocks.NewFile()
	manifest := NewManifest(file)
	appender := mocks.NewAppender()

	return NewManager(manifest, appender, ManagerOptions{
		ReservedIDsPerBatch:   5,
		MaxActiveTransactions: 100,
		TimeoutMs:             1000,
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
