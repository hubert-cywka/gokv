package tx

import (
	"kv/assert"
	"kv/engine/internal/mocks"
	storagemocks "kv/storage/mocks"
	"testing"
)

func TestTransactionManager_Begin(t *testing.T) {
	reservedIDsPerBatch := 5
	maxActiveTx := 5
	file := storagemocks.NewFile()
	manifest := NewManifest(file)
	appender := mocks.NewAppender()

	tm := NewManager(manifest, appender, ManagerOptions{
		ReservedIDsPerBatch:   uint64(reservedIDsPerBatch),
		MaxActiveTransactions: uint16(maxActiveTx),
		TimeoutMs:             5000,
	})

	t.Run("it increments transaction IDs", func(t *testing.T) {
		tx1, _ := tm.Begin()
		tx2, _ := tm.Begin()

		assert.True(t, tx2.ID > tx1.ID)

		_ = tx1.Commit()
		_ = tx2.Commit()
	})

	t.Run("it loads new transaction IDs once current batch is exhausted", func(t *testing.T) {
		oldMaxID := tm.maxReservedID

		for range reservedIDsPerBatch {
			tx, _ := tm.Begin()
			_ = tx.Commit()
		}

		assert.True(t, tm.maxReservedID > oldMaxID)
	})

	t.Run("it persists newly reserved IDs in a file once current batch is exhausted", func(t *testing.T) {
		prevLastReservedID, _ := manifest.LastReservedID()

		for range reservedIDsPerBatch {
			tx, _ := tm.Begin()
			_ = tx.Commit()
		}

		newLastReservedID, _ := manifest.LastReservedID()
		assert.Equal(t, prevLastReservedID+uint64(reservedIDsPerBatch), newLastReservedID)
	})

	t.Run("it sets correct xMin and xMax in snapshot", func(t *testing.T) {
		tx1, _ := tm.Begin()
		tx2, _ := tm.Begin()
		tx3, _ := tm.Begin()

		assert.Equal(t, tx3.snapshot.xMin, tx1.ID)
		assert.Equal(t, tx3.snapshot.xMax, tx3.ID)

		_ = tx1.Commit()
		_ = tx2.Commit()
		_ = tx3.Commit()
	})

	t.Run("it captures active transactions in snapshot", func(t *testing.T) {
		tx1, _ := tm.Begin()
		tx2, _ := tm.Begin()
		tx3, _ := tm.Begin()

		ok1 := tx3.snapshot.IsActive(tx1.ID)
		ok2 := tx3.snapshot.IsActive(tx2.ID)
		ok3 := tx3.snapshot.IsActive(tx3.ID)

		assert.True(t, ok1)
		assert.True(t, ok2)
		assert.True(t, ok3)

		_ = tx1.Commit()
		_ = tx2.Commit()
		_ = tx3.Commit()
	})

	t.Run("it captures snapshots with incremented xMin after oldest transaction commits", func(t *testing.T) {
		tx1, _ := tm.Begin()
		tx2, _ := tm.Begin()

		_ = tx1.Commit()

		tx3, _ := tm.Begin()

		assert.Equal(t, tx2.ID, tx3.snapshot.xMin)

		_ = tx2.Commit()
		_ = tx3.Commit()
	})

	t.Run("it captures snapshots with incremented xMin after oldest transaction aborts", func(t *testing.T) {
		tx1, _ := tm.Begin()
		tx2, _ := tm.Begin()

		tx1.Abort()

		tx3, _ := tm.Begin()

		assert.Equal(t, tx2.ID, tx3.snapshot.xMin)

		_ = tx2.Commit()
		_ = tx3.Commit()
	})

	t.Run("it returns error when number max of active transactions is exceeded", func(t *testing.T) {
		activeTxs := make(map[ID]*Transaction, maxActiveTx)
		for range maxActiveTx {
			tx, err := tm.Begin()
			assert.NoError(t, err)
			activeTxs[tx.ID] = tx
		}

		_, err := tm.Begin()
		assert.Error(t, err, MaxActiveTransactionsExceededError)

		for _, activeTx := range activeTxs {
			_ = activeTx.Commit()
		}
	})

	t.Run("it decreases number of active transactions after a transaction ends", func(t *testing.T) {
		activeTxs := make(map[ID]*Transaction, maxActiveTx)
		for range maxActiveTx {
			tx, err := tm.Begin()
			assert.NoError(t, err)
			activeTxs[tx.ID] = tx
		}

		for _, activeTx := range activeTxs {
			_ = activeTx.Commit()
		}

		_, err := tm.Begin()
		assert.NoError(t, err)
	})
}

func TestTransactionManager_Horizon(t *testing.T) {
	tm, _ := setup()

	t.Run("it returns next ID when no transactions are active", func(t *testing.T) {
		tx1, _ := tm.Begin()
		_ = tx1.Commit()

		expectedNextID := tx1.ID + 1
		assert.Equal(t, expectedNextID, tm.FindTxHorizon())
	})

	t.Run("it returns oldest active transaction ID", func(t *testing.T) {
		tx1, _ := tm.Begin()
		tx2, _ := tm.Begin()
		tx3, _ := tm.Begin()

		assert.Equal(t, tx1.ID, tm.FindTxHorizon())

		_ = tx1.Commit()
		assert.Equal(t, tx2.ID, tm.FindTxHorizon())

		_ = tx2.Commit()
		assert.Equal(t, tx3.ID, tm.FindTxHorizon())

		_ = tx3.Commit()
	})

	t.Run("it handles out-of-order commits", func(t *testing.T) {
		tx1, _ := tm.Begin()
		tx2, _ := tm.Begin()

		_ = tx2.Commit()
		assert.Equal(t, tx1.ID, tm.FindTxHorizon())

		_ = tx1.Commit()
	})
}
