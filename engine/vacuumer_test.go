package engine

import (
	"kv/engine/internal/mocks"
	"kv/engine/mvcc"
	"kv/engine/tx"
	"kv/engine/wal/record"
	storagemocks "kv/storage/mocks"
	"kv/test"
	"testing"
)

func TestVacuumer_Vacuum(t *testing.T) {
	txManager := setupTxManager()
	coordinator, vacuumer, versionMap, mockWriteAheadLog := setup()

	givenEntryCommitted := func(key string, value []byte) {
		setupTx := beginTransaction(t, txManager)
		_ = coordinator.Set(key, value, setupTx)
		_ = setupTx.Commit()
	}

	givenEntryDeleted := func(key string) {
		setupTx := beginTransaction(t, txManager)
		_ = coordinator.Delete(key, setupTx)
		_ = setupTx.Commit()
	}

	givenTransactionActive := func() *tx.Transaction {
		return beginTransaction(t, txManager)
	}

	t.Run("it keeps latest version when no transactions are active", func(t *testing.T) {
		key := "keep-no-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)

		vacuumer.Vacuum(txManager)

		chain, _ := versionMap.GetChain(key)
		got := chain.Head()

		mvcc.AssertNotPruned(t, got)
	})

	t.Run("it keeps latest version when older transactions are still active", func(t *testing.T) {
		key := "keep-older-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()

		vacuumer.Vacuum(txManager)
		_ = txA.Commit()

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertNotPruned(t, got)
	})

	t.Run("it removes older versions when no transactions are active", func(t *testing.T) {
		key := "remove-older-no-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		givenEntryCommitted(key, initialValue)

		vacuumer.Vacuum(txManager)

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertNotPruned(t, got)
		mvcc.AssertPruned(t, got.PreviousVersion())
	})

	t.Run("it freezes latest version when no transactions are active", func(t *testing.T) {
		key := "freeze-latest-no-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		givenEntryCommitted(key, initialValue)

		vacuumer.Vacuum(txManager)

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertFrozen(t, got)
	})

	t.Run("it appends 'freeze' record to WAL when version is frozen", func(t *testing.T) {
		key := "freeze-appends-to-wal"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		givenEntryCommitted(key, initialValue)

		vacuumer.Vacuum(txManager)

		freezeRecord := mockWriteAheadLog.Records[len(mockWriteAheadLog.Records)-1]
		test.AssertEqual(t, string(freezeRecord.Key), key)
		test.AssertEqual(t, freezeRecord.Kind, record.Freeze)
	})

	t.Run("it removes older versions when they are not visible by any transaction", func(t *testing.T) {
		key := "remove-older-not-visible-by-other-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()

		vacuumer.Vacuum(txManager)
		_ = txA.Commit()

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertNotPruned(t, got)
		mvcc.AssertPruned(t, got.PreviousVersion())
	})

	t.Run("it freezes version that is visible by all transactions", func(t *testing.T) {
		key := "freeze-visible-by-all-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()

		vacuumer.Vacuum(txManager)
		_ = txA.Commit()

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertFrozen(t, got)
	})

	t.Run("it does not remove older versions when they are still visible by other transactions", func(t *testing.T) {
		key := "keeps-older-versions-still-visible-by-other-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()
		givenEntryCommitted(key, initialValue)

		vacuumer.Vacuum(txManager)
		_ = txA.Commit()

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertNotPruned(t, got)
		mvcc.AssertNotPruned(t, got.PreviousVersion())
	})

	t.Run("it does not freeze versions that are not visible by all transactions", func(t *testing.T) {
		key := "does-not-freeze-versions-not-visible-by-other-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()
		givenEntryCommitted(key, initialValue)

		vacuumer.Vacuum(txManager)
		_ = txA.Commit()

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertNotFrozen(t, got)
	})

	t.Run("it removes tombstone not visible to any transaction", func(t *testing.T) {
		key := "delete-invisible"
		givenEntryCommitted(key, []byte("v1"))
		givenEntryDeleted(key)

		vacuumer.Vacuum(txManager)

		_, ok := versionMap.GetChain(key)
		test.AssertFalse(t, ok)
	})

	t.Run("it prunes older versions in a long chain", func(t *testing.T) {
		key := "chain-of-versions"
		givenEntryCommitted(key, []byte("v1"))
		givenEntryCommitted(key, []byte("v2"))
		givenEntryCommitted(key, []byte("v3"))
		givenEntryCommitted(key, []byte("v4"))
		givenEntryCommitted(key, []byte("v5"))

		vacuumer.Vacuum(txManager)

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertPruned(t, got.PreviousVersion())
	})

	t.Run("it does not remove uncommitted versions", func(t *testing.T) {
		key := "does-not-remove-uncommitted"
		givenEntryCommitted(key, []byte("v1"))
		txLive := beginTransaction(t, txManager)
		_ = coordinator.Set(key, []byte("v2"), txLive)

		vacuumer.Vacuum(txManager)

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertNotPruned(t, got)
		_ = txLive.Commit()
	})

	t.Run("it does not freeze uncommitted versions", func(t *testing.T) {
		key := "does-not-freeze-uncommitted"
		givenEntryCommitted(key, []byte("v1"))
		txLive := beginTransaction(t, txManager)
		_ = coordinator.Set(key, []byte("v2"), txLive)

		vacuumer.Vacuum(txManager)

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertNotFrozen(t, got)
		_ = txLive.Commit()
	})

	t.Run("it does not remove older version, when newer version is still uncommitted", func(t *testing.T) {
		key := "live-uncommitted"
		givenEntryCommitted(key, []byte("v1"))
		txLive := beginTransaction(t, txManager)
		_ = coordinator.Set(key, []byte("v2"), txLive)

		vacuumer.Vacuum(txManager)

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		mvcc.AssertNotPruned(t, got)
		mvcc.AssertNotPruned(t, got.PreviousVersion())
		_ = txLive.Commit()
	})
}

func beginTransaction(t *testing.T, txManager *tx.Manager) *tx.Transaction {
	transaction, err := txManager.Begin()
	test.AssertNoError(t, err)
	return transaction
}

func setupTxManager() *tx.Manager {
	file := storagemocks.NewFile()
	manifest := tx.NewManifest(file)
	writeAheadLog := mocks.NewAppender()

	return tx.NewManager(manifest, writeAheadLog, tx.ManagerOptions{
		ReservedIDsPerBatch:   1000,
		MaxActiveTransactions: 1000,
	})
}

func setup() (*mvcc.Store, *Vacuumer, *mvcc.VersionMap, *mocks.MockAppender) {
	versionMap := mvcc.NewVersionMap()
	mockWriteAheadLog := mocks.NewAppender()
	vacuumer := NewVacuumer(versionMap, mockWriteAheadLog)
	return mvcc.NewStore(versionMap), vacuumer, versionMap, mockWriteAheadLog
}
