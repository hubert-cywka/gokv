package mvcc

import (
	"kv/engine/internal/mocks"
	"kv/engine/tx"
	"kv/storage"
	"kv/test"
	"testing"
)

func AssertFrozen(t *testing.T, e *Version) {
	t.Helper()

	test.AssertEqual(t, e.XMin(), tx.IdFrozen)
}

func AssertNotFrozen(t *testing.T, e *Version) {
	t.Helper()

	test.AssertNotEqual(t, e.XMin(), tx.IdFrozen)
}

func AssertPruned(t *testing.T, e *Version) {
	t.Helper()

	test.AssertEqual(t, e, nil)
}

func AssertNotPruned(t *testing.T, e *Version) {
	t.Helper()

	test.AssertNotEqual(t, e, nil)
}

func beginTransaction(t *testing.T, txManager *tx.Manager) *tx.Transaction {
	transaction, err := txManager.Begin()
	test.AssertNoError(t, err)
	return transaction
}

func setupTxManager() *tx.Manager {
	file := storage.NewMockFile()
	manifest := tx.NewManifest(file)
	writeAheadLog := mocks.NewAppender()

	return tx.NewManager(manifest, writeAheadLog, tx.ManagerOptions{
		ReservedIDsPerBatch:   1000,
		MaxActiveTransactions: 1000,
	})
}

func setup() (*Store, *VersionMap) {
	versionMap := NewVersionMap()
	return NewStore(versionMap), versionMap
}
